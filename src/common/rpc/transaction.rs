/*
 * Copyright (C) 2022 Vaticle
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::{
    collections::HashMap,
    iter,
    ops::DerefMut,
    sync::{Arc, RwLock},
};

use crossbeam::{atomic::AtomicCell, channel::Sender as SyncSender};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use prost::Message;
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel as unbounded_async, UnboundedReceiver, UnboundedSender},
        oneshot::{channel as oneshot_async, Sender as AsyncOneshotSender},
    },
    time::{sleep_until, Instant},
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Streaming;
use typedb_protocol::transaction::{self, server::Server, stream::State};

use super::{
    connection::BackgroundRuntime,
    message::{QueryRequest, QueryResponse, Response, TransactionRequest, TransactionResponse},
};
use crate::{
    answer::{ConceptMap, Numeric},
    common::{error::ClientError, RequestID, Result, DISPATCH_INTERVAL},
    connection::core,
    TransactionType,
};

#[derive(Debug)]
pub(super) enum TransactionCallback {
    OneShot(AsyncOneshotSender<Result<TransactionResponse>>),
    Streamed(UnboundedSender<Result<TransactionResponse>>),
}

impl TransactionCallback {
    pub(crate) async fn send_error(self, error: ClientError) {
        match self {
            Self::OneShot(sink) => sink.send(Err(error.into())).ok(),
            Self::Streamed(sink) => sink.send(Err(error.into())).ok(),
        };
    }
}

#[derive(Debug)]
pub(crate) struct TransactionStream {
    type_: TransactionType,
    options: core::Options,
    request_sink: UnboundedSender<(TransactionRequest, Option<TransactionCallback>)>,
    is_open: Arc<AtomicCell<bool>>,
    shutdown_sink: UnboundedSender<()>,
}

impl Drop for TransactionStream {
    fn drop(&mut self) {
        self.is_open.store(false);
        self.shutdown_sink.send(()).ok();
    }
}

impl TransactionStream {
    pub(super) fn new(
        background_runtime: &BackgroundRuntime,
        type_: TransactionType,
        options: core::Options,
        response: Response,
    ) -> Self {
        let (request_sink, grpc_stream) = match response {
            Response::TransactionOpen { request_sink, grpc_stream } => (request_sink, grpc_stream),
            _ => unreachable!(),
        };
        let (buffer_sink, buffer_source) = unbounded_async();
        let (shutdown_sink, shutdown_source) = unbounded_async();
        let is_open = Arc::new(AtomicCell::new(true));
        background_runtime.spawn(transaction_worker(
            buffer_sink.clone(),
            buffer_source,
            request_sink,
            grpc_stream,
            is_open.clone(),
            shutdown_source,
        ));
        Self { type_, options, request_sink: buffer_sink, is_open, shutdown_sink }
    }

    pub(crate) fn type_(&self) -> TransactionType {
        self.type_
    }

    pub(crate) fn options(&self) -> &core::Options {
        &self.options
    }

    pub(crate) async fn commit(&self) -> Result {
        self.single(TransactionRequest::Commit).await?;
        Ok(())
    }

    pub(crate) async fn rollback(&self) -> Result {
        self.single(TransactionRequest::Rollback).await?;
        Ok(())
    }

    pub(crate) async fn define(&self, query: String, options: core::Options) -> Result {
        self.single(TransactionRequest::Query(QueryRequest::Define { query, options })).await?;
        Ok(())
    }

    pub(crate) async fn undefine(&self, query: String, options: core::Options) -> Result {
        self.single(TransactionRequest::Query(QueryRequest::Undefine { query, options })).await?;
        Ok(())
    }

    pub(crate) async fn delete(&self, query: String, options: core::Options) -> Result {
        self.single(TransactionRequest::Query(QueryRequest::Delete { query, options })).await?;
        Ok(())
    }

    pub(crate) fn match_(
        &self,
        query: String,
        options: core::Options,
    ) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let stream = self.query_stream(QueryRequest::Match { query, options })?;
        Ok(stream.flat_map(|result| match result {
            Ok(QueryResponse::Match { answers }) => stream_iter(answers.into_iter().map(Ok)),
            Ok(_) => stream_once(Err(ClientError::MissingResponseField("match").into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) fn insert(
        &self,
        query: String,
        options: core::Options,
    ) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let stream = self.query_stream(QueryRequest::Insert { query, options })?;
        Ok(stream.flat_map(|result| match result {
            Ok(QueryResponse::Insert { answers }) => stream_iter(answers.into_iter().map(Ok)),
            Ok(_) => stream_once(Err(ClientError::MissingResponseField("insert").into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) fn update(
        &self,
        query: String,
        options: core::Options,
    ) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let stream = self.query_stream(QueryRequest::Update { query, options })?;
        Ok(stream.flat_map(|result| match result {
            Ok(QueryResponse::Update { answers }) => stream_iter(answers.into_iter().map(Ok)),
            Ok(_) => stream_once(Err(ClientError::MissingResponseField("update").into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) async fn match_aggregate(
        &self,
        query: String,
        options: core::Options,
    ) -> Result<Numeric> {
        match self.query_single(QueryRequest::MatchAggregate { query, options }).await? {
            QueryResponse::MatchAggregate { answer } => Ok(answer),
            _ => Err(ClientError::MissingResponseField("match_aggregate_res"))?,
        }
    }

    async fn single(&self, req: TransactionRequest) -> Result<TransactionResponse> {
        if !self.is_open.load() {
            return Err(ClientError::SessionIsClosed().into());
        }
        let (res_sink, recv) = oneshot_async();
        self.request_sink.send((req, Some(TransactionCallback::OneShot(res_sink)))).unwrap();
        recv.await.unwrap().map(Into::into)
    }

    async fn query_single(&self, req: QueryRequest) -> Result<QueryResponse> {
        match self.single(TransactionRequest::Query(req)).await? {
            TransactionResponse::Query(query) => Ok(query),
            _ => todo!(), // TODO error
        }
    }

    fn stream(
        &self,
        req: TransactionRequest,
    ) -> Result<impl Stream<Item = Result<TransactionResponse>>> {
        if !self.is_open.load() {
            return Err(ClientError::SessionIsClosed().into());
        }
        let (res_part_sink, recv) = unbounded_async();
        self.request_sink.send((req, Some(TransactionCallback::Streamed(res_part_sink)))).unwrap();
        Ok(UnboundedReceiverStream::new(recv).map_ok(Into::into))
    }

    fn query_stream(&self, req: QueryRequest) -> Result<impl Stream<Item = Result<QueryResponse>>> {
        Ok(self.stream(TransactionRequest::Query(req))?.map(|response| match response {
            Ok(TransactionResponse::Query(query)) => Ok(query),
            Ok(_) => todo!(), // TODO error
            Err(err) => Err(err),
        }))
    }
}

fn stream_once<'a, T: Send + 'a>(value: T) -> stream::BoxStream<'a, T> {
    stream_iter(iter::once(value))
}

fn stream_iter<'a, T: Send + 'a>(
    iter: impl Iterator<Item = T> + Send + 'a,
) -> stream::BoxStream<'a, T> {
    Box::pin(stream::iter(iter))
}

pub(super) async fn transaction_worker(
    queue_sink: UnboundedSender<(TransactionRequest, Option<TransactionCallback>)>,
    request_source: UnboundedReceiver<(TransactionRequest, Option<TransactionCallback>)>,
    request_sink: SyncSender<transaction::Client>,
    grpc_stream: Streaming<transaction::Server>,
    is_open: Arc<AtomicCell<bool>>,
    shutdown_signal: UnboundedReceiver<()>,
) {
    let listener = Listener { request_sink: queue_sink, listeners: Default::default(), is_open };
    tokio::spawn(buffer(request_source, request_sink, listener.clone(), shutdown_signal));
    tokio::spawn(transaction_listener(grpc_stream, listener));
}

async fn buffer(
    mut request_source: UnboundedReceiver<(TransactionRequest, Option<TransactionCallback>)>,
    grpc_sink: SyncSender<transaction::Client>,
    mut listener: Listener,
    mut shutdown_signal: UnboundedReceiver<()>,
) {
    const MAX_GRPC_MESSAGE_LEN: usize = 1_000_000;
    let mut request_buffer = TransactionRequestBuffer::default();
    let mut next_dispatch = Instant::now() + DISPATCH_INTERVAL;
    loop {
        select! { biased;
            _ = shutdown_signal.recv() => {
                if !request_buffer.is_empty() {
                    grpc_sink.send(request_buffer.take()).unwrap();
                }
                break;
            }
            _ = sleep_until(next_dispatch) => {
                if !request_buffer.is_empty() {
                    grpc_sink.send(request_buffer.take()).unwrap();
                }
                next_dispatch = Instant::now() + DISPATCH_INTERVAL;
            }
            recv = request_source.recv() => {
                if let Some((request, callback)) = recv {
                    let request: transaction::Req = request.into();
                    if let Some(callback) = callback {
                        listener.register(request.req_id.clone().into(), callback);
                    }
                    if request_buffer.len() + request.encoded_len() > MAX_GRPC_MESSAGE_LEN {
                        grpc_sink.send(request_buffer.take()).unwrap();
                    }
                    request_buffer.push(request);
                } else {
                    break;
                }
            }
        }
    }
}

#[derive(Default)]
struct TransactionRequestBuffer {
    reqs: Vec<transaction::Req>,
    len_cache: usize,
}

impl TransactionRequestBuffer {
    fn is_empty(&self) -> bool {
        self.reqs.is_empty()
    }

    fn len(&self) -> usize {
        self.len_cache
    }

    fn push(&mut self, request: transaction::Req) {
        self.len_cache += request.encoded_len();
        self.reqs.push(request);
    }

    fn take(&mut self) -> transaction::Client {
        self.len_cache = 0;
        transaction::Client { reqs: std::mem::take(&mut self.reqs) }
    }
}

async fn transaction_listener(mut grpc_source: Streaming<transaction::Server>, listener: Listener) {
    loop {
        match grpc_source.next().await {
            Some(Ok(message)) => listener.consume(message).await,
            Some(Err(err)) => {
                break listener
                    .close(ClientError::TransactionIsClosedWithErrors(err.to_string()))
                    .await
            }
            None => break listener.close(ClientError::TransactionIsClosed()).await,
        }
    }
}

#[derive(Clone)]
struct Listener {
    request_sink: UnboundedSender<(TransactionRequest, Option<TransactionCallback>)>,
    listeners: Arc<RwLock<HashMap<RequestID, TransactionCallback>>>,
    is_open: Arc<AtomicCell<bool>>,
}

impl Listener {
    fn register(&mut self, request_id: RequestID, callback: TransactionCallback) {
        self.listeners.write().unwrap().insert(request_id, callback);
    }

    async fn consume(&self, message: transaction::Server) {
        match message.server {
            Some(Server::Res(res)) => self.consume_res(res),
            Some(Server::ResPart(res_part)) => self.consume_res_part(res_part).await,
            None => println!("{}", ClientError::MissingResponseField("server")),
        }
    }

    fn consume_res(&self, res: transaction::Res) {
        let req_id = res.req_id.clone().into();
        match self.listeners.write().unwrap().remove(&req_id) {
            Some(TransactionCallback::OneShot(sink)) => sink.send(Ok(res.into())).unwrap(),
            _ => {
                if !matches!(res.res.unwrap(), transaction::res::Res::OpenRes(_)) {
                    println!("{}", ClientError::UnknownRequestId(req_id));
                }
            }
        }
    }

    async fn consume_res_part(&self, res_part: transaction::ResPart) {
        let request_id = res_part.req_id.clone().into();

        match res_part.res {
            Some(transaction::res_part::Res::StreamResPart(stream_res_part)) => {
                match State::from_i32(stream_res_part.state).expect("enum out of range") {
                    State::Done => {
                        self.listeners.write().unwrap().remove(&request_id);
                    }
                    State::Continue => {
                        self.request_sink
                            .send((TransactionRequest::Stream { request_id }, None))
                            .unwrap();
                    }
                }
            }
            Some(_) => match self.listeners.read().unwrap().get(&request_id) {
                Some(TransactionCallback::Streamed(sink)) => {
                    sink.send(Ok(res_part.into())).ok();
                }
                _ => {
                    println!("{}", ClientError::UnknownRequestId(request_id));
                }
            },
            None => panic!("{}", ClientError::MissingResponseField("res_part.res")),
        }
    }

    async fn close(self, error: ClientError) {
        self.is_open.store(false);
        let mut listeners = std::mem::take(self.listeners.write().unwrap().deref_mut());
        for (_, listener) in listeners.drain() {
            listener.send_error(error.clone()).await;
        }
    }
}
