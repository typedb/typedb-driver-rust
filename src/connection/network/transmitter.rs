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
    ops::DerefMut,
    sync::{Arc, RwLock},
    time::Duration,
};

use crossbeam::{
    atomic::AtomicCell,
    channel::{bounded as bounded_blocking, Receiver as SyncReceiver, Sender as SyncSender},
};
use futures::{Stream, StreamExt, TryStreamExt};
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
    address::Address,
    channel::{open_encrypted_channel, open_plaintext_channel, GRPCChannel},
    message::{Request, Response, TransactionRequest},
    stub::RPCStub,
};
use crate::{
    common::{error::ClientError, RequestID, Result},
    connection::{runtime::BackgroundRuntime, TransactionResponse},
    error::InternalError,
    Credential,
};

fn oneshot_blocking<T>() -> (SyncSender<T>, SyncReceiver<T>) {
    bounded_blocking::<T>(0)
}

#[derive(Debug)]
enum OneShotSender<T> {
    Async(AsyncOneshotSender<Result<T>>),
    Blocking(SyncSender<Result<T>>),
}

impl<T> OneShotSender<T> {
    fn send(self, response: Result<T>) -> Result {
        match self {
            Self::Async(sink) => sink.send(response).map_err(|_| InternalError::SendError().into()),
            Self::Blocking(sink) => sink.send(response).map_err(Into::into),
        }
    }
}

pub(in crate::connection) struct RPCTransmitter {
    request_sink: UnboundedSender<(Request, OneShotSender<Response>)>,
    shutdown_sink: UnboundedSender<()>,
}

impl RPCTransmitter {
    pub(in crate::connection) fn start_plaintext(
        address: Address,
        runtime: &BackgroundRuntime,
    ) -> Self {
        let (request_sink, request_source) = unbounded_async();
        let (shutdown_sink, shutdown_source) = unbounded_async();
        runtime.spawn(async move {
            let channel = open_plaintext_channel(address.clone());
            let rpc = RPCStub::new(address.clone(), channel, None).await.unwrap();
            Self::dispatcher_loop(rpc, request_source, shutdown_source).await;
        });
        Self { request_sink, shutdown_sink }
    }

    pub(in crate::connection) fn start_encrypted(
        address: Address,
        credential: Credential,
        runtime: &BackgroundRuntime,
    ) -> Self {
        let (request_sink, request_source) = unbounded_async();
        let (shutdown_sink, shutdown_source) = unbounded_async();
        runtime.spawn(async move {
            let (channel, callcreds) = open_encrypted_channel(address.clone(), credential).unwrap();
            let rpc = RPCStub::new(address.clone(), channel, Some(callcreds)).await.unwrap();
            Self::dispatcher_loop(rpc, request_source, shutdown_source).await;
        });
        Self { request_sink, shutdown_sink }
    }

    pub(in crate::connection) async fn request_async(&self, request: Request) -> Result<Response> {
        let (response_sink, response) = oneshot_async();
        self.request_sink.send((request, OneShotSender::Async(response_sink)))?;
        response.await?
    }

    pub(in crate::connection) fn request_blocking(&self, request: Request) -> Result<Response> {
        let (response_sink, response) = oneshot_blocking();
        self.request_sink.send((request, OneShotSender::Blocking(response_sink)))?;
        response.recv()?
    }

    pub(in crate::connection) fn force_close(&self) {
        self.shutdown_sink.send(()).ok();
    }

    async fn dispatcher_loop<Channel: GRPCChannel>(
        rpc: RPCStub<Channel>,
        mut request_source: UnboundedReceiver<(Request, OneShotSender<Response>)>,
        mut shutdown_signal: UnboundedReceiver<()>,
    ) {
        while let Some((request, response_sink)) = select! {
            request = request_source.recv() => request,
            _ = shutdown_signal.recv() => None,
        } {
            let rpc = rpc.clone();
            tokio::spawn(async move {
                let response = Self::send_request(rpc, request).await;
                response_sink.send(response).ok();
            });
        }
    }

    async fn send_request<Channel: GRPCChannel>(
        mut rpc: RPCStub<Channel>,
        request: Request,
    ) -> Result<Response> {
        match request {
            Request::ServersAll => {
                rpc.servers_all(request.try_into()?).await.and_then(Response::try_from)
            }

            Request::DatabasesContains { .. } => {
                rpc.databases_contains(request.try_into()?).await.map(Response::from)
            }
            Request::DatabaseCreate { .. } => {
                rpc.databases_create(request.try_into()?).await.map(Response::from)
            }
            Request::DatabaseGet { .. } => {
                rpc.databases_get(request.try_into()?).await.and_then(Response::try_from)
            }
            Request::DatabasesAll => {
                rpc.databases_all(request.try_into()?).await.map(Response::from)
            }

            Request::DatabaseDelete { .. } => {
                rpc.database_delete(request.try_into()?).await.map(Response::from)
            }
            Request::DatabaseSchema { .. } => {
                rpc.database_schema(request.try_into()?).await.map(Response::from)
            }
            Request::DatabaseTypeSchema { .. } => {
                rpc.database_type_schema(request.try_into()?).await.map(Response::from)
            }
            Request::DatabaseRuleSchema { .. } => {
                rpc.database_rule_schema(request.try_into()?).await.map(Response::from)
            }

            Request::SessionOpen { .. } => {
                rpc.session_open(request.try_into()?).await.map(Response::from)
            }
            Request::SessionPulse { .. } => {
                rpc.session_pulse(request.try_into()?).await.map(Response::from)
            }
            Request::SessionClose { .. } => {
                rpc.session_close(request.try_into()?).await.map(Response::from)
            }

            Request::Transaction(transaction_request) => {
                rpc.transaction(transaction_request.into()).await.map(Response::from)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum TransactionCallback {
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

pub(in crate::connection) struct TransactionTransmitter {
    request_sink: UnboundedSender<(TransactionRequest, Option<TransactionCallback>)>,
    is_open: Arc<AtomicCell<bool>>,
    shutdown_sink: UnboundedSender<()>,
}

impl Drop for TransactionTransmitter {
    fn drop(&mut self) {
        self.is_open.store(false);
        self.shutdown_sink.send(()).ok();
    }
}

impl TransactionTransmitter {
    pub(in crate::connection) fn new(
        background_runtime: &BackgroundRuntime,
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
        Self { request_sink: buffer_sink, is_open, shutdown_sink }
    }

    pub(in crate::connection) async fn single(
        &self,
        req: TransactionRequest,
    ) -> Result<TransactionResponse> {
        if !self.is_open.load() {
            return Err(ClientError::SessionIsClosed().into());
        }
        let (res_sink, recv) = oneshot_async();
        self.request_sink.send((req, Some(TransactionCallback::OneShot(res_sink)))).unwrap();
        recv.await.unwrap().map(Into::into)
    }

    pub(in crate::connection) fn stream(
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
}

async fn transaction_worker(
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
    const DISPATCH_INTERVAL: Duration = Duration::from_millis(3);

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
