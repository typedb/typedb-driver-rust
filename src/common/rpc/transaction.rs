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
    fmt,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use crossbeam::{
    atomic::AtomicCell,
    channel::{bounded, unbounded, Receiver, Sender, TryRecvError},
};
use futures::{Stream, StreamExt};
use tokio::{spawn, time::sleep};
use tonic::Streaming;
use typedb_protocol::{
    transaction,
    transaction::{res::Res, res_part, server::Server, stream::State},
};

use crate::common::{
    error::{ClientError, Error},
    rpc::{
        builder::transaction::{client_msg, stream_req},
        ServerRPC,
    },
    DropGuard, RequestID, Result, DISPATCH_INTERVAL, POLL_INTERVAL,
};

#[derive(Clone)]
pub(crate) struct TransactionRPC {
    batching_dispatcher: Arc<BatchingDispatcher>,
    _drop_guard: Arc<DropGuard>,
}

impl fmt::Debug for TransactionRPC {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionRPC")
            .field("batching_dispatcher", &self.batching_dispatcher)
            .finish()
    }
}

impl TransactionRPC {
    pub(crate) async fn new(mut server_rpc: ServerRPC, open_req: transaction::Req) -> Result<Self> {
        let (request_sink, grpc_stream) = server_rpc.transaction(open_req).await?;
        let batching_dispatcher = Arc::new(BatchingDispatcher::new(request_sink).await?);

        spawn(Self::listen_loop(batching_dispatcher.clone(), grpc_stream));
        spawn(Self::dispatch_loop(batching_dispatcher.clone()));

        let _drop_guard = {
            let batching_dispatcher = batching_dispatcher.clone();
            Arc::new(DropGuard::call_function(move || batching_dispatcher.close(None)))
        };

        Ok(Self { batching_dispatcher, _drop_guard })
    }

    pub(crate) async fn single(&mut self, req: transaction::Req) -> Result<transaction::Res> {
        self.batching_dispatcher.single(req).await.map_err(|err| Error::new(err.to_string()).into())
    }

    pub(crate) fn stream(
        &mut self,
        req: transaction::Req,
    ) -> Result<impl Stream<Item = Result<transaction::ResPart>>> {
        self.batching_dispatcher.stream(req)
    }

    async fn dispatch_loop(batching_dispatcher: Arc<BatchingDispatcher>) {
        while batching_dispatcher.is_open() {
            sleep(DISPATCH_INTERVAL).await;
            batching_dispatcher.dispatch();
        }
    }

    async fn listen_loop(
        batching_dispatcher: Arc<BatchingDispatcher>,
        mut grpc_stream: Streaming<transaction::Server>,
    ) {
        while batching_dispatcher.is_open() {
            match grpc_stream.next().await {
                Some(Ok(message)) => batching_dispatcher.on_receive(message),
                Some(Err(err)) => {
                    batching_dispatcher.close(Some(err.into()));
                    break;
                }
                None => {
                    batching_dispatcher.close(None);
                    break;
                }
            }
            sleep(POLL_INTERVAL).await;
        }
    }
}

#[derive(Debug)]
struct BatchingDispatcher {
    request_sink: Sender<transaction::Req>,
    request_queue: RequestQueue,
    is_open: Arc<AtomicCell<bool>>,

    res_collectors: Mutex<HashMap<RequestID, Sender<Result<transaction::Res>>>>,
    res_part_collectors: Mutex<HashMap<RequestID, Sender<Result<transaction::ResPart>>>>,
}

impl BatchingDispatcher {
    async fn new(queue_sink: Sender<transaction::Client>) -> Result<Self> {
        let (request_sink, queue_source) = unbounded();
        let request_queue = RequestQueue { queue_source, queue_sink };

        Ok(Self {
            request_sink,
            request_queue,
            is_open: Arc::new(AtomicCell::new(true)),
            res_collectors: Default::default(),
            res_part_collectors: Default::default(),
        })
    }

    fn is_open(&self) -> bool {
        self.is_open.load()
    }

    fn dispatch(&self) {
        self.request_queue.dispatch()
    }

    async fn single(&self, req: transaction::Req) -> Result<transaction::Res> {
        if !self.is_open() {
            return Err(ClientError::TransactionIsClosed().into());
        }
        let (res_sink, recv) = bounded(1);
        self.res_collectors.lock().unwrap().insert(req.req_id.clone().into(), res_sink);
        self.request_sink.send(req).unwrap();
        async_recv(recv).await
    }

    fn stream(
        &self,
        req: transaction::Req,
    ) -> Result<impl Stream<Item = Result<transaction::ResPart>>> {
        if !self.is_open() {
            return Err(ClientError::TransactionIsClosed().into());
        }
        const BUFFER_SIZE: usize = 1024;
        let (res_part_sink, recv) = bounded(BUFFER_SIZE);
        let req_id: RequestID = req.req_id.clone().into();
        self.res_part_collectors.lock().unwrap().insert(req_id.clone(), res_part_sink);
        self.request_sink.send(req).unwrap();
        Ok(ResPartStream::new(recv, self.request_sink.clone(), req_id))
    }

    fn on_receive(&self, message: transaction::Server) {
        match message.server {
            Some(Server::Res(res)) => self.collect_res(res),
            Some(Server::ResPart(res_part)) => self.collect_res_part(res_part),
            None => println!("{}", ClientError::MissingResponseField("server")),
        }
    }

    fn collect_res(&self, res: transaction::Res) {
        let req_id = res.req_id.clone().into();
        match self.res_collectors.lock().unwrap().remove(&req_id) {
            Some(collector) => collector.send(Ok(res)).unwrap(),
            None => {
                if !matches!(res.res.unwrap(), Res::OpenRes(_)) {
                    println!("{}", ClientError::UnknownRequestId(req_id))
                }
            }
        }
    }

    fn collect_res_part(&self, res_part: transaction::ResPart) {
        let req_id = res_part.req_id.clone().into();
        let collector = self.res_part_collectors.lock().unwrap().remove(&req_id);
        match collector {
            Some(collector) => {
                if collector.send(Ok(res_part)).is_ok() {
                    self.res_part_collectors.lock().unwrap().insert(req_id, collector);
                }
            }
            None => {
                println!("{}", ClientError::UnknownRequestId(req_id));
            }
        }
    }

    fn close(&self, error: Option<Error>) {
        if self.is_open.compare_exchange(true, false).is_ok() {
            let error: Error = match error {
                Some(error) => {
                    self.dispatch();
                    ClientError::TransactionIsClosedWithErrors(error.to_string())
                }
                None => ClientError::TransactionIsClosed(),
            }
            .into();

            for (_, collector) in self.res_collectors.lock().unwrap().drain() {
                collector.send(Err(error.clone())).ok();
            }
            for (_, collector) in self.res_part_collectors.lock().unwrap().drain() {
                collector.send(Err(error.clone())).ok();
            }
        }
    }
}

async fn async_recv<T>(recv: Receiver<Result<T>>) -> Result<T> {
    loop {
        match recv.try_recv() {
            Ok(res) => break res,
            Err(TryRecvError::Disconnected) => Err(ClientError::TransactionIsClosed())?,
            Err(TryRecvError::Empty) => sleep(POLL_INTERVAL).await,
        }
    }
}

#[derive(Clone, Debug)]
struct RequestQueue {
    queue_source: Receiver<transaction::Req>,
    queue_sink: Sender<transaction::Client>,
}

impl RequestQueue {
    fn dispatch(&self) {
        if !self.queue_source.is_empty() {
            self.queue_sink.send(client_msg(self.queue_source.try_iter().collect())).unwrap();
        }
    }
}

#[derive(Debug)]
struct ResPartStream {
    source: Receiver<Result<transaction::ResPart>>,
    stream_req_sink: Sender<transaction::Req>,
    req_id: RequestID,
}

impl ResPartStream {
    fn new(
        source: Receiver<Result<transaction::ResPart>>,
        stream_req_sink: Sender<transaction::Req>,
        req_id: RequestID,
    ) -> Self {
        Self { source, stream_req_sink, req_id }
    }
}

impl Stream for ResPartStream {
    type Item = Result<transaction::ResPart>;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.source.try_recv();
        match poll {
            Ok(Ok(res_part)) => match &res_part.res {
                Some(res_part::Res::StreamResPart(stream_res_part)) => {
                    match State::from_i32(stream_res_part.state).expect("enum out of range") {
                        State::Done => Poll::Ready(None),
                        State::Continue => {
                            self.stream_req_sink.send(stream_req(self.req_id.clone())).unwrap();
                            ctx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    }
                }
                Some(_) => Poll::Ready(Some(Ok(res_part))),
                None => panic!("{}", ClientError::MissingResponseField("res_part.res")),
            },
            Ok(err @ Err(_)) => Poll::Ready(Some(err)),
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
            Err(TryRecvError::Empty) => {
                ctx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
