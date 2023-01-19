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
    mem,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use crossbeam::{
    atomic::AtomicCell,
    channel::{
        bounded, unbounded, Receiver as CrossbeamReceiver, Sender as CrossbeamSender, TryRecvError,
    },
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
    RequestID, Result,
};

#[derive(Clone, Debug)]
pub(crate) struct TransactionRPC {
    sender: Sender,
    receiver: Receiver,
}

impl TransactionRPC {
    pub(crate) async fn new(mut server_rpc: ServerRPC, open_req: transaction::Req) -> Result<Self> {
        let (req_sink, streaming_res) = server_rpc.transaction(open_req).await?;
        Ok(TransactionRPC { sender: Sender::new(req_sink), receiver: Receiver::new(streaming_res) })
    }

    pub(crate) async fn single_async(&mut self, req: transaction::Req) -> Result<transaction::Res> {
        let recv = self.send_request(req)?;
        loop {
            match recv.try_recv() {
                Ok(res) => match res {
                    Ok(result) => break Ok(result),
                    Err(err) => Err(Error::new(err.to_string()))?,
                },
                Err(TryRecvError::Disconnected) => Err(ClientError::TransactionIsClosed())?,
                Err(TryRecvError::Empty) => sleep(Duration::from_millis(3)).await, // TODO constant
            }
        }
    }

    pub(crate) fn single_blocking(&mut self, req: transaction::Req) -> Result<transaction::Res> {
        match self.send_request(req)?.recv() {
            Ok(result) => result,
            Err(err) => Err(Error::new(err.to_string())),
        }
    }

    fn send_request(
        &mut self,
        req: transaction::Req,
    ) -> Result<CrossbeamReceiver<Result<transaction::Res>>> {
        if !self.is_open() {
            return Err(ClientError::TransactionIsClosed().into());
        }
        let (res_sink, res_receiver) = bounded::<Result<transaction::Res>>(1);
        self.receiver.add_single(req.req_id.clone().into(), res_sink);
        self.sender.submit_message(req);
        Ok(res_receiver)
    }

    pub(crate) fn stream(&mut self, req: transaction::Req) -> ResPartStream {
        const BUFFER_SIZE: usize = 1024;
        let (res_part_sink, res_part_receiver) = bounded(BUFFER_SIZE);
        let (stream_req_sink, stream_req_receiver) = unbounded::<transaction::Req>();
        let req_id: RequestID = req.req_id.clone().into();
        self.receiver.add_stream(req_id.clone(), res_part_sink);
        let res_part_stream = ResPartStream::new(res_part_receiver, stream_req_sink, req_id);
        self.sender.add_message_provider(stream_req_receiver);
        self.sender.submit_message(req);
        res_part_stream
    }

    pub(crate) fn is_open(&self) -> bool {
        self.sender.is_open()
    }

    pub(crate) fn close(&self) {
        self.sender.close(None);
        self.receiver.close(None);
    }
}

#[derive(Clone, Debug)]
struct Sender {
    state: Arc<SenderState>,
}

impl Sender {
    pub(crate) fn new(req_sink: CrossbeamSender<transaction::Client>) -> Self {
        let state = Arc::new(SenderState::new(req_sink));
        spawn(state.clone().dispatch_loop());
        Sender { state }
    }

    fn submit_message(&self, req: transaction::Req) {
        self.state.submit_message(req);
    }

    fn add_message_provider(&self, provider: CrossbeamReceiver<transaction::Req>) {
        let cloned_state = self.state.clone();
        spawn(async move {
            loop {
                match provider.try_recv() {
                    Ok(req) => cloned_state.submit_message(req),
                    Err(TryRecvError::Disconnected) => break,
                    Err(TryRecvError::Empty) => sleep(Duration::from_millis(3)).await, // TODO constant
                }
            }
        });
    }

    fn is_open(&self) -> bool {
        self.state.is_open.load()
    }

    fn close(&self, error: Option<Error>) {
        self.state.close(error)
    }
}

#[derive(Debug)]
struct SenderState {
    req_sink: CrossbeamSender<transaction::Client>,
    // TODO: refactor to crossbeam_queue::ArrayQueue?
    queued_messages: Mutex<Vec<transaction::Req>>,
    // TODO: refactor to message passing for these atomics
    ongoing_task_count: AtomicCell<u8>,
    is_open: AtomicCell<bool>,
}

impl SenderState {
    fn new(req_sink: CrossbeamSender<transaction::Client>) -> Self {
        SenderState {
            req_sink,
            queued_messages: Mutex::new(Vec::new()),
            ongoing_task_count: AtomicCell::new(0),
            is_open: AtomicCell::new(true),
        }
    }

    fn submit_message(&self, req: transaction::Req) {
        self.queued_messages.lock().unwrap().push(req);
    }

    async fn dispatch_loop(self: Arc<Self>) {
        const DISPATCH_INTERVAL: Duration = Duration::from_millis(3);
        while self.is_open.load() {
            sleep(DISPATCH_INTERVAL).await;
            self.dispatch_messages();
        }
    }

    fn dispatch_messages(&self) {
        self.ongoing_task_count.fetch_add(1);
        let messages: Vec<_> = mem::take(self.queued_messages.lock().unwrap().as_mut());
        if !messages.is_empty() {
            self.req_sink.send(client_msg(messages)).unwrap();
        }
        self.ongoing_task_count.fetch_sub(1);
    }

    fn close(&self, error: Option<Error>) {
        if self.is_open.compare_exchange(true, false).is_ok() {
            if error.is_none() {
                self.dispatch_messages();
            }
            // TODO: refactor to non-busy wait?
            // TODO: this loop should have a timeout
            loop {
                if self.ongoing_task_count.load() == 0 {
                    break;
                }
            }
        }
    }
}

impl Drop for SenderState {
    fn drop(&mut self) {
        self.close(None)
    }
}

#[derive(Clone, Debug)]
struct Receiver {
    state: Arc<ReceiverState>,
}

impl Receiver {
    fn new(grpc_stream: Streaming<transaction::Server>) -> Self {
        let state = Arc::new(ReceiverState::new());
        spawn(state.clone().listen(grpc_stream));
        Receiver { state }
    }

    fn add_single(&mut self, req_id: RequestID, res_collector: ResCollector) {
        self.state.res_collectors.lock().unwrap().insert(req_id, res_collector);
    }

    fn add_stream(&mut self, req_id: RequestID, res_part_sink: ResPartCollector) {
        self.state.res_part_collectors.lock().unwrap().insert(req_id, res_part_sink);
    }

    fn close(&self, error: Option<Error>) {
        self.state.close(error)
    }
}

#[derive(Debug)]
struct ReceiverState {
    res_collectors: Mutex<HashMap<RequestID, ResCollector>>,
    res_part_collectors: Mutex<HashMap<RequestID, ResPartCollector>>,
    is_open: AtomicCell<bool>,
}

impl ReceiverState {
    fn new() -> Self {
        Self {
            res_collectors: Mutex::new(HashMap::new()),
            res_part_collectors: Mutex::new(HashMap::new()),
            is_open: AtomicCell::new(true),
        }
    }

    async fn listen(self: Arc<Self>, mut grpc_stream: Streaming<transaction::Server>) {
        loop {
            match grpc_stream.next().await {
                Some(Ok(message)) => {
                    self.clone().on_receive(message).await;
                }
                Some(Err(err)) => {
                    self.close(Some(err.into()));
                    break;
                }
                None => {
                    self.close(None);
                    break;
                }
            }
        }
    }

    async fn on_receive(&self, message: transaction::Server) {
        // TODO: If an error occurs here (or in some other background process), resources are not
        //  properly cleaned up, and the application may hang.
        match message.server {
            Some(Server::Res(res)) => self.collect_res(res),
            Some(Server::ResPart(res_part)) => {
                self.collect_res_part(res_part);
            }
            None => println!("{}", ClientError::MissingResponseField("server")),
        }
    }

    fn collect_res(&self, res: transaction::Res) {
        let req_id = res.req_id.clone().into();
        match self.res_collectors.lock().unwrap().remove(&req_id) {
            Some(collector) => collector.send(Ok(res)).unwrap(),
            None => {
                if let Res::OpenRes(_) = res.res.unwrap() {
                    // ignore open_res
                } else {
                    println!("{}", ClientError::UnknownRequestId(req_id))
                }
            }
        }
    }

    fn collect_res_part(&self, res_part: transaction::ResPart) {
        let req_id = res_part.req_id.clone().into();
        let value = self.res_part_collectors.lock().unwrap().remove(&req_id);
        match value {
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
                Some(error) => ClientError::TransactionIsClosedWithErrors(error.to_string()),
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

type ResCollector = CrossbeamSender<Result<transaction::Res>>;
type ResPartCollector = CrossbeamSender<Result<transaction::ResPart>>;

#[derive(Debug)]
pub(crate) struct ResPartStream {
    source: CrossbeamReceiver<Result<transaction::ResPart>>,
    stream_req_sink: CrossbeamSender<transaction::Req>,
    req_id: RequestID,
}

impl ResPartStream {
    fn new(
        source: CrossbeamReceiver<Result<transaction::ResPart>>,
        stream_req_sink: CrossbeamSender<transaction::Req>,
        req_id: RequestID,
    ) -> Self {
        ResPartStream { source, stream_req_sink, req_id }
    }
}

impl Stream for ResPartStream {
    type Item = Result<transaction::ResPart>;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.source.try_recv();
        match poll {
            Ok(Ok(res_part)) => {
                match &res_part.res {
                    Some(res_part::Res::StreamResPart(stream_res_part)) => {
                        // TODO: unwrap -> expect("enum out of range")
                        match State::from_i32(stream_res_part.state).unwrap() {
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
                }
            }
            Ok(err @ Err(_)) => Poll::Ready(Some(err)),
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
            Err(TryRecvError::Empty) => {
                ctx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
