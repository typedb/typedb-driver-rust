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
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crossbeam::{
    atomic::AtomicCell,
    channel::{unbounded, Receiver, Sender, TryRecvError},
};
use futures::Stream;
use tonic::{Response, Status, Streaming};
use typedb_protocol::{
    core_database, core_database_manager, session, transaction,
    type_db_client::TypeDbClient as RawCoreGRPC,
};

use crate::{
    async_enum_dispatch,
    common::{
        error::ClientError,
        rpc::{
            builder::{core, transaction::client_msg},
            channel::CallCredChannel,
            Channel,
        },
        Address, Result, StdResult, TonicChannel,
    },
};

#[derive(Clone, Debug)]
enum CoreGRPC {
    Plaintext(RawCoreGRPC<TonicChannel>),
    Encrypted(RawCoreGRPC<CallCredChannel>),
}

impl CoreGRPC {
    pub fn new(channel: Channel) -> Self {
        match channel {
            Channel::Plaintext(channel) => Self::Plaintext(RawCoreGRPC::new(channel)),
            Channel::Encrypted(channel) => Self::Encrypted(RawCoreGRPC::new(channel)),
        }
    }

    async_enum_dispatch! { { Plaintext, Encrypted }
        pub async fn databases_contains(
            &mut self,
            request: core_database_manager::contains::Req,
        ) -> StdResult<Response<core_database_manager::contains::Res>, Status>;

        pub async fn databases_create(
            &mut self,
            request: core_database_manager::create::Req,
        ) -> StdResult<Response<core_database_manager::create::Res>, Status>;

        pub async fn databases_all(
            &mut self,
            request: core_database_manager::all::Req,
        ) -> StdResult<Response<core_database_manager::all::Res>, Status>;

        pub async fn database_schema(
            &mut self,
            request: core_database::schema::Req,
        ) -> StdResult<Response<core_database::schema::Res>, Status>;

        pub async fn database_type_schema(
            &mut self,
            request: core_database::type_schema::Req,
        ) -> StdResult<Response<core_database::type_schema::Res>, Status>;

        pub async fn database_rule_schema(
            &mut self,
            request: core_database::rule_schema::Req,
        ) -> StdResult<Response<core_database::rule_schema::Res>, Status>;

        pub async fn database_delete(
            &mut self,
            request: core_database::delete::Req,
        ) -> StdResult<Response<core_database::delete::Res>, Status>;

        pub async fn session_open(
            &mut self,
            request: session::open::Req,
        ) -> StdResult<Response<session::open::Res>, Status>;

        pub async fn session_close(
            &mut self,
            request: session::close::Req,
        ) -> StdResult<Response<session::close::Res>, Status>;

        pub async fn session_pulse(
            &mut self,
            request: session::pulse::Req,
        ) -> StdResult<Response<session::pulse::Res>, Status>;

        pub async fn transaction(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = transaction::Client>,
        ) -> StdResult<Response<Streaming<transaction::Server>>, Status>;
    }
}

macro_rules! single {
    ($self:ident, $future:expr) => {{
        if !$self.is_open() {
            Err(ClientError::ClientIsClosed())?;
        }
        Ok($future.await?.into_inner())
    }};
}

macro_rules! bidi_stream {
    ($self:ident, $sink:ident, $future:expr) => {{
        if !$self.is_open() {
            Err(ClientError::ClientIsClosed())?;
        }
        Ok(($sink, $future.await?.into_inner()))
    }};
}

#[derive(Clone, Debug)]
pub(crate) struct CoreRPC {
    core_grpc: CoreGRPC,
    is_open: Arc<AtomicCell<bool>>,
}

impl CoreRPC {
    pub(super) fn new(channel: Channel, is_open: Arc<AtomicCell<bool>>) -> Self {
        Self { core_grpc: CoreGRPC::new(channel), is_open }
    }

    pub(crate) async fn connect(address: Address) -> Result<Self> {
        let is_open = Arc::new(AtomicCell::new(true));
        Self::new(Channel::open_plaintext(address)?, is_open).validated().await
    }

    async fn validated(mut self) -> Result<Self> {
        // TODO: temporary hack to validate connection until we have client pulse
        self.databases_all(core::database_manager::all_req()).await?;
        Ok(self)
    }

    fn is_open(&self) -> bool {
        self.is_open.load()
    }

    pub(crate) fn force_close(&self) {
        self.is_open.store(false);
    }

    pub(crate) async fn databases_contains(
        &mut self,
        req: core_database_manager::contains::Req,
    ) -> Result<core_database_manager::contains::Res> {
        single!(self, self.core_grpc.databases_contains(req))
    }

    pub(crate) async fn databases_create(
        &mut self,
        req: core_database_manager::create::Req,
    ) -> Result<core_database_manager::create::Res> {
        single!(self, self.core_grpc.databases_create(req))
    }

    pub(crate) async fn databases_all(
        &mut self,
        req: core_database_manager::all::Req,
    ) -> Result<core_database_manager::all::Res> {
        single!(self, self.core_grpc.databases_all(req))
    }

    pub(crate) async fn database_delete(
        &mut self,
        req: core_database::delete::Req,
    ) -> Result<core_database::delete::Res> {
        single!(self, self.core_grpc.database_delete(req))
    }

    pub(crate) async fn database_schema(
        &mut self,
        req: core_database::schema::Req,
    ) -> Result<core_database::schema::Res> {
        single!(self, self.core_grpc.database_schema(req))
    }

    pub(crate) async fn database_type_schema(
        &mut self,
        req: core_database::type_schema::Req,
    ) -> Result<core_database::type_schema::Res> {
        single!(self, self.core_grpc.database_type_schema(req))
    }

    pub(crate) async fn database_rule_schema(
        &mut self,
        req: core_database::rule_schema::Req,
    ) -> Result<core_database::rule_schema::Res> {
        single!(self, self.core_grpc.database_rule_schema(req))
    }

    pub(crate) async fn session_open(
        &mut self,
        req: session::open::Req,
    ) -> Result<session::open::Res> {
        single!(self, self.core_grpc.session_open(req))
    }

    pub(crate) async fn session_close(
        &mut self,
        req: session::close::Req,
    ) -> Result<session::close::Res> {
        single!(self, self.core_grpc.session_close(req))
    }

    pub(crate) async fn session_pulse(
        &mut self,
        req: session::pulse::Req,
    ) -> Result<session::pulse::Res> {
        single!(self, self.core_grpc.session_pulse(req))
    }

    pub(crate) async fn transaction(
        &mut self,
        open_req: transaction::Req,
    ) -> Result<(Sender<transaction::Client>, Streaming<transaction::Server>)> {
        let (sender, receiver) = unbounded();
        sender.send(client_msg(vec![open_req])).unwrap();
        bidi_stream!(self, sender, self.core_grpc.transaction(ReceiverStream::from(receiver)))
    }
}

struct ReceiverStream<T> {
    recv: Receiver<T>,
}

impl<T> From<Receiver<T>> for ReceiverStream<T> {
    fn from(recv: Receiver<T>) -> Self {
        Self { recv }
    }
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.recv.try_recv();
        match poll {
            Ok(res) => Poll::Ready(Some(res)),
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
            Err(TryRecvError::Empty) => {
                ctx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
