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
    task::{Context, Poll},
};

use async_trait::async_trait;
use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};
use futures::{future::BoxFuture, Stream, TryFutureExt};
use tonic::{Response, Streaming};
use typedb_protocol::{
    core_database, core_database_manager, session, transaction,
    type_db_client::TypeDbClient as CoreGRPC,
};

use super::{channel::GRPCChannel, TonicResult};
use crate::common::Result;

#[async_trait]
pub(super) trait ServerRPC<Channel: GRPCChannel>: Clone + Send + 'static {
    async fn single<F, R>(&mut self, call: F) -> Result<R>
    where
        for<'a> F: Fn(&'a mut Self) -> BoxFuture<'a, TonicResult<R>> + Send + Sync,
        R: 'static;

    fn core_grpc_mut(&mut self) -> &mut CoreGRPC<Channel>;

    async fn databases_contains(
        &mut self,
        req: core_database_manager::contains::Req,
    ) -> Result<core_database_manager::contains::Res> {
        self.single(|this| Box::pin(this.core_grpc_mut().databases_contains(req.clone()))).await
    }

    async fn databases_create(
        &mut self,
        req: core_database_manager::create::Req,
    ) -> Result<core_database_manager::create::Res> {
        self.single(|this| Box::pin(this.core_grpc_mut().databases_create(req.clone()))).await
    }

    async fn database_delete(
        &mut self,
        req: core_database::delete::Req,
    ) -> Result<core_database::delete::Res> {
        self.single(|this| Box::pin(this.core_grpc_mut().database_delete(req.clone()))).await
    }

    async fn database_schema(
        &mut self,
        req: core_database::schema::Req,
    ) -> Result<core_database::schema::Res> {
        self.single(|this| Box::pin(this.core_grpc_mut().database_schema(req.clone()))).await
    }

    async fn database_type_schema(
        &mut self,
        req: core_database::type_schema::Req,
    ) -> Result<core_database::type_schema::Res> {
        self.single(|this| Box::pin(this.core_grpc_mut().database_type_schema(req.clone()))).await
    }

    async fn database_rule_schema(
        &mut self,
        req: core_database::rule_schema::Req,
    ) -> Result<core_database::rule_schema::Res> {
        self.single(|this| Box::pin(this.core_grpc_mut().database_rule_schema(req.clone()))).await
    }

    async fn session_open(&mut self, req: session::open::Req) -> Result<session::open::Res> {
        self.single(|this| Box::pin(this.core_grpc_mut().session_open(req.clone()))).await
    }

    async fn session_close(&mut self, req: session::close::Req) -> Result<session::close::Res> {
        self.single(|this| Box::pin(this.core_grpc_mut().session_close(req.clone()))).await
    }

    async fn session_pulse(&mut self, req: session::pulse::Req) -> Result<session::pulse::Res> {
        self.single(|this| Box::pin(this.core_grpc_mut().session_pulse(req.clone()))).await
    }

    async fn transaction(
        &mut self,
        open_req: transaction::Req,
    ) -> Result<(Sender<transaction::Client>, Streaming<transaction::Server>)> {
        self.single(|this| {
            let (sender, receiver) = unbounded();
            sender.send(transaction::Client { reqs: vec![open_req.clone()] }).unwrap();
            Box::pin(
                this.core_grpc_mut()
                    .transaction(ReceiverStream::from(receiver))
                    .map_ok(|stream| Response::new((sender, stream.into_inner()))),
            )
        })
        .await
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
