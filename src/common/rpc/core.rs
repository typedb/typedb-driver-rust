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

use async_trait::async_trait;
use futures::{future::BoxFuture, FutureExt};
use typedb_protocol::{core_database_manager, type_db_client::TypeDbClient as CoreGRPC};

use super::{
    channel::{open_plaintext_channel, PlainTextChannel},
    message::Request,
    server::ServerRPC,
    TonicResult,
};
use crate::common::{Address, Result};

#[derive(Clone, Debug)]
pub(crate) struct CoreRPC {
    core_grpc: CoreGRPC<PlainTextChannel>,
}

impl CoreRPC {
    fn new(channel: PlainTextChannel) -> Self {
        Self { core_grpc: CoreGRPC::new(channel) }
    }

    pub(crate) async fn connect(address: Address) -> Result<Self> {
        Self::new(open_plaintext_channel(address)).validated().await
    }

    async fn validated(mut self) -> Result<Self> {
        // TODO: temporary hack to validate connection until we have client pulse
        self.databases_all(Request::DatabasesAll {}.into()).await?;
        Ok(self)
    }

    pub(super) async fn databases_all(
        &mut self,
        req: core_database_manager::all::Req,
    ) -> Result<core_database_manager::all::Res> {
        self.single(|this| Box::pin(this.core_grpc_mut().databases_all(req.clone()))).await
    }
}

#[async_trait]
impl ServerRPC<PlainTextChannel> for CoreRPC {
    async fn single<F, R>(&mut self, call: F) -> Result<R>
    where
        for<'a> F: Fn(&'a mut Self) -> BoxFuture<'a, TonicResult<R>> + Send + Sync,
        R: 'static,
    {
        call(self).map(|r| Ok(r?.into_inner())).await
    }

    fn core_grpc_mut(&mut self) -> &mut CoreGRPC<PlainTextChannel> {
        &mut self.core_grpc
    }
}
