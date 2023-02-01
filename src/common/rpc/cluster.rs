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

use std::sync::Arc;

use async_trait::async_trait;
use futures::{future::BoxFuture, FutureExt};
use typedb_protocol::{
    cluster_database_manager, cluster_user, server_manager,
    type_db_client::TypeDbClient as CoreGRPC,
    type_db_cluster_client::TypeDbClusterClient as ClusterGRPC,
};

use super::{
    channel::{open_encrypted_channel, CallCredChannel},
    server::ServerRPC,
    TonicResult,
};
use crate::common::{
    credential::CallCredentials, error::ClientError, Address, Credential, Error, Result,
};

#[derive(Clone, Debug)]
pub(crate) struct ClusterServerRPC {
    core_grpc: CoreGRPC<CallCredChannel>,
    cluster_grpc: ClusterGRPC<CallCredChannel>,
    call_credentials: Arc<CallCredentials>,
}

impl ClusterServerRPC {
    pub(crate) async fn new(address: Address, credential: Credential) -> Result<Self> {
        let (channel, call_credentials) = open_encrypted_channel(address, credential)?;
        let mut this = Self {
            core_grpc: CoreGRPC::new(channel.clone()),
            cluster_grpc: ClusterGRPC::new(channel),
            call_credentials,
        };
        this.renew_token().await?;
        Ok(this)
    }

    pub(super) async fn validated(self) -> Result<Self> {
        // self.databases_all(cluster::database_manager::all_req()).await?;
        Ok(self)
    }

    async fn call_with_auto_renew_token<F, R>(&mut self, call: F) -> Result<R>
    where
        for<'a> F: Fn(&'a mut Self) -> BoxFuture<'a, Result<R>>,
    {
        if self.call_credentials.has_token() {
            match call(self).await {
                Err(Error::Client(ClientError::ClusterTokenCredentialInvalid())) => (),
                res => return res,
            }
        }
        self.renew_token().await?;
        call(self).await
    }

    async fn renew_token(&mut self) -> Result {
        self.call_credentials.reset_token();
        let req =
            cluster_user::token::Req { username: self.call_credentials.username().to_owned() };
        let token = self.user_token(req).await?.token;
        self.call_credentials.set_token(token);
        Ok(())
    }

    async fn user_token(
        &mut self,
        username: cluster_user::token::Req,
    ) -> Result<cluster_user::token::Res> {
        Ok(self.cluster_grpc.user_token(username).await?.into_inner())
    }

    pub(crate) async fn servers_all(
        &mut self,
        req: server_manager::all::Req,
    ) -> Result<server_manager::all::Res> {
        self.single(|this| Box::pin(this.cluster_grpc.servers_all(req.clone()))).await
    }

    pub(crate) async fn databases_get(
        &mut self,
        req: cluster_database_manager::get::Req,
    ) -> Result<cluster_database_manager::get::Res> {
        self.single(|this| Box::pin(this.cluster_grpc.databases_get(req.clone()))).await
    }

    pub(crate) async fn databases_all(
        &mut self,
        req: cluster_database_manager::all::Req,
    ) -> Result<cluster_database_manager::all::Res> {
        self.single(|this| Box::pin(this.cluster_grpc.databases_all(req.clone()))).await
    }
}

#[async_trait]
impl ServerRPC<CallCredChannel> for ClusterServerRPC {
    async fn single<F, R>(&mut self, call: F) -> Result<R>
    where
        for<'a> F: Fn(&'a mut Self) -> BoxFuture<'a, TonicResult<R>> + Send + Sync,
        R: 'static,
    {
        self.call_with_auto_renew_token(|this| Box::pin(call(this).map(|r| Ok(r?.into_inner()))))
            .await
    }

    fn core_grpc_mut(&mut self) -> &mut CoreGRPC<CallCredChannel> {
        &mut self.core_grpc
    }
}
