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
    future::Future,
    pin::Pin,
    result::Result as StdResult,
    sync::{Arc, Mutex},
};

use tonic::{
    codegen::InterceptedService, service::Interceptor, transport::Channel, Code, Request, Response,
    Status,
};
use typedb_protocol::{
    cluster_user, type_db_cluster_client::TypeDbClusterClient as ProtoTypeDBClusterClient,
};

use crate::{
    common::{
        rpc::builder::{cluster, cluster::user::token_req},
        Executor, Result,
    },
};
use crate::common::Credential;

#[derive(Clone, Debug)]
struct CallCredentials {
    credential: Credential,
    token: Option<String>,
}

impl CallCredentials {
    fn new(credential: Credential) -> Self {
        Self { credential, token: None }
    }

    fn set_token(&mut self, token: String) {
        self.token = Some(token);
    }

    fn reset_token(&mut self) {
        self.token = None;
    }

    fn inject(&self, mut request: Request<()>) -> Request<()> {
        dbg!(self);
        request.metadata_mut().insert("username", self.credential.username().try_into().unwrap());
        match &self.token {
            Some(token) => request.metadata_mut().insert("token", token.try_into().unwrap()),
            None => request
                .metadata_mut()
                .insert("password", self.credential.password().try_into().unwrap()),
        };
        request
    }
}

#[derive(Clone, Debug)]
struct CredentialInjector {
    call_credentials: Arc<Mutex<CallCredentials>>,
}

impl CredentialInjector {
    fn new(credential_handler: Arc<Mutex<CallCredentials>>) -> Self {
        Self { call_credentials: credential_handler }
    }
}

impl Interceptor for CredentialInjector {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        Ok(self.call_credentials.lock().unwrap().inject(request))
    }
}

pub(crate) type CallCredChannel = InterceptedService<Channel, CredentialInjector>;
pub(crate) type TonicResult<R> = StdResult<Response<R>, Status>;

#[derive(Clone, Debug)]
pub(crate) struct ClusterClient {
    cluster_client: ProtoTypeDBClusterClient<CallCredChannel>,
    pub(crate) executor: Arc<Executor>,
    credential_handler: Arc<Mutex<CallCredentials>>,
}

impl ClusterClient {
    pub(crate) async fn new(channel: Channel, credential: Credential) -> Result<Self> {
        let shared_cred = Arc::new(Mutex::new(CallCredentials::new(credential)));
        Self::construct(
            ProtoTypeDBClusterClient::with_interceptor(
                channel,
                CredentialInjector::new(shared_cred.clone()),
            ),
            shared_cred,
        )
        .await
    }

    async fn construct(
        cluster_client: ProtoTypeDBClusterClient<CallCredChannel>,
        credential_handler: Arc<Mutex<CallCredentials>>,
    ) -> Result<Self> {
        // TODO: temporary hack to validate connection until we have client pulse
        let mut this = Self {
            cluster_client,
            executor: Arc::new(Executor::new().expect("Failed to create Executor")),
            credential_handler,
        };
        this.renew_token().await?;
        this.check_connection().await?;
        Ok(this)
    }

    async fn check_connection(&mut self) -> Result<()> {
        self.cluster_client.databases_all(cluster::database_manager::all_req()).await?;
        Ok(())
    }

    async fn renew_token(&mut self) -> Result {
        self.credential_handler.lock().unwrap().reset_token();
        let req = token_req(self.credential_handler.lock().unwrap().credential.username());
        let token = self.user_token(req).await?.token;
        dbg!(&token);
        self.credential_handler.lock().unwrap().set_token(token);
        Ok(())
    }

    async fn may_renew_token<'me, F, R>(&'me mut self, call: F) -> Result<R>
    where
        for<'a> F: Fn(&'a mut Self) -> Pin<Box<dyn Future<Output = TonicResult<R>> + 'a>>,
    {
        match call(self).await {
            // TODO proper error conversions
            Err(err) if err.code() == Code::Unauthenticated => {
                self.renew_token().await?;
                Ok(call(self).await?.into_inner())
            }
            res => Ok(res?.into_inner()),
        }
    }

    pub(crate) async fn user_token(
        &mut self,
        username: cluster_user::token::Req,
    ) -> Result<cluster_user::token::Res> {
        Ok(self.cluster_client.user_token(username).await?.into_inner())
    }

    pub(crate) async fn servers_all(
        &mut self,
    ) -> Result<typedb_protocol::server_manager::all::Res> {
        self.may_renew_token(|this| {
            Box::pin(this.cluster_client.servers_all(cluster::server_manager::all_req()))
        })
        .await
    }

    pub(crate) async fn databases_get(
        &mut self,
        req: typedb_protocol::cluster_database_manager::get::Req,
    ) -> Result<typedb_protocol::cluster_database_manager::get::Res> {
        self.may_renew_token(|this| Box::pin(this.cluster_client.databases_get(req.clone()))).await
    }
}
