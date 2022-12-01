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

use std::{fmt::Debug, future::Future, sync::Arc};

use futures::{channel::mpsc, SinkExt};
use tonic::{
    codegen::InterceptedService, service::Interceptor, transport::Channel, Request, Response,
    Status, Streaming,
};
use typedb_protocol::{
    core_database, core_database_manager, session, transaction,
    type_db_client::TypeDbClient as ProtoTypeDBClient,
    type_db_cluster_client::TypeDbClusterClient as ProtoTypeDBClusterClient,
};

use crate::{
    connection::cluster::Credential,
    common::{Executor, Result},
    rpc::builder::{cluster, core, transaction::client_msg},
};

#[derive(Clone, Debug)]
pub(crate) struct RpcClient {
    typedb: ProtoTypeDBClient<Channel>,
    pub(crate) executor: Arc<Executor>,
}

impl RpcClient {
    pub(crate) async fn new(channel: Channel) -> Result<Self> {
        Self::construct(ProtoTypeDBClient::new(channel)).await
    }

    pub(crate) async fn new_with_cred(channel: Channel, credential: Credential) -> Result<Self> {
        Self::construct(ProtoTypeDBClient::new(channel)).await
    }

    pub(crate) async fn connect(address: &str) -> Result<Self> {
        Self::construct(ProtoTypeDBClient::connect(address.to_string()).await?).await
    }

    async fn construct(mut client: ProtoTypeDBClient<Channel>) -> Result<Self> {
        // TODO: temporary hack to validate connection until we have client pulse
        RpcClient::check_connection(&mut client).await?;
        Ok(RpcClient {
            typedb: client,
            executor: Arc::new(Executor::new().expect("Failed to create Executor")),
        })
    }

    async fn check_connection(client: &mut ProtoTypeDBClient<Channel>) -> Result<()> {
        client.databases_all(core::database_manager::all_req()).await?;
        Ok(())
    }

    pub(crate) async fn databases_contains(
        &mut self,
        req: core_database_manager::contains::Req,
    ) -> Result<core_database_manager::contains::Res> {
        single(self.typedb.databases_contains(req)).await
    }

    pub(crate) async fn databases_create(
        &mut self,
        req: core_database_manager::create::Req,
    ) -> Result<core_database_manager::create::Res> {
        single(self.typedb.databases_create(req)).await
    }

    pub(crate) async fn databases_all(
        &mut self,
        req: core_database_manager::all::Req,
    ) -> Result<core_database_manager::all::Res> {
        single(self.typedb.databases_all(req)).await
    }

    pub(crate) async fn database_delete(
        &mut self,
        req: core_database::delete::Req,
    ) -> Result<core_database::delete::Res> {
        single(self.typedb.database_delete(req)).await
    }

    pub(crate) async fn database_rule_schema(
        &mut self,
        req: core_database::rule_schema::Req,
    ) -> Result<core_database::rule_schema::Res> {
        single(self.typedb.database_rule_schema(req)).await
    }

    pub(crate) async fn database_schema(
        &mut self,
        req: core_database::schema::Req,
    ) -> Result<core_database::schema::Res> {
        single(self.typedb.database_schema(req)).await
    }

    pub(crate) async fn database_type_schema(
        &mut self,
        req: core_database::type_schema::Req,
    ) -> Result<core_database::type_schema::Res> {
        single(self.typedb.database_type_schema(req)).await
    }

    pub(crate) async fn session_open(
        &mut self,
        req: session::open::Req,
    ) -> Result<session::open::Res> {
        single(self.typedb.session_open(req)).await
    }

    pub(crate) async fn session_close(
        &mut self,
        req: session::close::Req,
    ) -> Result<session::close::Res> {
        single(self.typedb.session_close(req)).await
    }

    pub(crate) async fn transaction(
        &mut self,
        open_req: transaction::Req,
    ) -> Result<(mpsc::Sender<transaction::Client>, Streaming<transaction::Server>)> {
        // TODO: refactor to crossbeam channel
        let (mut sender, receiver) = mpsc::channel::<transaction::Client>(256);
        sender.send(client_msg(vec![open_req])).await.unwrap();
        bidi_stream(sender, self.typedb.transaction(receiver)).await
    }
}

#[derive(Clone, Debug)]
struct CredentialInjector {
    credential: Arc<Credential>,
    token: Option<String>,
}

impl CredentialInjector {
    fn new(credential: Arc<Credential>) -> Self {
        Self { credential, token: None }
    }
}

impl Interceptor for CredentialInjector {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        request.metadata_mut().insert("username", self.credential.username().try_into().unwrap());
        match &self.token {
            Some(token) => request.metadata_mut().insert("token", token.try_into().unwrap()),
            None => request
                .metadata_mut()
                .insert("password", self.credential.password().try_into().unwrap()),
        };
        Ok(request)
    }
}

type CallCredChannel = InterceptedService<Channel, CredentialInjector>;

#[derive(Clone, Debug)]
pub(crate) struct RpcClusterClient {
    typedb: ProtoTypeDBClusterClient<CallCredChannel>,
    pub(crate) executor: Arc<Executor>,
    credential: Arc<Credential>,
}

impl RpcClusterClient {
    pub(crate) async fn new(channel: Channel, credential: Credential) -> Result<Self> {
        let shared_cred = Arc::new(credential);
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
        mut client: ProtoTypeDBClusterClient<CallCredChannel>,
        credential: Arc<Credential>,
    ) -> Result<Self> {
        // TODO: temporary hack to validate connection until we have client pulse
        RpcClusterClient::check_connection(&mut client).await?;
        Ok(RpcClusterClient {
            typedb: client,
            executor: Arc::new(Executor::new().expect("Failed to create Executor")),
            credential,
        })
    }

    async fn check_connection(
        client: &mut ProtoTypeDBClusterClient<CallCredChannel>,
    ) -> Result<()> {
        client.databases_all(cluster::database_manager::all_req()).await?;
        Ok(())
    }

    pub(crate) async fn user_token(&mut self, username: &str) -> Result<String> {
        todo!()
    }

    pub(crate) async fn servers_all(
        &mut self,
    ) -> Result<typedb_protocol::server_manager::all::Res> {
        single(self.typedb.servers_all(cluster::server_manager::all_req())).await
    }
}

async fn single<T>(
    res: impl Future<Output = ::core::result::Result<Response<T>, Status>>,
) -> Result<T> {
    // TODO: check if we need ensureConnected() from client-java
    res.await.map(|res| res.into_inner()).map_err(|status| status.into())
}

async fn bidi_stream<T, U>(
    req_sink: mpsc::Sender<T>,
    res: impl Future<Output = ::core::result::Result<Response<Streaming<U>>, Status>>,
) -> Result<(mpsc::Sender<T>, Streaming<U>)> {
    res.await.map(|resp| (req_sink, resp.into_inner())).map_err(|status| status.into())
}
