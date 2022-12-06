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

use std::{future::Future, result::Result as StdResult, sync::Arc};

use futures::{channel::mpsc, SinkExt};
use tonic::{transport::Channel, Response, Status, Streaming};
use typedb_protocol::{
    core_database, core_database_manager, session, transaction,
    type_db_client::TypeDbClient as ProtoTypeDBClient,
};

use crate::common::{
    rpc::builder::{core, transaction::client_msg},
    Executor, Result,
};

#[derive(Clone, Debug)]
pub(crate) struct Client<T> {
    client: ProtoTypeDBClient<T>,
    pub(crate) executor: Arc<Executor>,
}

impl Client<Channel> {
    pub(crate) async fn connect(address: &str) -> Result<Self> {
        Self::construct(ProtoTypeDBClient::connect(address.to_string()).await?).await
    }
}

impl<T> Client<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    pub(crate) async fn new(channel: T) -> Result<Self> {
        Self::construct(ProtoTypeDBClient::new(channel)).await
    }

    async fn construct(mut client: ProtoTypeDBClient<T>) -> Result<Self> {
        // TODO: temporary hack to validate connection until we have client pulse
        Self::check_connection(&mut client).await?;
        Ok(Self { client, executor: Arc::new(Executor::new().expect("Failed to create Executor")) })
    }

    async fn check_connection(client: &mut ProtoTypeDBClient<T>) -> Result<()> {
        client.databases_all(core::database_manager::all_req()).await?;
        Ok(())
    }

    pub(crate) async fn databases_contains(
        &mut self,
        req: core_database_manager::contains::Req,
    ) -> Result<core_database_manager::contains::Res> {
        single(self.client.databases_contains(req)).await
    }

    pub(crate) async fn databases_create(
        &mut self,
        req: core_database_manager::create::Req,
    ) -> Result<core_database_manager::create::Res> {
        single(self.client.databases_create(req)).await
    }

    pub(crate) async fn databases_all(
        &mut self,
        req: core_database_manager::all::Req,
    ) -> Result<core_database_manager::all::Res> {
        single(self.client.databases_all(req)).await
    }

    pub(crate) async fn database_delete(
        &mut self,
        req: core_database::delete::Req,
    ) -> Result<core_database::delete::Res> {
        single(self.client.database_delete(req)).await
    }

    pub(crate) async fn database_rule_schema(
        &mut self,
        req: core_database::rule_schema::Req,
    ) -> Result<core_database::rule_schema::Res> {
        single(self.client.database_rule_schema(req)).await
    }

    pub(crate) async fn database_schema(
        &mut self,
        req: core_database::schema::Req,
    ) -> Result<core_database::schema::Res> {
        single(self.client.database_schema(req)).await
    }

    pub(crate) async fn database_type_schema(
        &mut self,
        req: core_database::type_schema::Req,
    ) -> Result<core_database::type_schema::Res> {
        single(self.client.database_type_schema(req)).await
    }

    pub(crate) async fn session_open(
        &mut self,
        req: session::open::Req,
    ) -> Result<session::open::Res> {
        single(self.client.session_open(req)).await
    }

    pub(crate) async fn session_close(
        &mut self,
        req: session::close::Req,
    ) -> Result<session::close::Res> {
        single(self.client.session_close(req)).await
    }

    pub(crate) async fn transaction(
        &mut self,
        open_req: transaction::Req,
    ) -> Result<(mpsc::Sender<transaction::Client>, Streaming<transaction::Server>)> {
        // TODO: refactor to crossbeam channel
        let (mut sender, receiver) = mpsc::channel::<transaction::Client>(256);
        sender.send(client_msg(vec![open_req])).await.unwrap();
        bidi_stream(sender, self.client.transaction(receiver)).await
    }
}

pub(crate) async fn single<T>(
    res: impl Future<Output = StdResult<Response<T>, Status>>,
) -> Result<T> {
    // TODO: check if we need ensureConnected() from client-java
    Ok(res.await?.into_inner())
}

pub(crate) async fn bidi_stream<T, U>(
    req_sink: mpsc::Sender<T>,
    res: impl Future<Output = StdResult<Response<Streaming<U>>, Status>>,
) -> Result<(mpsc::Sender<T>, Streaming<U>)> {
    Ok((req_sink, res.await?.into_inner()))
}
