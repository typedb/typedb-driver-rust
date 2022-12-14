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
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures::channel::mpsc;
use tonic::Streaming;
use typedb_protocol::{
    cluster_database_manager, cluster_user, core_database, core_database_manager, session,
    transaction, type_db_cluster_client::TypeDbClusterClient as ProtoTypeDBClusterClient,
};

use crate::common::{
    credential::CallCredentials,
    error::ClientError,
    rpc,
    rpc::{
        builder::{cluster, cluster::user::token_req},
        channel::CallCredChannel,
        Channel,
    },
    Address, Credential, Error, Executor, Result, TonicResult,
};

#[derive(Debug, Clone)]
pub(crate) struct ClusterClientManager {
    cluster_clients: HashMap<Address, ClusterClient>,
}

impl ClusterClientManager {
    pub(crate) async fn fetch_current_addresses(
        addresses: &[&str],
        credential: &Credential,
    ) -> Result<HashSet<Address>> {
        for address in addresses {
            match ClusterClient::new_validated(address.parse()?, credential.clone()).await {
                Ok(mut client) => {
                    let servers = client.servers_all().await?.servers;
                    return servers.into_iter().map(|server| server.address.parse()).collect();
                }
                Err(Error::Client(ClientError::UnableToConnect())) => (),
                Err(err) => Err(err)?,
            }
        }
        Err(ClientError::UnableToConnect())?
    }

    pub(crate) fn new(addresses: HashSet<Address>, credential: Credential) -> Result<Arc<Self>> {
        let cluster_clients = addresses
            .into_iter()
            .map(|address| {
                Ok((address.clone(), ClusterClient::new_lazy(address, credential.clone())?))
            })
            .collect::<Result<_>>()?;
        Ok(Arc::new(Self { cluster_clients }))
    }

    pub(crate) fn len(&self) -> usize {
        self.cluster_clients.len()
    }

    pub(crate) fn addresses(&self) -> impl Iterator<Item = &Address> {
        self.cluster_clients.keys()
    }

    pub(crate) fn get(&self, address: &Address) -> ClusterClient {
        self.cluster_clients.get(address).unwrap().clone()
    }

    pub(crate) fn get_any(&self) -> ClusterClient {
        // TODO round robin?
        self.cluster_clients.values().next().unwrap().clone()
    }

    pub(crate) fn iter_cloned(&self) -> impl Iterator<Item = ClusterClient> + '_ {
        self.cluster_clients.values().cloned()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ClusterClient {
    address: Address,
    server_client: rpc::ServerClient,
    cluster_client: ProtoTypeDBClusterClient<CallCredChannel>,
    pub(crate) executor: Arc<Executor>,
    credential_handler: Arc<Mutex<CallCredentials>>,
}

impl ClusterClient {
    pub(crate) fn new_lazy(address: Address, credential: Credential) -> Result<Self> {
        let (channel, credential_handler) = Channel::open_encrypted(address.clone(), credential)?;
        Ok(Self {
            address,
            server_client: rpc::ServerClient::new_lazy(channel.clone())?,
            cluster_client: ProtoTypeDBClusterClient::new(channel.into()),
            executor: Arc::new(Executor::new().expect("Failed to create Executor")),
            credential_handler,
        })
    }

    pub(crate) async fn new_validated(address: Address, credential: Credential) -> Result<Self> {
        let mut this = Self::new_lazy(address, credential)?;
        this.validate_connection().await?;
        Ok(this)
    }

    pub(crate) fn into_server_client(self) -> rpc::ServerClient {
        self.server_client
    }

    pub(crate) fn address(&self) -> &Address {
        &self.address
    }

    async fn validate_connection(&mut self) -> Result<()> {
        self.cluster_client.databases_all(cluster::database_manager::all_req()).await?;
        Ok(())
    }

    async fn may_renew_token<F, R>(&mut self, call: F) -> Result<R>
    where
        for<'a> F: Fn(&'a mut Self) -> Pin<Box<dyn Future<Output = TonicResult<R>> + 'a>>,
    {
        match call(self).await.map_err(Error::from) {
            Err(Error::Client(ClientError::ClusterTokenCredentialInvalid())) => {
                self.renew_token().await?;
                Ok(call(self).await?.into_inner())
            }
            res => Ok(res?.into_inner()),
        }
    }

    async fn renew_token(&mut self) -> Result {
        self.credential_handler.lock().unwrap().reset_token();
        let req = token_req(self.credential_handler.lock().unwrap().username());
        let token = self.user_token(req).await?.token;
        self.credential_handler.lock().unwrap().set_token(token);
        Ok(())
    }

    async fn user_token(
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
        req: cluster_database_manager::get::Req,
    ) -> Result<cluster_database_manager::get::Res> {
        self.may_renew_token(|this| Box::pin(this.cluster_client.databases_get(req.clone()))).await
    }

    pub(crate) async fn databases_all(
        &mut self,
        req: cluster_database_manager::all::Req,
    ) -> Result<cluster_database_manager::all::Res> {
        self.may_renew_token(|this| Box::pin(this.cluster_client.databases_all(req.clone()))).await
    }

    // server client pasthrough
    pub(crate) async fn databases_contains(
        &mut self,
        req: core_database_manager::contains::Req,
    ) -> Result<core_database_manager::contains::Res> {
        self.server_client.databases_contains(req).await
    }

    pub(crate) async fn databases_create(
        &mut self,
        req: core_database_manager::create::Req,
    ) -> Result<core_database_manager::create::Res> {
        self.server_client.databases_create(req).await
    }

    pub(crate) async fn database_delete(
        &mut self,
        req: core_database::delete::Req,
    ) -> Result<core_database::delete::Res> {
        self.server_client.database_delete(req).await
    }

    pub(crate) async fn database_rule_schema(
        &mut self,
        req: core_database::rule_schema::Req,
    ) -> Result<core_database::rule_schema::Res> {
        self.server_client.database_rule_schema(req).await
    }

    pub(crate) async fn database_schema(
        &mut self,
        req: core_database::schema::Req,
    ) -> Result<core_database::schema::Res> {
        self.server_client.database_schema(req).await
    }

    pub(crate) async fn database_type_schema(
        &mut self,
        req: core_database::type_schema::Req,
    ) -> Result<core_database::type_schema::Res> {
        self.server_client.database_type_schema(req).await
    }

    pub(crate) async fn session_open(
        &mut self,
        req: session::open::Req,
    ) -> Result<session::open::Res> {
        self.server_client.session_open(req).await
    }

    pub(crate) async fn session_close(
        &mut self,
        req: session::close::Req,
    ) -> Result<session::close::Res> {
        self.server_client.session_close(req).await
    }

    pub(crate) async fn transaction(
        &mut self,
        open_req: transaction::Req,
    ) -> Result<(mpsc::Sender<transaction::Client>, Streaming<transaction::Server>)> {
        self.server_client.transaction(open_req).await
    }
}
