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

mod database;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

use futures::{future::join_all, StreamExt};
use tonic::{
    transport::{Channel},
    Code,
};

use self::database::{Database, DatabaseManager, Replica};
use crate::{
    common::{
        error::MESSAGES,
        rpc,
        rpc::{builder::cluster::database_manager::get_req, cluster_client::TonicResult},
    },
    Result,
};
use crate::common::Credential;

pub struct Client {
    server_clients: HashMap<String, ServerClient>,
    databases: DatabaseManager,
    cluster_databases: HashMap<String, Database>,
}

impl Client {
    pub async fn new(init_addresses: &HashSet<String>, credential: Credential) -> Result<Self> {
        Ok(Self {
            server_clients: create_clients(init_addresses, credential).await?,
            databases: DatabaseManager::new().await?,
            cluster_databases: HashMap::new(),
        })
    }

    async fn run_failsafe_any_replica<C, F, R>(
        &mut self,
        caller: &mut C,
        database_name: &str,
        fun: F,
    ) -> Result<R>
    where
        F: for<'a> Fn(
            &'a mut C,
            &mut ServerClient,
            &Replica,
        ) -> Pin<Box<dyn Future<Output = TonicResult<R>> + 'a>>,
    {
        if !self.cluster_databases.contains_key(database_name) {
            self.fetch_database_replicas(database_name).await?;
        }
        let database = self.cluster_databases.get(database_name).unwrap();

        let mut replicas = Vec::with_capacity(database.replicas.len());
        replicas.push(database.preferred_replica.clone());
        replicas.extend(database.replicas.iter().filter(|r| !r.is_preferred).cloned());

        for (n, replica) in replicas.iter().enumerate() {
            let f = if n == 0 { &fun } else { &fun };
            match f(caller, self.server_clients.get_mut(&replica.address).unwrap(), &replica).await
            {
                Err(err) if err.code() == Code::Unavailable => (),
                res => return Ok(res?.into_inner()),
            }
        }
        Err(MESSAGES.client.unable_to_connect.to_err(vec![]))
    }

    async fn fetch_database_replicas(&mut self, database_name: &str) -> Result {
        for (address, server_client) in self.server_clients.iter_mut() {
            match server_client.cluster_client.databases_get(get_req(database_name)).await {
                _ => todo!(),
            }
        }
        Ok(())
    }
}

async fn fetch_current_addresses(
    addresses: &HashSet<String>,
    credential: &Credential,
) -> Result<HashSet<String>> {
    for address in addresses {
        match ServerClient::new(address, credential.clone()).await {
            Ok(mut client) => return client.servers().await,
            Err(err) if err == MESSAGES.client.unable_to_connect.to_err(vec![]) => (),
            Err(err) => return Err(err),
        }
    }
    Err(MESSAGES.client.unable_to_connect.to_err(vec![]))
}

async fn create_clients(
    init_addresses: &HashSet<String>,
    credential: Credential,
) -> Result<HashMap<String, ServerClient>> {
    let addresses = fetch_current_addresses(init_addresses, &credential).await?;
    let mut clients =
        join_all(addresses.iter().map(|addr| ServerClient::new(&addr, credential.clone())))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

    if join_all(clients.iter_mut().map(ServerClient::validate)).await.iter().all(Result::is_err) {
        Err(MESSAGES.client.unable_to_connect.to_err(vec![]))?;
    }

    Ok(addresses.into_iter().zip(clients.into_iter()).collect())
}

pub struct ServerClient {
    address: String,
    cluster_client: rpc::ClusterClient,
}

impl ServerClient {
    async fn new(address: &str, credential: Credential) -> Result<Self> {
        let uri = if address.contains("://") {
            address.parse().unwrap()
        } else {
            format!("http://{}", address).parse().unwrap()
        };

        let channel = if credential.is_tls_enabled() {
            Channel::builder(uri).tls_config(credential.tls_config()?)?.connect().await?
        } else {
            Channel::builder(uri).connect().await.expect("")
        };
        Ok(Self {
            address: address.to_owned(),
            cluster_client: rpc::ClusterClient::new(channel, credential).await?,
        })
    }

    async fn servers(&mut self) -> Result<HashSet<String>> {
        self.cluster_client
            .servers_all()
            .await
            .map(|res| res.servers.into_iter().map(|server| server.address).collect())
    }

    async fn validate(&mut self) -> Result<()> {
        // self.client.validate().await
        Ok(())
    }
}
