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

use std::{fmt, fmt::Debug, future::Future, sync::Arc, time::Duration};

use futures::future::try_join_all;
use log::debug;
use tokio::time::sleep;

use crate::{
    common::{
        error::ClientError,
        rpc,
        rpc::builder::{
            cluster::database_manager::{all_req, get_req},
            core::database_manager::{contains_req, create_req},
        },
        Address, Error, Result,
    },
    connection::server,
};

#[derive(Clone)]
pub struct Replica {
    pub(crate) address: Address,
    database_name: String,
    is_primary: bool,
    term: i64,
    is_preferred: bool,
    database: server::Database,
}

impl Debug for Replica {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Replica")
            .field("address", &self.address)
            .field("database_name", &self.database_name)
            .field("is_primary", &self.is_primary)
            .field("term", &self.term)
            .field("is_preferred", &self.is_preferred)
            .finish()
    }
}

impl Replica {
    fn new(
        name: &str,
        metadata: typedb_protocol::cluster_database::Replica,
        rpc_client: rpc::Client,
    ) -> Replica {
        Self {
            address: metadata.address.parse().expect("Invalid URI received from the server"),
            database_name: name.to_owned(),
            is_primary: metadata.primary,
            term: metadata.term,
            is_preferred: metadata.preferred,
            database: server::Database::new(name, rpc_client),
        }
    }

    fn from_proto(
        proto: typedb_protocol::ClusterDatabase,
        rpc_cluster_manager: &rpc::ClusterClientManager,
    ) -> Vec<Replica> {
        proto
            .replicas
            .into_iter()
            .map(|replica| {
                let rpc_client =
                    rpc_cluster_manager.get(&replica.address.parse().unwrap()).into_core();
                Replica::new(&proto.name, replica, rpc_client)
            })
            .collect()
    }
}

#[derive(Clone)]
pub struct Database {
    pub name: String,
    replicas: Vec<Replica>,
    rpc_cluster_manager: Arc<rpc::ClusterClientManager>,
}

impl Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("cluster::Database")
            .field("name", &self.name)
            .field("replicas", &self.replicas)
            .finish()
    }
}

impl Database {
    async fn new(
        proto: typedb_protocol::ClusterDatabase,
        rpc_cluster_manager: Arc<rpc::ClusterClientManager>,
    ) -> Result<Self> {
        let name = proto.name.clone();
        let replicas = Replica::from_proto(proto, &rpc_cluster_manager);
        Ok(Self { name, replicas, rpc_cluster_manager })
    }

    async fn get(name: &str, rpc_cluster_manager: Arc<rpc::ClusterClientManager>) -> Result<Self> {
        let mut this =
            Self { name: name.to_string(), replicas: Vec::default(), rpc_cluster_manager };
        this.fetch_replicas().await?;
        Ok(this)
    }

    async fn fetch_replicas(&mut self) -> Result {
        for mut client in self.rpc_cluster_manager.iter_cloned() {
            let res = client.databases_get(get_req(&self.name)).await;
            match res {
                Ok(res) => {
                    self.replicas =
                        Replica::from_proto(res.database.unwrap(), &self.rpc_cluster_manager);
                    return Ok(());
                }
                Err(Error::Client(ClientError::UnableToConnect())) => {
                    debug!(
                        "Failed to fetch replica info for database '{}' from {}. Attempting next server.",
                        self.name,
                        client.address()
                    );
                }
                Err(err) => return Err(err),
            }
        }
        Err(self.cluster_unable_to_connect())
    }

    fn cluster_unable_to_connect(&self) -> Error {
        Error::Client(ClientError::ClusterUnableToConnect(
            self.rpc_cluster_manager
                .addresses()
                .map(Address::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ))
    }

    pub(crate) fn primary_replica(&mut self) -> Option<Replica> {
        self.replicas.iter().filter(|r| r.is_primary).max_by_key(|r| r.term).cloned()
    }

    pub(crate) async fn run_on_any_replica<F, P, R>(&mut self, task: F) -> Result<R>
    where
        F: Fn(String, rpc::ClusterClient, bool) -> P,
        P: Future<Output = Result<R>>,
    {
        let mut is_first_run = true;
        for replica in self.replicas.iter() {
            match task(
                self.name.clone(),
                self.rpc_cluster_manager.get(&replica.address),
                is_first_run,
            )
            .await
            {
                Err(Error::Client(ClientError::UnableToConnect())) => {
                    debug!("Unable to connect to {}. Attempting next server.", replica.address);
                }
                res => return res,
            }
            is_first_run = false;
        }
        Err(self.cluster_unable_to_connect())
    }

    pub(crate) async fn run_on_primary_replica<F, P, R>(&mut self, task: F) -> Result<R>
    where
        F: Fn(String, rpc::ClusterClient, bool) -> P,
        P: Future<Output = Result<R>>,
    {
        let mut primary_replica = if let Some(replica) = self.primary_replica() {
            replica
        } else {
            self.seek_primary_replica().await?
        };

        let max_retries = 10; // FIXME constant
        for retry in 0..max_retries {
            match task(
                self.name.clone(),
                self.rpc_cluster_manager.get(&primary_replica.address),
                retry == 0,
            )
            .await
            {
                Err(Error::Client(
                    ClientError::ClusterReplicaNotPrimary() | ClientError::UnableToConnect(),
                )) => {
                    println!("Primary replica error, waiting...");
                    Self::wait_for_primary_replica_selection().await;
                    primary_replica = self.seek_primary_replica().await?;
                }
                res => return res,
            }
        }
        Err(self.cluster_unable_to_connect())
    }

    async fn seek_primary_replica(&mut self) -> Result<Replica> {
        let fetch_max_retries = 10; // FIXME constant
        for _retry in 0..fetch_max_retries {
            println!("Seeking, attempt #{}...", _retry + 1);
            self.fetch_replicas().await?;
            if let Some(replica) = self.primary_replica() {
                println!("Got primary replica!");
                return Ok(replica);
            }
            Self::wait_for_primary_replica_selection().await;
        }
        Err(self.cluster_unable_to_connect())
    }

    async fn wait_for_primary_replica_selection() {
        sleep(Duration::from_secs(2)).await; // FIXME constant
    }

    pub async fn delete(mut self) -> Result {
        self.primary_replica().unwrap().database.delete().await
    }

    pub async fn rule_schema(&mut self) -> Result<String> {
        self.primary_replica().unwrap().database.rule_schema().await
    }

    pub async fn schema(&mut self) -> Result<String> {
        self.primary_replica().unwrap().database.schema().await
    }

    pub async fn type_schema(&mut self) -> Result<String> {
        self.primary_replica().unwrap().database.type_schema().await
    }
}

#[derive(Clone, Debug)]
pub struct DatabaseManager {
    rpc_cluster_manager: Arc<rpc::ClusterClientManager>,
}

impl DatabaseManager {
    pub(crate) async fn new(rpc_cluster_manager: Arc<rpc::ClusterClientManager>) -> Result<Self> {
        Ok(Self { rpc_cluster_manager })
    }

    pub async fn get(&mut self, name: &str) -> Result<Database> {
        Database::get(name, self.rpc_cluster_manager.clone()).await
    }

    pub async fn contains(&mut self, name: &str) -> Result<bool> {
        Ok(self
            .run_failsafe(name, move |name, mut server_client, _| {
                let req = contains_req(&name);
                async move { server_client.databases_contains(req).await }
            })
            .await?
            .contains)
    }

    pub async fn create(&mut self, name: &str) -> Result {
        self.run_failsafe(name, |name, mut server_client, _| {
            let req = create_req(&name);
            async move { server_client.databases_create(req).await }
        })
        .await?;
        Ok(())
    }

    pub async fn all(&mut self) -> Result<Vec<Database>> {
        let mut error_buffer = Vec::with_capacity(self.rpc_cluster_manager.len());
        let mut database_list = None;
        for mut server_client in self.rpc_cluster_manager.iter_cloned() {
            match server_client.databases_all(all_req()).await {
                Err(err) => error_buffer.push(format!("- {}: {}", server_client.address(), err)),
                Ok(list) => {
                    database_list = Some(list);
                    break;
                }
            }
        }
        if database_list.is_none() {
            Err(ClientError::ClusterAllNodesFailed(error_buffer.join("\n")))?;
        }

        try_join_all(
            database_list
                .unwrap()
                .databases
                .into_iter()
                .map(|proto_db| Database::new(proto_db, self.rpc_cluster_manager.clone())),
        )
        .await
    }

    async fn run_failsafe<F, P, R>(&mut self, name: &str, task: F) -> Result<R>
    where
        F: Fn(String, rpc::ClusterClient, bool) -> P,
        P: Future<Output = Result<R>>,
    {
        let mut database = Database::get(name, self.rpc_cluster_manager.clone()).await?;
        match database.run_on_any_replica(&task).await {
            Err(Error::Client(ClientError::ClusterReplicaNotPrimary())) => {
                database.run_on_any_replica(&task).await
            }
            res => res,
        }
    }
}
