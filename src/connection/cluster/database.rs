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

use log::debug;
use tokio::time::sleep;

use crate::{
    common::{
        error::ClientError,
        rpc::builder::{
            cluster::database_manager::{all_req, get_req},
            core::database_manager::{contains_req, create_req},
        },
        Address, ClusterRPC, ClusterServerRPC, Error, Result,
    },
    connection::server,
};

#[derive(Clone, Debug)]
pub struct DatabaseManager {
    cluster_rpc: Arc<ClusterRPC>,
}

impl DatabaseManager {
    pub(crate) fn new(cluster_rpc: Arc<ClusterRPC>) -> Self {
        Self { cluster_rpc }
    }

    pub async fn get(&mut self, name: &str) -> Result<Database> {
        Database::get(name, self.cluster_rpc.clone()).await
    }

    pub async fn contains(&mut self, name: &str) -> Result<bool> {
        Ok(self
            .run_failsafe(name, move |database, mut server_rpc, _| {
                let req = contains_req(&database.name);
                async move { server_rpc.databases_contains(req).await }
            })
            .await?
            .contains)
    }

    pub async fn create(&mut self, name: &str) -> Result {
        self.run_failsafe(name, |database, mut server_rpc, _| {
            let req = create_req(&database.name);
            async move { server_rpc.databases_create(req).await }
        })
        .await?;
        Ok(())
    }

    pub async fn all(&mut self) -> Result<Vec<Database>> {
        let mut error_buffer = Vec::with_capacity(self.cluster_rpc.len());
        let mut databases = None;
        for mut server_client in self.cluster_rpc.iter_servers_cloned() {
            match server_client.databases_all(all_req()).await {
                Err(err) => error_buffer.push(format!("- {}: {}", server_client.address(), err)),
                Ok(list) => {
                    databases = Some(list.databases);
                    break;
                }
            }
        }
        if databases.is_none() {
            Err(ClientError::ClusterAllNodesFailed(error_buffer.join("\n")))?;
        }

        databases
            .unwrap()
            .into_iter()
            .map(|proto_db| Database::new(proto_db, self.cluster_rpc.clone()))
            .collect()
    }

    async fn run_failsafe<F, P, R>(&mut self, name: &str, task: F) -> Result<R>
    where
        F: Fn(server::Database, ClusterServerRPC, bool) -> P,
        P: Future<Output = Result<R>>,
    {
        Database::get(name, self.cluster_rpc.clone()).await?.run_failsafe(&task).await
    }
}

#[derive(Clone)]
pub struct Database {
    pub name: String,
    replicas: Vec<Replica>,
    cluster_rpc: Arc<ClusterRPC>,
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
    fn new(proto: typedb_protocol::ClusterDatabase, cluster_rpc: Arc<ClusterRPC>) -> Result<Self> {
        let name = proto.name.clone();
        let replicas = Replica::from_proto(proto, &cluster_rpc);
        Ok(Self { name, replicas, cluster_rpc })
    }

    async fn get(name: &str, cluster_rpc: Arc<ClusterRPC>) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            replicas: Replica::fetch_all(name, cluster_rpc.clone()).await?,
            cluster_rpc,
        })
    }

    pub(crate) fn primary_replica(&mut self) -> Option<Replica> {
        self.replicas.iter().filter(|r| r.is_primary).max_by_key(|r| r.term).cloned()
    }

    pub(crate) async fn run_on_any_replica<F, P, R>(&mut self, task: F) -> Result<R>
    where
        F: Fn(server::Database, ClusterServerRPC, bool) -> P,
        P: Future<Output = Result<R>>,
    {
        let mut is_first_run = true;
        for replica in self.replicas.iter() {
            match task(
                replica.database.clone(),
                self.cluster_rpc.get_server(&replica.address),
                is_first_run,
            )
            .await
            {
                Err(Error::Client(ClientError::UnableToConnect())) => {
                    println!("Unable to connect to {}. Attempting next server.", replica.address);
                }
                res => return res,
            }
            is_first_run = false;
        }
        Err(self.cluster_rpc.unable_to_connect())
    }

    pub(crate) async fn run_on_primary_replica<F, P, R>(&mut self, task: F) -> Result<R>
    where
        F: Fn(server::Database, ClusterServerRPC, bool) -> P,
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
                primary_replica.database.clone(),
                self.cluster_rpc.get_server(&primary_replica.address),
                retry == 0,
            )
            .await
            {
                Err(Error::Client(
                    ClientError::ClusterReplicaNotPrimary() | ClientError::UnableToConnect(),
                )) => {
                    debug!("Primary replica error, waiting...");
                    Self::wait_for_primary_replica_selection().await;
                    primary_replica = self.seek_primary_replica().await?;
                }
                res => return res,
            }
        }
        Err(self.cluster_rpc.unable_to_connect())
    }

    pub(crate) async fn run_failsafe<F, P, R>(&mut self, task: F) -> Result<R>
    where
        F: Fn(server::Database, ClusterServerRPC, bool) -> P,
        P: Future<Output = Result<R>>,
    {
        match self.run_on_any_replica(&task).await {
            Err(Error::Client(ClientError::ClusterReplicaNotPrimary())) => {
                debug!("Attempted to run on a non-primary replica, retrying on primary...");
                self.run_on_primary_replica(&task).await
            }
            res => res,
        }
    }

    async fn seek_primary_replica(&mut self) -> Result<Replica> {
        let fetch_max_retries = 10; // FIXME constant
        for _ in 0..fetch_max_retries {
            self.replicas = Replica::fetch_all(&self.name, self.cluster_rpc.clone()).await?;
            if let Some(replica) = self.primary_replica() {
                return Ok(replica);
            }
            Self::wait_for_primary_replica_selection().await;
        }
        Err(self.cluster_rpc.unable_to_connect())
    }

    async fn wait_for_primary_replica_selection() {
        sleep(Duration::from_secs(2)).await; // FIXME constant
    }

    pub async fn delete(mut self) -> Result {
        self.run_on_primary_replica(|database, _, _| database.delete()).await
    }

    pub async fn schema(&mut self) -> Result<String> {
        self.run_failsafe(|mut database, _, _| async move { database.schema().await }).await
    }

    pub async fn rule_schema(&mut self) -> Result<String> {
        self.run_failsafe(|mut database, _, _| async move { database.rule_schema().await }).await
    }

    pub async fn type_schema(&mut self) -> Result<String> {
        self.run_failsafe(|mut database, _, _| async move { database.type_schema().await }).await
    }
}

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
        rpc_client: ClusterServerRPC,
    ) -> Self {
        Self {
            address: metadata.address.parse().expect("Invalid URI received from the server"),
            database_name: name.to_owned(),
            is_primary: metadata.primary,
            term: metadata.term,
            is_preferred: metadata.preferred,
            database: server::Database::new(name, rpc_client.into()),
        }
    }

    fn from_proto(proto: typedb_protocol::ClusterDatabase, cluster_rpc: &ClusterRPC) -> Vec<Self> {
        proto
            .replicas
            .into_iter()
            .map(|replica| {
                let server_rpc = cluster_rpc.get_server(&replica.address.parse().unwrap());
                Replica::new(&proto.name, replica, server_rpc)
            })
            .collect()
    }

    async fn fetch_all(name: &str, cluster_rpc: Arc<ClusterRPC>) -> Result<Vec<Self>> {
        for mut client in cluster_rpc.iter_servers_cloned() {
            let res = client.databases_get(get_req(&name)).await;
            match res {
                Ok(res) => {
                    return Ok(Replica::from_proto(res.database.unwrap(), &cluster_rpc));
                }
                Err(Error::Client(ClientError::UnableToConnect())) => {
                    println!(
                        "Failed to fetch replica info for database '{}' from {}. Attempting next server.",
                        name,
                        client.address()
                    );
                }
                Err(err) => return Err(err),
            }
        }
        Err(cluster_rpc.unable_to_connect())
    }
}
