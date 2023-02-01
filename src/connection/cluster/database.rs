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

use std::{fmt, future::Future, ops::Deref, time::Duration};

use log::debug;
use tokio::time::sleep;

use crate::{
    common::{
        error::ClientError, Address, ClusterConnection, ClusterServerConnection, DatabaseProto,
        Error, ReplicaProto, Result,
    },
    connection::server,
};

#[derive(Clone)]
pub struct Database {
    name: String,
    replicas: Vec<Replica>,
    cluster_connection: ClusterConnection,
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("name", &self.name)
            .field("replicas", &self.replicas)
            .finish()
    }
}

impl Database {
    const PRIMARY_REPLICA_TASK_MAX_RETRIES: usize = 10;
    const FETCH_REPLICAS_MAX_RETRIES: usize = 10;
    const WAIT_FOR_PRIMARY_REPLICA_SELECTION: Duration = Duration::from_secs(2);

    pub(super) fn new(proto: DatabaseProto, cluster_connection: ClusterConnection) -> Result<Self> {
        let name = proto.name.clone();
        let replicas = Replica::from_proto(proto, &cluster_connection);
        Ok(Self { name, replicas, cluster_connection })
    }

    pub(super) async fn get(name: String, cluster_connection: ClusterConnection) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            replicas: Replica::fetch_all(name, cluster_connection.clone()).await?,
            cluster_connection,
        })
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub async fn delete(mut self) -> Result {
        self.run_on_primary_replica(|database, _, _| database.delete()).await
    }

    pub async fn schema(&mut self) -> Result<String> {
        self.run_failsafe(|database, _, _| async move { database.schema().await }).await
    }

    pub async fn type_schema(&mut self) -> Result<String> {
        self.run_failsafe(|database, _, _| async move { database.type_schema().await }).await
    }

    pub async fn rule_schema(&mut self) -> Result<String> {
        self.run_failsafe(|database, _, _| async move { database.rule_schema().await }).await
    }

    pub(crate) async fn run_failsafe<F, P, R>(&mut self, task: F) -> Result<R>
    where
        F: Fn(server::Database, ClusterServerConnection, bool) -> P,
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

    async fn run_on_any_replica<F, P, R>(&mut self, task: F) -> Result<R>
    where
        F: Fn(server::Database, ClusterServerConnection, bool) -> P,
        P: Future<Output = Result<R>>,
    {
        let mut is_first_run = true;
        for replica in self.replicas.iter() {
            match task(
                replica.database.clone(),
                self.cluster_connection.get_server_connection(&replica.address),
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
        Err(self.cluster_connection.unable_to_connect())
    }

    async fn run_on_primary_replica<F, P, R>(&mut self, task: F) -> Result<R>
    where
        F: Fn(server::Database, ClusterServerConnection, bool) -> P,
        P: Future<Output = Result<R>>,
    {
        let mut primary_replica = if let Some(replica) = self.primary_replica() {
            replica
        } else {
            self.seek_primary_replica().await?
        };

        for retry in 0..Self::PRIMARY_REPLICA_TASK_MAX_RETRIES {
            match task(
                primary_replica.database.clone(),
                self.cluster_connection.get_server_connection(&primary_replica.address),
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
        Err(self.cluster_connection.unable_to_connect())
    }

    async fn seek_primary_replica(&mut self) -> Result<Replica> {
        for _ in 0..Self::FETCH_REPLICAS_MAX_RETRIES {
            self.replicas =
                Replica::fetch_all(self.name.clone(), self.cluster_connection.clone()).await?;
            if let Some(replica) = self.primary_replica() {
                return Ok(replica);
            }
            Self::wait_for_primary_replica_selection().await;
        }
        Err(self.cluster_connection.unable_to_connect())
    }

    fn primary_replica(&mut self) -> Option<Replica> {
        self.replicas.iter().filter(|r| r.is_primary).max_by_key(|r| r.term).cloned()
    }

    async fn wait_for_primary_replica_selection() {
        sleep(Self::WAIT_FOR_PRIMARY_REPLICA_SELECTION).await;
    }
}

#[derive(Clone)]
pub struct Replica {
    address: Address,
    database_name: String,
    is_primary: bool,
    term: i64,
    is_preferred: bool,
    database: server::Database,
}

impl fmt::Debug for Replica {
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
        name: String,
        metadata: ReplicaProto,
        server_connection: ClusterServerConnection,
    ) -> Self {
        Self {
            address: metadata.address,
            database_name: name.clone(),
            is_primary: metadata.is_primary,
            term: metadata.term,
            is_preferred: metadata.is_preferred,
            database: server::Database::new(name, server_connection.deref().clone()), // FIXME haxx
        }
    }

    fn from_proto(proto: DatabaseProto, cluster_connection: &ClusterConnection) -> Vec<Self> {
        proto
            .replicas
            .into_iter()
            .map(|replica| {
                let server_connection = cluster_connection.get_server_connection(&replica.address);
                Replica::new(proto.name.clone(), replica, server_connection)
            })
            .collect()
    }

    async fn fetch_all(name: String, cluster_connection: ClusterConnection) -> Result<Vec<Self>> {
        for connection in cluster_connection.iter_server_connections_cloned() {
            let res = connection.get_database_replicas(name.clone()).await;
            match res {
                Ok(res) => {
                    return Ok(Replica::from_proto(res, &cluster_connection));
                }
                Err(Error::Client(ClientError::UnableToConnect())) => {
                    println!(
                        "Failed to fetch replica info for database '{}' from {}. Attempting next server.",
                        name,
                        connection.address()
                    );
                }
                Err(err) => return Err(err),
            }
        }
        Err(cluster_connection.unable_to_connect())
    }
}
