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

use std::{fmt, future::Future, sync::RwLock, time::Duration};

use log::debug;
use tokio::time::sleep;

use crate::{
    common::{
        error::ClientError, Address, Connection, DatabaseProto, Error, ReplicaProto, Result,
        ServerConnection, SessionID,
    },
    connection::server,
};

pub struct Database {
    name: String,
    replicas: RwLock<Vec<Replica>>,
    connection: Connection,
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

    pub(in crate::connection) fn new(proto: DatabaseProto, connection: Connection) -> Result<Self> {
        let name = proto.name.clone();
        let replicas = RwLock::new(Replica::from_proto(proto, &connection));
        Ok(Self { name, replicas, connection })
    }

    pub(in crate::connection) async fn get(name: String, connection: Connection) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            replicas: RwLock::new(Replica::fetch_all(name, connection.clone()).await?),
            connection,
        })
    }

    pub(crate) fn close_session(&self, session_id: SessionID) -> Result {
        // FIXME
        for sc in self.connection.iter_server_connections_cloned() {
            sc.close_session(session_id.clone()).ok();
        }
        Ok(())
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub async fn delete(self) -> Result {
        self.run_on_primary_replica(|database, _, _| database.delete()).await
    }

    pub async fn schema(&self) -> Result<String> {
        self.run_failsafe(|database, _, _| async move { database.schema().await }).await
    }

    pub async fn type_schema(&self) -> Result<String> {
        self.run_failsafe(|database, _, _| async move { database.type_schema().await }).await
    }

    pub async fn rule_schema(&self) -> Result<String> {
        self.run_failsafe(|database, _, _| async move { database.rule_schema().await }).await
    }

    pub(crate) async fn run_failsafe<F, P, R>(&self, task: F) -> Result<R>
    where
        F: Fn(server::Database, ServerConnection, bool) -> P,
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

    async fn run_on_any_replica<F, P, R>(&self, task: F) -> Result<R>
    where
        F: Fn(server::Database, ServerConnection, bool) -> P,
        P: Future<Output = Result<R>>,
    {
        let mut is_first_run = true;
        let replicas = self.replicas.read().unwrap().clone();
        for replica in replicas.iter() {
            match task(
                replica.database.clone(),
                self.connection.get_server_connection(&replica.address),
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
        Err(self.connection.unable_to_connect())
    }

    async fn run_on_primary_replica<F, P, R>(&self, task: F) -> Result<R>
    where
        F: Fn(server::Database, ServerConnection, bool) -> P,
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
                self.connection.get_server_connection(&primary_replica.address),
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
        Err(self.connection.unable_to_connect())
    }

    async fn seek_primary_replica(&self) -> Result<Replica> {
        for _ in 0..Self::FETCH_REPLICAS_MAX_RETRIES {
            let replicas = Replica::fetch_all(self.name.clone(), self.connection.clone()).await?;
            *self.replicas.write().unwrap() = replicas;
            if let Some(replica) = self.primary_replica() {
                return Ok(replica);
            }
            Self::wait_for_primary_replica_selection().await;
        }
        Err(self.connection.unable_to_connect())
    }

    fn primary_replica(&self) -> Option<Replica> {
        self.replicas
            .read()
            .unwrap()
            .iter()
            .filter(|r| r.is_primary)
            .max_by_key(|r| r.term)
            .cloned()
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
    fn new(name: String, metadata: ReplicaProto, server_connection: ServerConnection) -> Self {
        Self {
            address: metadata.address,
            database_name: name.clone(),
            is_primary: metadata.is_primary,
            term: metadata.term,
            is_preferred: metadata.is_preferred,
            database: server::Database::new(name, server_connection),
        }
    }

    fn from_proto(proto: DatabaseProto, connection: &Connection) -> Vec<Self> {
        proto
            .replicas
            .into_iter()
            .map(|replica| {
                let server_connection = connection.get_server_connection(&replica.address);
                Replica::new(proto.name.clone(), replica, server_connection)
            })
            .collect()
    }

    async fn fetch_all(name: String, connection: Connection) -> Result<Vec<Self>> {
        for server_connection in connection.iter_server_connections_cloned() {
            let res = server_connection.get_database_replicas(name.clone()).await;
            match res {
                Ok(res) => {
                    return Ok(Replica::from_proto(res, &connection));
                }
                Err(Error::Client(ClientError::UnableToConnect())) => {
                    println!(
                        "Failed to fetch replica info for database '{}' from {}. Attempting next server.",
                        name,
                        server_connection.address()
                    );
                }
                Err(err) => return Err(err),
            }
        }
        Err(connection.unable_to_connect())
    }
}
