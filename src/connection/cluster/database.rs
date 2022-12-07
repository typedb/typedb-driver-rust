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

use futures::future::try_join_all;
use tonic::{Response, Status};

use crate::{
    common::{
        error::MESSAGES,
        rpc,
        rpc::builder::{
            cluster::database_manager::{all_req, get_req},
            core::database_manager::{contains_req, create_req},
        },
        Result,
    },
    connection::server,
};

type TonicResult<R> = StdResult<Response<R>, Status>;

#[derive(Clone, Debug)]
pub struct Replica {
    pub(crate) address: String,
    pub(crate) database_name: String,
    pub(crate) is_primary: bool,
    pub(crate) is_preferred: bool,
    database: server::Database,
}

impl Replica {
    fn new(
        name: &str,
        metadata: typedb_protocol::cluster_database::Replica,
        rpc_client: rpc::Client,
    ) -> Replica {
        Self {
            address: metadata.address,
            database_name: name.to_owned(),
            is_primary: metadata.primary,
            is_preferred: metadata.preferred,
            database: server::Database::new(name, rpc_client),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Database {
    pub name: String,
    replicas: Vec<Replica>,
}

impl Database {
    async fn new(
        proto: typedb_protocol::ClusterDatabase,
        rpc_cluster_manager: Arc<rpc::ClusterClientManager>,
    ) -> Result<Self> {
        let replicas = proto
            .replicas
            .into_iter()
            .map(|replica| {
                let rpc_client = rpc_cluster_manager.get(&replica.address).into_core();
                Replica::new(&proto.name, replica, rpc_client)
            })
            .collect();
        Ok(Self { name: proto.name, replicas })
    }

    fn primary_replica(&self) -> Replica {
        self.replicas.iter().find(|replica| replica.is_primary).unwrap().clone()
    }

    pub async fn delete(self) -> Result {
        self.primary_replica().database.delete().await
    }

    pub async fn rule_schema(&mut self) -> Result<String> {
        self.primary_replica().database.rule_schema().await
    }

    pub async fn schema(&mut self) -> Result<String> {
        self.primary_replica().database.schema().await
    }

    pub async fn type_schema(&mut self) -> Result<String> {
        self.primary_replica().database.type_schema().await
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
        let maybe_proto_db = self
            .run_failsafe(move |mut server_client| {
                let req = get_req(name);
                async move { server_client.databases_get(req).await }
            })
            .await?
            .database;
        if let Some(proto_db) = maybe_proto_db {
            Database::new(proto_db, self.rpc_cluster_manager.clone()).await
        } else {
            Err(MESSAGES.client.db_does_not_exist.to_err(vec![name]))
        }
    }

    pub async fn contains(&mut self, name: &str) -> Result<bool> {
        Ok(self
            .run_failsafe(move |mut server_client| {
                let req = contains_req(name);
                async move { server_client.databases_contains(req).await }
            })
            .await?
            .contains)
    }

    pub async fn create(&mut self, name: &str) -> Result {
        self.run_failsafe(|mut server_client| {
            let req = create_req(name);
            async move { server_client.databases_create(req).await }
        })
        .await?;
        Ok(())
    }

    pub async fn all(&mut self) -> Result<Vec<Database>> {
        try_join_all(
            self.run_failsafe(|mut server_client| async move {
                server_client.databases_all(all_req()).await
            })
            .await?
            .databases
            .into_iter()
            .map(|proto_db| Database::new(proto_db, self.rpc_cluster_manager.clone())),
        )
        .await
    }

    async fn run_failsafe<F, P, R>(&mut self, task: F) -> Result<R>
    where
        F: Fn(rpc::ClusterClient) -> P,
        P: Future<Output = Result<R>>,
    {
        for client in self.rpc_cluster_manager.iter() {
            match task(client).await {
                // FIXME proper error handling
                Ok(r) => return Ok(r),
                Err(_) => (),
            }
        }
        Err(MESSAGES.client.unable_to_connect.to_err(vec![]))
    }
}
