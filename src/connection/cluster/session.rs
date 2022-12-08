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

use std::sync::Arc;

use super::Database;
use crate::{
    common::{rpc, Result, SessionType, TransactionType},
    connection::{core, server, server::Transaction},
};

pub struct Session {
    pub database: Database,
    pub session_type: SessionType,

    server_session: server::Session,
    rpc_cluster_client_manager: Arc<rpc::ClusterClientManager>,
}

impl Session {
    // TODO options
    pub(crate) async fn new(
        mut database: Database,
        session_type: SessionType,
        rpc_cluster_client_manager: Arc<rpc::ClusterClientManager>,
    ) -> Result<Self> {
        let primary_address = &database.primary_replica().await.address;
        let server_session = server::Session::new(
            &database.name,
            session_type,
            core::Options::default(),
            rpc_cluster_client_manager.get(primary_address).into_core(),
        )
        .await?;
        Ok(Self { database, session_type, server_session, rpc_cluster_client_manager })
    }

    //TODO options
    pub async fn transaction(&self, transaction_type: TransactionType) -> Result<Transaction> {
        self.server_session.transaction(transaction_type).await
    }
}
