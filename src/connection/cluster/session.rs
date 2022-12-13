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
        let server_session = database
            .run_on_primary_replica(|name, client, _| async {
                let name = name; // move just `name`
                server::Session::new(
                    &name.clone(),
                    session_type,
                    core::Options::default(),
                    client.into_core(),
                )
                .await
            })
            .await?;

        Ok(Self { database, session_type, server_session, rpc_cluster_client_manager })
    }

    //TODO options
    pub async fn transaction(&mut self, transaction_type: TransactionType) -> Result<Transaction> {
        let (session, transaction) = self
            .database
            .run_on_primary_replica(|name, client, is_first_run| {
                let session_type = self.session_type.clone();
                let session = &self.server_session;
                async move {
                    if is_first_run {
                        let transaction = session.transaction(transaction_type).await?;
                        Ok((None, transaction))
                    } else {
                        let server_session = server::Session::new(
                            &name.clone(),
                            session_type,
                            core::Options::default(),
                            client.into_core(),
                        )
                        .await?;
                        let transaction = server_session.transaction(transaction_type).await?;
                        Ok((Some(server_session), transaction))
                    }
                }
            })
            .await?;

        if let Some(session) = session {
            self.server_session = session;
        }

        Ok(transaction)
    }
}
