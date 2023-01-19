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

use super::{Client, Database};
use crate::{
    common::{error::ClientError, Result, SessionType, TransactionType},
    connection::{core, server},
};

#[derive(Clone, Debug)]
pub struct Session {
    database: Database,
    session_type: SessionType,
    server_session: server::Session,
    client: Client,
}

impl Session {
    pub(super) async fn new(
        mut database: Database,
        session_type: SessionType,
        client: Client,
        // TODO options
    ) -> Result<Self> {
        let server_session = database
            .run_failsafe(|database, server_rpc, _| async {
                let database = database;
                client
                    .session_manager()
                    .lock()
                    .unwrap()
                    .new_session(
                        database.name(),
                        session_type,
                        server_rpc.into(),
                        core::Options::default(),
                        client.clone().into(),
                    )
                    .await
            })
            .await?;

        Ok(Self { database, session_type, server_session, client })
    }

    pub fn database_name(&self) -> &str {
        self.database.name()
    }

    pub fn type_(&self) -> SessionType {
        self.session_type
    }

    pub fn is_open(&self) -> bool {
        self.server_session.is_open()
    }

    pub fn force_close(&mut self) {
        self.server_session.force_close();
    }

    pub async fn transaction(
        &mut self,
        transaction_type: TransactionType,
    ) -> Result<server::Transaction> {
        self.transaction_with_options(transaction_type, core::Options::new_core()).await
    }

    pub async fn transaction_with_options(
        &mut self,
        transaction_type: TransactionType,
        _options: core::Options, // TODO options
    ) -> Result<server::Transaction> {
        if !self.is_open() {
            Err(ClientError::SessionIsClosed())?
        }

        let (session, transaction) = self
            .database
            .run_failsafe(|database, server_rpc, is_first_run| {
                let session_type = self.session_type;
                let mut session = self.server_session.clone();
                let client = self.client.clone();
                async move {
                    if is_first_run {
                        let transaction = session.transaction(transaction_type).await?;
                        Ok((session, transaction))
                    } else {
                        let mut server_session = client
                            .session_manager()
                            .lock()
                            .unwrap()
                            .new_session(
                                database.name(),
                                session_type,
                                server_rpc.into(),
                                core::Options::default(),
                                client.clone().into(),
                            )
                            .await?;
                        let transaction = server_session.transaction(transaction_type).await?;
                        Ok((server_session, transaction))
                    }
                }
            })
            .await?;

        self.server_session = session;
        Ok(transaction)
    }
}
