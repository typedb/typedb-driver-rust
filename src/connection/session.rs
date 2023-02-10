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

use std::{sync::RwLock, time::Duration};

use crossbeam::atomic::AtomicCell;

use crate::{
    common::{error::ClientError, Result, SessionID, SessionType, TransactionType},
    connection::{server, Options},
    Database,
};

#[derive(Debug)]
pub struct Session {
    database: Database,
    server_session_id: RwLock<SessionID>,
    session_type: SessionType,
    network_latency: AtomicCell<Duration>,
    is_open: AtomicCell<bool>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.force_close()
    }
}

impl Session {
    pub async fn new(database: Database, session_type: SessionType) -> Result<Self> {
        let (server_session_id, network_latency) = database
            .run_failsafe(|database, _, _| async move {
                database.open_session(session_type, Options::default()).await
            })
            .await?;

        Ok(Self {
            database,
            session_type,
            server_session_id: RwLock::new(server_session_id),
            network_latency: AtomicCell::new(network_latency),
            is_open: AtomicCell::new(true),
        })
    }

    pub fn database_name(&self) -> &str {
        self.database.name()
    }

    pub fn type_(&self) -> SessionType {
        self.session_type
    }

    pub fn is_open(&self) -> bool {
        self.is_open.load()
    }

    pub fn force_close(&self) {
        if self.is_open.compare_exchange(true, false).is_ok() {
            self.database.close_session(self.server_session_id.read().unwrap().clone()).ok();
            // FIXME log error?
        }
    }

    pub async fn transaction(
        &self,
        transaction_type: TransactionType,
    ) -> Result<server::Transaction> {
        self.transaction_with_options(transaction_type, Options::new_core()).await
    }

    pub async fn transaction_with_options<'me>(
        &'me self,
        transaction_type: TransactionType,
        options: Options, // TODO options
    ) -> Result<server::Transaction> {
        if !self.is_open() {
            Err(ClientError::SessionIsClosed())?
        }

        let (session_id, network_latency, transaction_stream) = self
            .database
            .run_failsafe(|database, _, is_first_run| {
                let session_id = self.server_session_id.read().unwrap().clone();
                let session_type = self.session_type;
                let options = options.clone();
                let network_latency = self.network_latency.load();
                async move {
                    let (session_id, network_latency) = if is_first_run {
                        (session_id.clone(), network_latency.clone())
                    } else {
                        database.open_session(session_type, options.clone()).await?
                    };
                    Ok((
                        session_id.clone(),
                        network_latency.clone(),
                        database
                            .open_transaction(
                                session_id,
                                transaction_type,
                                options.clone(),
                                network_latency,
                            )
                            .await?,
                    ))
                }
            })
            .await?;

        *self.server_session_id.write().unwrap() = session_id;
        self.network_latency.store(network_latency);
        server::Transaction::<'me>::new(transaction_stream)
    }
}
