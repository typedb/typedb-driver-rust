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

use std::{fmt, sync::Arc, time::Duration};

use crossbeam::atomic::AtomicCell;
use tokio::time::Instant;

use crate::{
    common::{error::ClientError, Connection, Result, SessionID, SessionType, TransactionType},
    connection::{core, server, ClientHandle},
};

pub struct Session {
    database_name: String,
    session_type: SessionType,
    id: SessionID,
    network_latency: Duration,
    connection: Connection,
    is_open: Arc<AtomicCell<bool>>,
    // RAII guards
    _client_handle: ClientHandle,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("database_name", &self.database_name)
            .field("session_type", &self.session_type)
            .field("id", &self.id)
            .field("network_latency", &self.network_latency)
            .field("is_open", &self.is_open)
            .finish()
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.force_close()
    }
}

impl Session {
    pub(in crate::connection) async fn new(
        database_name: &str,
        session_type: SessionType,
        options: core::Options,
        connection: Connection,
        _client_handle: ClientHandle,
    ) -> Result<Self> {
        let start_time = Instant::now();
        let (id, server_duration) =
            connection.open_session(database_name.to_string(), session_type, options).await?;

        Ok(Session {
            database_name: database_name.to_owned(),
            session_type,
            id,
            connection,
            network_latency: start_time.elapsed() - server_duration,
            is_open: Arc::new(AtomicCell::new(true)),
            _client_handle,
        })
    }

    pub fn database_name(&self) -> &str {
        self.database_name.as_str()
    }

    pub fn type_(&self) -> SessionType {
        self.session_type
    }

    pub fn is_open(&self) -> bool {
        self.is_open.load()
    }

    pub fn force_close(&self) {
        if self.is_open.compare_exchange(true, false).is_ok() {
            self.connection.close_session(self.id.clone()).ok(); // FiXME log error?
        }
    }

    pub async fn transaction(
        &self,
        transaction_type: TransactionType,
    ) -> Result<server::Transaction> {
        self.transaction_with_options(transaction_type, core::Options::new_core()).await
    }

    pub async fn transaction_with_options<'me>(
        &'me self,
        transaction_type: TransactionType,
        options: core::Options,
    ) -> Result<server::Transaction> {
        if !self.is_open() {
            Err(ClientError::SessionIsClosed())?
        }
        let transaction_stream = self
            .connection
            .open_transaction(self.id.clone(), transaction_type, options, self.network_latency)
            .await?;
        server::Transaction::<'me>::new(transaction_stream)
    }
}
