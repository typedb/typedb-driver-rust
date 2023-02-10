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

use std::{
    fmt::{Display, Formatter},
    time::{Duration, Instant},
};

use crate::{
    common::{Result, ServerConnection, SessionID, TransactionStream},
    connection::Options,
    SessionType, TransactionType,
};

#[derive(Clone, Debug)]
pub struct Database {
    name: String,
    connection: ServerConnection,
}

impl Database {
    pub(crate) fn new(name: String, connection: ServerConnection) -> Self {
        Database { name, connection }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub async fn delete(self) -> Result {
        self.connection.delete_database(self.name).await
    }

    pub async fn schema(&self) -> Result<String> {
        self.connection.database_schema(self.name.clone()).await
    }

    pub async fn type_schema(&self) -> Result<String> {
        self.connection.database_type_schema(self.name.clone()).await
    }

    pub async fn rule_schema(&self) -> Result<String> {
        self.connection.database_rule_schema(self.name.clone()).await
    }

    pub(crate) async fn open_session(
        &self,
        session_type: SessionType,
        options: Options,
    ) -> Result<(SessionID, Duration)> {
        let start = Instant::now();
        let (session_id, server_duration) =
            self.connection.open_session(self.name.clone(), session_type, options).await?;
        Ok((session_id, start.elapsed() - server_duration))
    }

    pub(crate) async fn open_transaction(
        &self,
        session_id: SessionID,
        transaction_type: TransactionType,
        options: Options,
        network_latency: Duration,
    ) -> Result<TransactionStream> {
        self.connection
            .open_transaction(session_id, transaction_type, options, network_latency)
            .await
    }
}

impl Display for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}
