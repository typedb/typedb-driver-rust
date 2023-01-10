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

use std::time::{Duration, Instant};

use crossbeam::{atomic::AtomicCell, channel::Sender};

use crate::{
    common::{
        error::ClientError, rpc::builder::session::open_req, DropGuard, Result, ServerRPC,
        SessionID, SessionType, TransactionType,
    },
    connection::{core, server},
};

#[derive(Debug)]
pub struct Session {
    database_name: String,
    session_type: SessionType,
    id: SessionID,
    server_rpc: ServerRPC,
    is_open_atomic: AtomicCell<bool>,
    network_latency: Duration,
    close_guard: DropGuard<SessionID>,
}

impl Session {
    pub(crate) async fn new(
        database_name: &str,
        session_type: SessionType,
        options: core::Options,
        mut server_rpc: ServerRPC,
        on_close: Sender<SessionID>,
    ) -> Result<Self> {
        let start_time = Instant::now();
        let open_req = open_req(database_name, session_type.to_proto(), options.to_proto());
        let res = server_rpc.session_open(open_req).await?;
        let id: SessionID = res.session_id.into();
        Ok(Session {
            database_name: database_name.to_owned(),
            session_type,
            close_guard: DropGuard::new(on_close, id.clone()),
            id,
            server_rpc,
            is_open_atomic: AtomicCell::new(true),
            network_latency: Self::compute_network_latency(start_time, res.server_duration_millis),
        })
    }

    pub(crate) fn id(&self) -> &SessionID {
        &self.id
    }

    pub(crate) fn rpc(&self) -> ServerRPC {
        self.server_rpc.clone()
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn type_(&self) -> SessionType {
        self.session_type
    }

    pub fn is_open(&self) -> bool {
        self.is_open_atomic.load()
    }

    pub async fn transaction(
        &self,
        transaction_type: TransactionType,
    ) -> Result<server::Transaction> {
        self.transaction_with_options(transaction_type, core::Options::default()).await
    }

    pub async fn transaction_with_options(
        &self,
        transaction_type: TransactionType,
        options: core::Options,
    ) -> Result<server::Transaction> {
        if !self.is_open() {
            Err(ClientError::SessionIsClosed())?
        }
        server::Transaction::new(
            self.id.clone(),
            transaction_type,
            options,
            self.network_latency,
            self.server_rpc.clone(),
        )
        .await
    }

    fn compute_network_latency(start_time: Instant, server_duration_millis: i32) -> Duration {
        Instant::now() - start_time - Duration::from_millis(server_duration_millis as u64)
    }
}
