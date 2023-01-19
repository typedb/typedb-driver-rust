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
use futures::TryFutureExt;
use tokio::{
    spawn,
    time::{sleep_until, Instant},
};

use crate::{
    common::{
        error::ClientError,
        rpc::builder::session::{close_req, open_req, pulse_req},
        BlockingDispatcher, DropGuard, Result, ServerRPC, SessionID, SessionType, TransactionType,
    },
    connection::{core, server, ClientHandle},
};

#[derive(Clone)]
pub struct Session {
    database_name: String,
    session_type: SessionType,

    id: SessionID,
    server_rpc: ServerRPC,
    network_latency: Duration,

    is_open: Arc<AtomicCell<bool>>,

    close_guard: Arc<DropGuard>,
    session_pulse_task_close_guard: Arc<DropGuard>,
    client_handle: ClientHandle,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("database_name", &self.database_name)
            .field("session_type", &self.session_type)
            .field("id", &self.id)
            .field("server_rpc", &self.server_rpc)
            .field("network_latency", &self.network_latency)
            .field("is_open", &self.is_open)
            .finish()
    }
}

impl Session {
    const PULSE_INTERVAL: Duration = Duration::from_secs(5);

    pub(in crate::connection) async fn new(
        database_name: &str,
        session_type: SessionType,
        options: core::Options,
        mut server_rpc: ServerRPC,
        close_request_dispatcher: BlockingDispatcher,
        client_handle: ClientHandle,
    ) -> Result<Self> {
        let start_time = Instant::now();
        let open_req = open_req(database_name, session_type.to_proto(), options.to_proto());
        let res = server_rpc.session_open(open_req).await?;
        let id: SessionID = res.session_id.into();

        let pulse_task_handle = spawn(Self::session_pulse_task(server_rpc.clone(), id.clone()));

        let close_callback = {
            let id = id.clone();
            let mut server_rpc = server_rpc.clone();
            move || {
                close_request_dispatcher.dispatch(Box::pin(async move {
                    server_rpc.session_close(close_req(id)).map_ok(|_| ()).await
                }));
            }
        };

        Ok(Session {
            database_name: database_name.to_owned(),
            session_type,
            id,
            server_rpc,
            network_latency: Self::compute_network_latency(start_time, res.server_duration_millis),
            is_open: Arc::new(AtomicCell::new(true)),
            close_guard: Arc::new(DropGuard::new(close_callback)),
            session_pulse_task_close_guard: Arc::new(DropGuard::new(move || {
                pulse_task_handle.abort()
            })),
            client_handle,
        })
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn type_(&self) -> SessionType {
        self.session_type
    }

    pub fn is_open(&self) -> bool {
        self.is_open.load()
    }

    pub fn force_close(&mut self) {
        if self.is_open.compare_exchange(true, false).is_ok() {
            self.session_pulse_task_close_guard.release();
            self.close_guard.release();
        }
    }

    pub(crate) fn id(&self) -> &SessionID {
        &self.id
    }

    pub(crate) fn rpc(&self) -> ServerRPC {
        self.server_rpc.clone()
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
            self.clone(),
        )
        .await
    }

    fn compute_network_latency(start_time: Instant, server_duration_millis: i32) -> Duration {
        Instant::now() - start_time - Duration::from_millis(server_duration_millis as u64)
    }

    async fn session_pulse_task(mut session_rpc: ServerRPC, session_id: SessionID) {
        let mut next_pulse = Instant::now();
        loop {
            session_rpc.session_pulse(pulse_req(session_id.clone())).await.unwrap();
            next_pulse += Self::PULSE_INTERVAL;
            sleep_until(next_pulse).await;
        }
    }
}
