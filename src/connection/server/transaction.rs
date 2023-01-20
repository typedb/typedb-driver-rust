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

use super::Session;
use crate::{
    common::{
        rpc::builder::transaction::{commit_req, open_req, rollback_req},
        DropGuard, Result, ServerRPC, SessionID, TransactionRPC, TransactionType,
    },
    connection::core,
    query::QueryManager,
};

#[derive(Clone)]
pub struct Transaction {
    type_: TransactionType,
    options: core::Options,
    pub query: QueryManager,
    rpc: TransactionRPC,
    is_open: Arc<AtomicCell<bool>>,
    // RAII guards
    _drop_guard: Arc<DropGuard>,
    _session_handle: Session,
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("type_", &self.type_)
            .field("options", &self.options)
            .finish()
    }
}

impl Transaction {
    pub(super) async fn new(
        session_id: SessionID,
        transaction_type: TransactionType,
        options: core::Options,
        network_latency: Duration,
        server_rpc: ServerRPC,
        _session_handle: Session,
    ) -> Result<Self> {
        let open_req = open_req(
            session_id,
            transaction_type.to_proto(),
            options.to_proto(),
            network_latency.as_millis() as i32,
        );
        let rpc = TransactionRPC::new(server_rpc, open_req).await?;
        let is_open = Arc::new(AtomicCell::new(true));
        let drop_callback = {
            let mut rpc = rpc.clone();
            let is_open = is_open.clone();
            move || {
                if transaction_type == TransactionType::Write && is_open.compare_exchange(true, false).is_ok() {
                    rpc.single_blocking(rollback_req()).unwrap();
                }
            }
        };
        Ok(Transaction {
            type_: transaction_type,
            options,
            query: QueryManager::new(rpc.clone()),
            rpc,
            is_open,
            _session_handle,
            _drop_guard: Arc::new(DropGuard::call_function(drop_callback)),
        })
    }

    pub async fn commit(mut self) -> Result {
        if self.is_open.compare_exchange(true, false).is_ok() {
            self.rpc.single_async(commit_req()).await?;
        }
        Ok(())
    }

    pub async fn rollback(mut self) -> Result {
        if self.is_open.compare_exchange(true, false).is_ok() {
            self.rpc.single_async(rollback_req()).await?;
        }
        Ok(())
    }
}
