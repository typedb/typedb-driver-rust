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

use std::{fmt, time::Duration};

use super::Session;
use crate::{
    common::{
        rpc::builder::transaction::{commit_req, open_req, rollback_req},
        Result, ServerRPC, SessionID, TransactionRPC, TransactionType,
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
    session_handle: Session,
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
    pub(in crate::connection) async fn new(
        session_id: SessionID,
        transaction_type: TransactionType,
        options: core::Options,
        network_latency: Duration,
        server_rpc: ServerRPC,
        session_handle: Session,
    ) -> Result<Self> {
        let open_req = open_req(
            session_id,
            transaction_type.to_proto(),
            options.to_proto(),
            network_latency.as_millis() as i32,
        );
        let rpc = TransactionRPC::new(server_rpc, open_req).await?;
        Ok(Transaction {
            type_: transaction_type,
            options,
            query: QueryManager::new(rpc.clone()),
            rpc,
            session_handle,
        })
    }

    pub async fn commit(&mut self) -> Result {
        self.rpc.single_async(commit_req()).await?;
        Ok(())
    }

    pub async fn rollback(&mut self) -> Result {
        self.rpc.single_async(rollback_req()).await?;
        Ok(())
    }
}
