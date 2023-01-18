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

use tokio::runtime::{Handle, RuntimeFlavor};

use super::{DatabaseManager, Session};
use crate::{
    common::{ClusterRPC, Credential, Result, SessionType},
    server,
};

#[derive(Clone, Debug)]
pub struct Client {
    session_manager: Arc<server::SessionManager>,
    databases: DatabaseManager,
    cluster_rpc: Arc<ClusterRPC>,
}

impl Client {
    pub async fn new<T: AsRef<str>>(init_addresses: &[T], credential: Credential) -> Result<Self> {
        if Handle::current().runtime_flavor() == RuntimeFlavor::CurrentThread {
            todo!();
        }
        let addresses = ClusterRPC::fetch_current_addresses(init_addresses, &credential).await?;
        let cluster_rpc = ClusterRPC::new(addresses, credential)?;
        Ok(Self {
            session_manager: server::SessionManager::new(),
            databases: DatabaseManager::new(cluster_rpc.clone()),
            cluster_rpc,
        })
    }

    pub fn force_close(self) {
        self.session_manager.force_close();
        // TODO: also force close database connections
    }

    pub fn databases(&mut self) -> &mut DatabaseManager {
        &mut self.databases
    }

    pub async fn session(
        &mut self,
        database_name: &str,
        session_type: SessionType,
    ) -> Result<Session> {
        Session::new(self.databases.get(database_name).await?, session_type, self.clone()).await
    }

    pub(super) fn session_manager(&self) -> &Arc<server::SessionManager> {
        &self.session_manager
    }
}
