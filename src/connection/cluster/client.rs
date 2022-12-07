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

use std::{collections::HashSet, sync::Arc};

use super::{DatabaseManager, Session};
use crate::common::{rpc, Credential, Result};

pub struct Client {
    rpc_cluster_client_manager: Arc<rpc::ClusterClientManager>,
    databases: DatabaseManager,
    sessions: Vec<Session>,
}

impl Client {
    pub async fn new(init_addresses: &HashSet<String>, credential: Credential) -> Result<Self> {
        let addresses =
            rpc::ClusterClientManager::fetch_current_addresses(init_addresses, &credential).await?;
        let rpc_cluster_client_manager =
            rpc::ClusterClientManager::new(addresses.clone(), credential).await?;

        let databases = DatabaseManager::new(rpc_cluster_client_manager.clone()).await?;

        Ok(Self { rpc_cluster_client_manager, databases, sessions: Vec::new() })
    }

    pub fn databases(&mut self) -> &mut DatabaseManager {
        &mut self.databases
    }
}
