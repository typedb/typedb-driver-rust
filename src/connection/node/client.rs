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

use tonic::transport::Channel;

use crate::{
    common::{rpc, Result, SessionType},
    connection::{
        core::options::Options,
        node::{DatabaseManager, Session},
    },
};

pub(crate) struct Client<T> {
    pub databases: DatabaseManager,
    pub(crate) rpc_client: rpc::Client<T>,
}

impl Client<Channel> {
    pub async fn new(rpc_client: rpc::Client<Channel>) -> Result<Self> {
        Ok(Self { databases: DatabaseManager::new(rpc_client.clone()), rpc_client })
    }

    pub async fn with_default_address() -> Result<Self> {
        let rpc_client = rpc::Client::connect("http://0.0.0.0:1729").await?;
        Self::new(rpc_client).await
    }

    pub async fn session(
        &mut self,
        database_name: &str,
        session_type: SessionType,
    ) -> Result<Session> {
        self.session_with_options(database_name, session_type, Options::default()).await
    }

    pub async fn session_with_options(
        &mut self,
        database_name: &str,
        session_type: SessionType,
        options: Options,
    ) -> Result<Session> {
        Session::new(database_name, session_type, options, &self.rpc_client).await
    }
}
