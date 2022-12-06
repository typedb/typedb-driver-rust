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

use std::{future::Future, pin::Pin, result::Result as StdResult};

use tonic::{Response, Status};

use crate::{common::Result, connection::cluster::ServerClient};

type TonicResult<R> = StdResult<Response<R>, Status>;

#[derive(Clone, Debug)]
pub struct Replica {
    pub(crate) address: String,
    pub(crate) database_name: String,
    pub(crate) is_primary: bool,
    pub(crate) is_preferred: bool,
}

#[derive(Clone, Debug)]
pub struct Database {
    pub(crate) replicas: Vec<Replica>,
    pub(crate) preferred_replica: Replica,
}

#[derive(Clone, Debug)]
pub struct DatabaseManager {}

impl DatabaseManager {
    pub(crate) async fn new() -> Result<Self> {
        Ok(Self {})
    }

    pub async fn contains(&mut self, name: &str) -> Result<bool> {
        self.run_failsafe(|this, server_client, _| server_client.databases.contains(name)).await
    }

    pub async fn run_failsafe<F, R>(&mut self, task: F) -> Result<R>
    where
        F: for<'a> FnOnce(
            &'a mut Self,
            &mut ServerClient,
            &Replica,
        ) -> Pin<Box<dyn Future<Output = TonicResult<R>> + 'a>>,
    {
    }
}
