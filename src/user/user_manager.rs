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

use std::future::Future;

use crate::{common::Result, connection::ServerConnection, error::ConnectionError, Connection, User};

#[derive(Clone, Debug)]
pub struct UserManager {
    connection: Connection,
}

impl UserManager {
    pub fn new(connection: Connection) -> Self {
        Self { connection }
    }

    // pub async fn contains(&self, username: String) -> Result<bool> {
    //     self.run_failsafe(name.into(), move |database, server_connection, _| async move {
    //         server_connection.database_exists(database.name().to_owned()).await
    //     }).await
    //     self.connection.
    // }

    pub async fn all(&self) -> Result<Vec<User>> {
        let mut error_buffer = Vec::with_capacity(self.connection.server_count());
        for server_connection in self.connection.connections() {
            match server_connection.all_users().await {
                Ok(list) => {
                    return Ok(list);
                }
                Err(err) => error_buffer.push(format!("- {}: {}", server_connection.address(), err)),
            }
        }
        Err(ConnectionError::ClusterAllNodesFailed(error_buffer.join("\n")))?
    }

    pub async fn contains(&self, username: String) -> Result<bool> {
        let mut error_buffer = Vec::with_capacity(self.connection.server_count());
        for server_connection in self.connection.connections() {
            match server_connection.contains_user(username.clone()).await {
                Ok(contains) => {
                    return Ok(contains);
                }
                Err(err) => error_buffer.push(format!("- {}: {}", server_connection.address(), err)),
            }
        }
        Err(ConnectionError::ClusterAllNodesFailed(error_buffer.join("\n")))?
    }

    // pub fn all() -> Vec<User> {
    //
    // }
    // async fn run_failsafe<F, P, R>(&self, name: String, task: F) -> Result<R>
    //     where
    //         F: Fn(ServerDatabase, ServerConnection, bool) -> P,
    //         P: Future<Output = Result<R>>,
    // {
    //     Database::get(name, self.connection.clone()).await?.run_failsafe(&task).await
    // }
}
