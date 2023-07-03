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
        self.run_on_nodes(username, |server_connection, username| async move {
            server_connection.contains_user(username).await
        })
        .await
    }

    pub async fn create(&self, username: String, password: String) -> Result<()> {
        let mut error_buffer = Vec::with_capacity(self.connection.server_count());
        for server_connection in self.connection.connections() {
            match server_connection.create_user(username.clone(), password.clone()).await {
                Ok(()) => {
                    return Ok(());
                }
                Err(err) => error_buffer.push(format!("- {}: {}", server_connection.address(), err)),
            }
        }
        Err(ConnectionError::ClusterAllNodesFailed(error_buffer.join("\n")))?
    }

    pub async fn delete(&self, username: String) -> Result<()> {
        self.run_on_nodes(username, |server_connection, username| async move {
            server_connection.delete_user(username).await
        })
        .await
    }

    pub async fn get(&self, username: String) -> Result<Option<User>> {
        self.run_on_nodes(
            username,
            |server_connection, username| async move { server_connection.get_user(username).await },
        )
        .await
    }

    pub async fn set_password(&self, username: String, password: String) -> Result<()> {
        let mut error_buffer = Vec::with_capacity(self.connection.server_count());
        for server_connection in self.connection.connections() {
            match server_connection.set_user_password(username.clone(), password.clone()).await {
                Ok(()) => {
                    return Ok(());
                }
                Err(err) => error_buffer.push(format!("- {}: {}", server_connection.address(), err)),
            }
        }
        Err(ConnectionError::ClusterAllNodesFailed(error_buffer.join("\n")))?
    }

    async fn run_on_nodes<F, P, R>(&self, username: String, task: F) -> Result<R>
    where
        F: Fn(ServerConnection, String) -> P,
        P: Future<Output = Result<R>>,
    {
        let mut error_buffer = Vec::with_capacity(self.connection.server_count());
        for server_connection in self.connection.connections() {
            match task(server_connection.clone(), username.clone()).await {
                Ok(res) => {
                    return Ok(res);
                }
                Err(err) => error_buffer.push(format!("- {}: {}", server_connection.address(), err)),
            }
        }
        Err(ConnectionError::ClusterAllNodesFailed(error_buffer.join("\n")))?
    }
}
