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

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crossbeam::channel::Sender;
use tokio::task::JoinHandle;

use crate::{
    common::{
        rpc::{blocking_dispatcher, blocking_dispatcher::Request},
        Result, ServerRPC, SessionID, SessionType, POLL_INTERVAL,
    },
    connection::{core, server, ClientHandle},
};

#[derive(Debug)]
pub(crate) struct SessionManager {
    close_message_sink: Sender<Request>,
    session_rpcs: Arc<RwLock<HashMap<SessionID, ServerRPC>>>,
    session_close_task: JoinHandle<()>,
    session_close_task_close_signal: Sender<()>,
}

impl SessionManager {
    pub(crate) fn new() -> Arc<Self> {
        let session_rpcs = Arc::new(RwLock::new(HashMap::new()));

        let (close_message_sink, session_close_task_close_signal, session_close_task) =
            blocking_dispatcher::new();

        Arc::new(Self {
            close_message_sink,
            session_rpcs,
            session_close_task,
            session_close_task_close_signal,
        })
    }

    pub fn force_close(self: Arc<Self>) {
        todo!()
    }

    pub(in crate::connection) async fn new_session(
        self: &Arc<Self>,
        database_name: &str,
        session_type: SessionType,
        server_rpc: ServerRPC,
        options: core::Options,
        client_handle: ClientHandle,
    ) -> Result<server::Session> {
        let session = server::Session::new(
            database_name,
            session_type,
            options,
            server_rpc.clone(),
            self.close_message_sink.clone(),
            client_handle,
        )
        .await?;
        self.session_rpcs.write().unwrap().insert(session.id().clone(), server_rpc);
        Ok(session)
    }
}

impl Drop for SessionManager {
    fn drop(&mut self) {
        self.session_close_task_close_signal.send(()).unwrap();
        while !self.session_close_task.is_finished() {
            std::thread::sleep(POLL_INTERVAL)
        }
    }
}
