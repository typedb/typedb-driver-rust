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

use crossbeam::channel::{bounded, Sender};

use crate::{
    common::{
        thread::{session_close_thread, session_pulse_thread},
        DropGuard, Result, ServerRPC, SessionID, SessionType,
    },
    connection::{core, server},
};

#[derive(Debug)]
pub(crate) struct SessionManager {
    close_message_sink: Sender<SessionID>,

    server_rpc: ServerRPC,
    session_rpcs: Arc<RwLock<HashMap<SessionID, ServerRPC>>>,
    pulse_thread_guard: DropGuard,
    session_close_thread_guard: DropGuard,
}

impl SessionManager {
    pub(crate) fn new(server_rpc: ServerRPC) -> Self {
        let session_rpcs = Arc::new(RwLock::new(HashMap::new()));

        let (pulse_thread_close_signal, close_signal_source) = bounded(1);
        server_rpc
            .executor()
            .spawn_ok(session_pulse_thread(session_rpcs.clone(), close_signal_source.clone()));

        let (session_close_thread_close_signal, close_signal_source) = bounded(0);
        let (close_message_sink, close_message_source) = bounded(256);
        server_rpc.executor().spawn_ok(session_close_thread(
            session_rpcs.clone(),
            close_signal_source,
            close_message_source,
        ));

        Self {
            session_rpcs,
            server_rpc,
            pulse_thread_guard: DropGuard::new(pulse_thread_close_signal, ()),
            session_close_thread_guard: DropGuard::new(session_close_thread_close_signal, ()),
            close_message_sink,
        }
    }

    pub(crate) async fn session(
        &mut self,
        database_name: &str,
        session_type: SessionType,
        options: core::Options,
    ) -> Result<server::Session> {
        let session = server::Session::new(
            database_name,
            session_type,
            options,
            self.server_rpc.clone(),
            self.close_message_sink.clone(),
        )
        .await?;
        self.session_rpcs.write().unwrap().insert(session.id().clone(), self.server_rpc.clone());
        Ok(session)
    }
}
