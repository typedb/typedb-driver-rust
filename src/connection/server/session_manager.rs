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

use std::{collections::HashMap, fmt};

use futures::future::try_join_all;

use super::Session;
use crate::{
    common::{
        rpc::builder::session::close_req, BlockingDispatcher, DispatcherThreadHandle, Result,
        ServerRPC, SessionID, SessionType,
    },
    connection::{core, ClientHandle},
};

pub(crate) struct SessionManager {
    session_rpcs: HashMap<SessionID, ServerRPC>,
    dispatcher: BlockingDispatcher,
    // RAII handle
    _dispatcher_thread_handle: DispatcherThreadHandle,
}

impl fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionManager").field("open_sessions", &self.session_rpcs).finish()
    }
}

impl SessionManager {
    pub(crate) fn new() -> Self {
        let session_rpcs = HashMap::new();
        let (dispatcher, _dispatcher_thread_handle) = BlockingDispatcher::new();
        Self { dispatcher, session_rpcs, _dispatcher_thread_handle }
    }

    pub async fn force_close(&mut self) {
        try_join_all(
            self.session_rpcs
                .drain()
                .map(|(id, mut rpc)| async move { rpc.session_close(close_req(id)).await }),
        )
        .await
        .unwrap();
    }

    pub(in crate::connection) async fn new_session(
        &mut self,
        database_name: &str,
        session_type: SessionType,
        server_rpc: ServerRPC,
        options: core::Options,
        client_handle: ClientHandle,
    ) -> Result<Session> {
        let session = Session::new(
            database_name,
            session_type,
            options,
            server_rpc.clone(),
            self.dispatcher.clone(),
            client_handle,
        )
        .await?;
        self.session_rpcs.insert(session.id().clone(), server_rpc);
        Ok(session)
    }

    pub(super) async fn session_closed(&mut self, id: SessionID) {
        let mut server_rpc = self.session_rpcs.remove(&id).unwrap();
        server_rpc.session_close(close_req(id)).await.unwrap();
    }
}
