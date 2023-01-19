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
    fmt,
    sync::{Arc, RwLock},
};

use crate::{
    common::{
        BlockingDispatcher, DispatcherThreadHandle, Result, ServerRPC, SessionID, SessionType,
    },
    connection::{core, server, ClientHandle},
};

pub(crate) struct SessionManager {
    session_rpcs: Arc<RwLock<HashMap<SessionID, ServerRPC>>>,
    dispatcher: BlockingDispatcher,
    actor_handle: DispatcherThreadHandle,
}

impl fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("typedb_client::server::SessionManager").finish()
    }
}

impl SessionManager {
    pub(crate) fn new() -> Arc<Self> {
        let session_rpcs = Arc::new(RwLock::new(HashMap::new()));

        let (dispatcher, actor_handle) = BlockingDispatcher::new();

        Arc::new(Self { dispatcher, session_rpcs, actor_handle })
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
            self.dispatcher.clone(),
            client_handle,
        )
        .await?;
        self.session_rpcs.write().unwrap().insert(session.id().clone(), server_rpc);
        Ok(session)
    }
}
