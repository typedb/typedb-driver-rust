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

use std::{collections::HashMap, fmt, sync::Arc};

use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use futures::future::join_all;
use tokio::{spawn, time::sleep};

use super::Session;
use crate::{
    common::{
        rpc::builder::session::close_req, DropGuard, Result, ServerRPC, SessionID, SessionType,
        POLL_INTERVAL,
    },
    connection::{core, ClientHandle},
};

#[derive(Clone)]
pub(crate) struct SessionManager {
    open_sink: Sender<(SessionID, ServerRPC)>,
    close_sink: Sender<SessionID>,

    _background_handler_guard: Arc<DropGuard>,
}

impl fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionManager").finish()
    }
}

impl SessionManager {
    pub(crate) fn new() -> Self {
        let (open_sink, open_source) = unbounded();
        let (close_sink, close_source) = unbounded();
        let (session_task_shutdown_sink, session_task_shutdown_source) = bounded(0);
        let background_lifetime_handler = spawn(Self::background_lifetime_handler(
            open_source,
            close_source,
            session_task_shutdown_source,
        ));
        Self {
            open_sink,
            close_sink,
            _background_handler_guard: Arc::new(DropGuard::call_function(move || {
                session_task_shutdown_sink.send(()).unwrap();
                while !background_lifetime_handler.is_finished() {
                    std::thread::sleep(POLL_INTERVAL);
                }
            })),
        }
    }

    pub(in crate::connection) async fn new_session(
        &self,
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
            self.clone(),
            client_handle,
        )
        .await?;
        self.open_sink.send((session.id().clone(), server_rpc)).unwrap();
        Ok(session)
    }

    pub(super) fn session_closed(&self, session_id: SessionID) {
       self.close_sink.send(session_id).unwrap();
    }

    pub fn force_close(self) {
        self._background_handler_guard.release();
    }

    async fn background_lifetime_handler(
        open_source: Receiver<(SessionID, ServerRPC)>,
        close_source: Receiver<SessionID>,
        shutdown_source: Receiver<()>,
    ) {
        let mut session_rpcs = HashMap::new();
        loop {
            session_rpcs.extend(open_source.try_iter());
            if shutdown_source.try_recv().is_ok() {
                Self::close_all(session_rpcs.into_iter()).await;
                break;
            }

            Self::close_all(close_source.try_iter().map(|id| {
                let rpc = session_rpcs.remove(&id).unwrap();
                (id, rpc)
            }))
            .await;

            sleep(POLL_INTERVAL).await;
        }
    }

    async fn close_all(sessions_to_close: impl Iterator<Item = (SessionID, ServerRPC)>) {
        join_all(
            sessions_to_close
                .map(|(id, mut rpc)| async move { rpc.session_close(close_req(id)).await }),
        )
        .await;
    }
}
