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
    open_message_sink: Sender<(SessionID, ServerRPC)>,
    close_message_sink: Sender<SessionID>,

    session_task_guard: Arc<DropGuard>,
}

impl fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionManager").finish()
    }
}

impl SessionManager {
    pub(crate) fn new() -> Self {
        let (open_message_sink, open_message_source) = unbounded();
        let (close_message_sink, close_message_source) = unbounded();
        let (session_task_shutdown_sink, session_task_shutdown_source) = bounded(0);
        spawn(Self::session_task(
            open_message_source,
            close_message_source,
            session_task_shutdown_source,
        ));
        Self {
            open_message_sink,
            close_message_sink,
            session_task_guard: Arc::new(DropGuard::send_message(session_task_shutdown_sink, ())),
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
            self.close_message_sink.clone(),
            client_handle,
        )
        .await?;
        self.open_message_sink.send((session.id().clone(), server_rpc)).unwrap();
        Ok(session)
    }

    pub fn force_close(self) {
        self.session_task_guard.release();
    }

    async fn session_task(
        open_message_source: Receiver<(SessionID, ServerRPC)>,
        close_message_source: Receiver<SessionID>,
        shutdown_source: Receiver<()>,
    ) {
        let mut session_rpcs = HashMap::new();
        loop {
            session_rpcs.extend(open_message_source.try_iter());
            if shutdown_source.try_recv().is_ok() {
                join_all(
                    session_rpcs
                        .into_iter()
                        .map(|(id, mut rpc)| async move { rpc.session_close(close_req(id)).await }),
                )
                .await;
                break;
            }

            let sessions_to_close: Vec<_> = close_message_source
                .try_iter()
                .map(|id| (session_rpcs.remove(&id).unwrap(), id))
                .collect();
            join_all(
                sessions_to_close
                    .into_iter()
                    .map(|(mut rpc, id)| async move { rpc.session_close(close_req(id)).await }),
            )
            .await;

            sleep(POLL_INTERVAL).await;
        }
    }
}
