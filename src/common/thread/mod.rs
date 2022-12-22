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
    thread::sleep,
    time::{Duration, Instant},
};

use crossbeam::channel::Receiver;
use futures::future::join_all;
use log::warn;

use crate::common::{
    error::ClientError,
    rpc::builder::session::{close_req, pulse_req},
    ServerRPC, SessionID,
};

const PULSE_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) async fn session_pulse_thread(
    sessions: Arc<RwLock<HashMap<SessionID, ServerRPC>>>,
    close_signal_source: Receiver<()>,
) {
    let mut next_run = Instant::now();
    loop {
        if close_signal_source.try_recv().is_ok() {
            break;
        }
        let reqs = sessions.read().unwrap().clone();
        join_all(reqs.into_iter().map(|(session_id, mut rpc)| async move {
            rpc.session_pulse(pulse_req(session_id)).await
        }))
        .await;
        next_run += PULSE_INTERVAL;
        sleep(next_run - Instant::now());
    }
}

const POLL_INTERVAL: Duration = Duration::from_millis(3);

pub(crate) async fn session_close_thread(
    session_rpcs: Arc<RwLock<HashMap<SessionID, ServerRPC>>>,
    close_signal_source: Receiver<()>,
    session_close_source: Receiver<SessionID>,
) {
    loop {
        while let Ok(session_id) = session_close_source.try_recv() {
            let mut rpc = session_rpcs.write().unwrap().remove(&session_id).unwrap();
            // TODO: the request errors harmlessly if the session is already closed. Protocol should
            //       expose the cause of the error and we can use that to decide whether to warn here.
            if rpc.session_close(close_req(session_id)).await.is_err() {
                warn!("{}", ClientError::SessionCloseFailed())
            }
        }
        if close_signal_source.try_recv().is_ok() {
            break;
        }
        sleep(POLL_INTERVAL);
    }
}
