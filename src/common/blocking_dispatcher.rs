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

use std::{future::Future, pin::Pin};

use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use futures::future::join_all;
use log::warn;
use tokio::{spawn, time::sleep};

use crate::common::{Result, POLL_INTERVAL};

#[derive(Clone)]
pub(crate) struct BlockingDispatcher {
    message_sink: Sender<SyncFuture>,
}

impl BlockingDispatcher {
    pub fn new() -> (Self, DispatcherThreadHandle) {
        let (message_sink, message_source) = unbounded();
        (Self { message_sink }, DispatcherThreadHandle::new(message_source))
    }

    pub fn dispatch(&self, future: Pin<Box<dyn Future<Output = Result<()>> + Send>>) {
        let (backchannel_sink, backchannel_source) = bounded(0);
        let request = SyncFuture { future, backchannel: backchannel_sink };
        self.message_sink.send(request).unwrap();
        backchannel_source.recv().unwrap();
    }
}

pub(crate) struct DispatcherThreadHandle {
    shutdown_sink: Sender<()>,
}

impl DispatcherThreadHandle {
    fn new(message_source: Receiver<SyncFuture>) -> Self {
        let (shutdown_sink, shutdown_source) = bounded(0);
        spawn(Self::listener_thread(message_source, shutdown_source));
        Self { shutdown_sink }
    }

    async fn listener_thread(message_source: Receiver<SyncFuture>, shutdown_source: Receiver<()>) {
        loop {
            if shutdown_source.try_recv().is_ok() {
                break;
            }

            join_all(message_source.try_iter().map(|request| async move {
                if let Err(err) = request.future.await {
                    warn!("{}", err);
                }
                request.backchannel.send(()).unwrap();
            }))
            .await;
            sleep(POLL_INTERVAL).await;
        }
    }
}

impl Drop for DispatcherThreadHandle {
    fn drop(&mut self) {
        self.shutdown_sink.send(()).unwrap()
    }
}

struct SyncFuture {
    future: Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    backchannel: Sender<()>,
}
