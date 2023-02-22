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

use crossbeam::channel::{
    bounded as bounded_blocking, Receiver as SyncReceiver, Sender as SyncSender,
};
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel as unbounded_async, UnboundedReceiver, UnboundedSender},
        oneshot::{channel as oneshot_async, Sender as AsyncOneshotSender},
    },
};

use crate::{
    common::{address::Address, Result},
    connection::{
        network::{
            channel::{open_encrypted_channel, open_plaintext_channel, GRPCChannel},
            message::{Request, Response},
            stub::RPCStub,
        },
        runtime::BackgroundRuntime,
    },
    error::InternalError,
    Credential,
};

fn oneshot_blocking<T>() -> (SyncSender<T>, SyncReceiver<T>) {
    bounded_blocking::<T>(0)
}

#[derive(Debug)]
enum OneShotSender<T> {
    Async(AsyncOneshotSender<Result<T>>),
    Blocking(SyncSender<Result<T>>),
}

impl<T> OneShotSender<T> {
    fn send(self, response: Result<T>) -> Result {
        match self {
            Self::Async(sink) => sink.send(response).map_err(|_| InternalError::SendError().into()),
            Self::Blocking(sink) => sink.send(response).map_err(Into::into),
        }
    }
}

pub(in crate::connection) struct RPCTransmitter {
    request_sink: UnboundedSender<(Request, OneShotSender<Response>)>,
    shutdown_sink: UnboundedSender<()>,
}

impl RPCTransmitter {
    pub(in crate::connection) fn start_plaintext(
        address: Address,
        runtime: &BackgroundRuntime,
    ) -> Self {
        let (request_sink, request_source) = unbounded_async();
        let (shutdown_sink, shutdown_source) = unbounded_async();
        runtime.spawn(async move {
            let channel = open_plaintext_channel(address.clone());
            let rpc = RPCStub::new(address.clone(), channel, None).await.unwrap();
            Self::dispatcher_loop(rpc, request_source, shutdown_source).await;
        });
        Self { request_sink, shutdown_sink }
    }

    pub(in crate::connection) fn start_encrypted(
        address: Address,
        credential: Credential,
        runtime: &BackgroundRuntime,
    ) -> Self {
        let (request_sink, request_source) = unbounded_async();
        let (shutdown_sink, shutdown_source) = unbounded_async();
        runtime.spawn(async move {
            let (channel, callcreds) = open_encrypted_channel(address.clone(), credential).unwrap();
            let rpc = RPCStub::new(address.clone(), channel, Some(callcreds)).await.unwrap();
            Self::dispatcher_loop(rpc, request_source, shutdown_source).await;
        });
        Self { request_sink, shutdown_sink }
    }

    pub(in crate::connection) async fn request_async(&self, request: Request) -> Result<Response> {
        let (response_sink, response) = oneshot_async();
        self.request_sink.send((request, OneShotSender::Async(response_sink)))?;
        response.await?
    }

    pub(in crate::connection) fn request_blocking(&self, request: Request) -> Result<Response> {
        let (response_sink, response) = oneshot_blocking();
        self.request_sink.send((request, OneShotSender::Blocking(response_sink)))?;
        response.recv()?
    }

    pub(in crate::connection) fn force_close(&self) {
        self.shutdown_sink.send(()).ok();
    }

    async fn dispatcher_loop<Channel: GRPCChannel>(
        rpc: RPCStub<Channel>,
        mut request_source: UnboundedReceiver<(Request, OneShotSender<Response>)>,
        mut shutdown_signal: UnboundedReceiver<()>,
    ) {
        while let Some((request, response_sink)) = select! {
            request = request_source.recv() => request,
            _ = shutdown_signal.recv() => None,
        } {
            let rpc = rpc.clone();
            tokio::spawn(async move {
                let response = Self::send_request(rpc, request).await;
                response_sink.send(response).ok();
            });
        }
    }

    async fn send_request<Channel: GRPCChannel>(
        mut rpc: RPCStub<Channel>,
        request: Request,
    ) -> Result<Response> {
        match request {
            Request::ServersAll => {
                rpc.servers_all(request.try_into()?).await.and_then(Response::try_from)
            }

            Request::DatabasesContains { .. } => {
                rpc.databases_contains(request.try_into()?).await.map(Response::from)
            }
            Request::DatabaseCreate { .. } => {
                rpc.databases_create(request.try_into()?).await.map(Response::from)
            }
            Request::DatabaseGet { .. } => {
                rpc.databases_get(request.try_into()?).await.and_then(Response::try_from)
            }
            Request::DatabasesAll => {
                rpc.databases_all(request.try_into()?).await.and_then(Response::try_from)
            }

            Request::DatabaseDelete { .. } => {
                rpc.database_delete(request.try_into()?).await.map(Response::from)
            }
            Request::DatabaseSchema { .. } => {
                rpc.database_schema(request.try_into()?).await.map(Response::from)
            }
            Request::DatabaseTypeSchema { .. } => {
                rpc.database_type_schema(request.try_into()?).await.map(Response::from)
            }
            Request::DatabaseRuleSchema { .. } => {
                rpc.database_rule_schema(request.try_into()?).await.map(Response::from)
            }

            Request::SessionOpen { .. } => {
                rpc.session_open(request.try_into()?).await.map(Response::from)
            }
            Request::SessionPulse { .. } => {
                rpc.session_pulse(request.try_into()?).await.map(Response::from)
            }
            Request::SessionClose { .. } => {
                rpc.session_close(request.try_into()?).await.map(Response::from)
            }

            Request::Transaction(transaction_request) => {
                rpc.transaction(transaction_request.into()).await.map(Response::from)
            }
        }
    }
}
