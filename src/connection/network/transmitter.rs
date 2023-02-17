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

use futures::TryFutureExt;
use tokio::{select, sync::mpsc::UnboundedReceiver};

use super::{
    channel::{open_encrypted_channel, open_plaintext_channel, GRPCChannel},
    message::{Request, Response},
    stub::RPCStub,
};
use crate::{
    common::{Address, Result},
    connection::OneShotSender,
    Credential,
};

pub(in crate::connection) async fn send_request<Channel: GRPCChannel>(
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
        Request::DatabasesAll => rpc.databases_all(request.try_into()?).await.map(Response::from),

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
            rpc.session_close(request.try_into()?).map_ok(Response::from).await
        }

        Request::Transaction(transaction_request) => {
            rpc.transaction(transaction_request.into()).await.map(Response::from)
        }
    }
}

pub(in crate::connection) async fn start_grpc_worker_plaintext(
    address: Address,
    request_source: UnboundedReceiver<(Request, OneShotSender<Response>)>,
    shutdown_signal: UnboundedReceiver<()>,
) {
    let channel = open_plaintext_channel(address.clone());
    let rpc = RPCStub::new(address.clone(), channel, None).await.unwrap();
    grpc_worker(rpc, request_source, shutdown_signal).await;
}

pub(in crate::connection) async fn start_grpc_worker_encrypted(
    address: Address,
    credential: Credential,
    request_source: UnboundedReceiver<(Request, OneShotSender<Response>)>,
    shutdown_signal: UnboundedReceiver<()>,
) {
    let (channel, callcreds) = open_encrypted_channel(address.clone(), credential).unwrap();
    let rpc = RPCStub::new(address.clone(), channel, Some(callcreds)).await.unwrap();
    grpc_worker(rpc, request_source, shutdown_signal).await;
}

async fn grpc_worker<Channel: GRPCChannel>(
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
            let response = send_request(rpc, request).await;
            response_sink.send(response).ok();
        });
    }
}
