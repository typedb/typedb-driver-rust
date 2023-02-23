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
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use itertools::Itertools;
use tokio::{
    select,
    sync::mpsc::{unbounded_channel as unbounded_async, UnboundedReceiver, UnboundedSender},
    time::{sleep_until, Instant},
};

use super::{
    network::{
        channel::open_encrypted_channel,
        message::{Request, Response, TransactionRequest},
        stub::RPCStub,
        transmitter::RPCTransmitter,
    },
    TransactionStream,
};
use crate::{
    common::{
        address::Address,
        error::{ConnectionError, Error},
        info::{DatabaseInfo, SessionInfo},
        Result, SessionID, SessionType, TransactionType,
    },
    connection::{network::transmitter::TransactionTransmitter, runtime::BackgroundRuntime},
    error::InternalError,
    Credential, Options,
};

#[derive(Clone)]
pub struct Connection {
    server_connections: HashMap<Address, ServerConnection>,
    background_runtime: Arc<BackgroundRuntime>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

impl Connection {
    pub fn new_plaintext(address: impl AsRef<str>) -> Result<Self> {
        let address: Address = address.as_ref().parse()?;
        let background_runtime = Arc::new(BackgroundRuntime::new()?);
        let server_connection = ServerConnection::new_plaintext(background_runtime.clone(), address.clone())?;
        Ok(Self { server_connections: [(address, server_connection)].into(), background_runtime })
    }

    pub fn new_encrypted<T: AsRef<str> + Sync>(init_addresses: &[T], credential: Credential) -> Result<Self> {
        let background_runtime = Arc::new(BackgroundRuntime::new()?);

        let init_addresses = init_addresses.iter().map(|addr| addr.as_ref().parse()).try_collect()?;
        let addresses =
            background_runtime.block_on(Self::fetch_current_addresses(init_addresses, credential.clone()))?;

        let mut server_connections = HashMap::with_capacity(addresses.len());
        for address in addresses {
            let server_connection =
                ServerConnection::new_encrypted(background_runtime.clone(), address.clone(), credential.clone())?;
            server_connections.insert(address, server_connection);
        }

        Ok(Self { server_connections, background_runtime })
    }

    async fn fetch_current_addresses(addresses: Vec<Address>, credential: Credential) -> Result<HashSet<Address>> {
        for address in addresses {
            let (channel, call_credentials) = open_encrypted_channel(address.clone(), credential.clone())?;
            match RPCStub::new(address, channel, Some(call_credentials)).await?.validated().await {
                Ok(mut client) => {
                    return match client.servers_all(Request::ServersAll.try_into()?).await?.try_into()? {
                        Response::ServersAll { servers } => Ok(servers.into_iter().collect()),
                        other => Err(InternalError::UnexpectedResponseType(format!("{:?}", other)).into()),
                    };
                }
                Err(Error::Connection(ConnectionError::UnableToConnect())) => (),
                Err(err) => Err(err)?,
            }
        }
        Err(ConnectionError::UnableToConnect())?
    }

    pub fn force_close(self) -> Result {
        self.server_connections.values().map(ServerConnection::force_close).try_collect()?;
        self.background_runtime.force_close()
    }

    pub(crate) fn server_count(&self) -> usize {
        self.server_connections.len()
    }

    pub(crate) fn iter_addresses(&self) -> impl Iterator<Item = &Address> {
        self.server_connections.keys()
    }

    pub(crate) fn get_server_connection(&self, address: &Address) -> Result<ServerConnection> {
        self.server_connections
            .get(address)
            .cloned()
            .ok_or_else(|| InternalError::UnknownConnectionAddress(address.to_string()).into())
    }

    pub(crate) fn iter_server_connections_cloned(&self) -> impl Iterator<Item = ServerConnection> + '_ {
        self.server_connections.values().cloned()
    }

    pub(crate) fn unable_to_connect_error(&self) -> Error {
        Error::Connection(ConnectionError::ClusterUnableToConnect(
            self.iter_addresses().map(Address::to_string).collect::<Vec<_>>().join(","),
        ))
    }
}

#[derive(Clone)]
pub(crate) struct ServerConnection {
    address: Address,
    background_runtime: Arc<BackgroundRuntime>,
    open_sessions: Arc<Mutex<HashMap<SessionID, UnboundedSender<()>>>>,
    request_transmitter: Arc<RPCTransmitter>,
}

impl fmt::Debug for ServerConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerConnection").field("address", &self.address).finish()
    }
}

impl ServerConnection {
    fn new_plaintext(background_runtime: Arc<BackgroundRuntime>, address: Address) -> Result<Self> {
        let request_transmitter = Arc::new(RPCTransmitter::start_plaintext(address.clone(), &background_runtime)?);
        Ok(Self { address, background_runtime, open_sessions: Default::default(), request_transmitter })
    }

    fn new_encrypted(
        background_runtime: Arc<BackgroundRuntime>,
        address: Address,
        credential: Credential,
    ) -> Result<Self> {
        let request_transmitter =
            Arc::new(RPCTransmitter::start_encrypted(address.clone(), credential, &background_runtime)?);
        Ok(Self { address, background_runtime, open_sessions: Default::default(), request_transmitter })
    }

    pub(crate) fn address(&self) -> &Address {
        &self.address
    }

    async fn request_async(&self, request: Request) -> Result<Response> {
        if !self.background_runtime.is_open() {
            return Err(ConnectionError::ConnectionIsClosed().into());
        }
        self.request_transmitter.request_async(request).await
    }

    fn request_blocking(&self, request: Request) -> Result<Response> {
        if !self.background_runtime.is_open() {
            return Err(ConnectionError::ConnectionIsClosed().into());
        }
        self.request_transmitter.request_blocking(request)
    }

    pub(crate) fn force_close(&self) -> Result {
        let session_ids: Vec<SessionID> = self.open_sessions.lock().unwrap().keys().cloned().collect();
        for session_id in session_ids.into_iter() {
            self.close_session(session_id).ok();
        }
        self.request_transmitter.force_close()
    }

    pub(crate) async fn database_exists(&self, database_name: String) -> Result<bool> {
        match self.request_async(Request::DatabasesContains { database_name }).await? {
            Response::DatabasesContains { contains } => Ok(contains),
            other => Err(InternalError::UnexpectedResponseType(format!("{:?}", other)).into()),
        }
    }

    pub(crate) async fn create_database(&self, database_name: String) -> Result {
        self.request_async(Request::DatabaseCreate { database_name }).await?;
        Ok(())
    }

    pub(crate) async fn get_database_replicas(&self, database_name: String) -> Result<DatabaseInfo> {
        match self.request_async(Request::DatabaseGet { database_name }).await? {
            Response::DatabaseGet { database } => Ok(database),
            other => Err(InternalError::UnexpectedResponseType(format!("{:?}", other)).into()),
        }
    }

    pub(crate) async fn all_databases(&self) -> Result<Vec<DatabaseInfo>> {
        match self.request_async(Request::DatabasesAll).await? {
            Response::DatabasesAll { databases } => Ok(databases),
            other => Err(InternalError::UnexpectedResponseType(format!("{:?}", other)).into()),
        }
    }

    pub(crate) async fn database_schema(&self, database_name: String) -> Result<String> {
        match self.request_async(Request::DatabaseSchema { database_name }).await? {
            Response::DatabaseSchema { schema } => Ok(schema),
            other => Err(InternalError::UnexpectedResponseType(format!("{:?}", other)).into()),
        }
    }

    pub(crate) async fn database_type_schema(&self, database_name: String) -> Result<String> {
        match self.request_async(Request::DatabaseTypeSchema { database_name }).await? {
            Response::DatabaseTypeSchema { schema } => Ok(schema),
            other => Err(InternalError::UnexpectedResponseType(format!("{:?}", other)).into()),
        }
    }

    pub(crate) async fn database_rule_schema(&self, database_name: String) -> Result<String> {
        match self.request_async(Request::DatabaseRuleSchema { database_name }).await? {
            Response::DatabaseRuleSchema { schema } => Ok(schema),
            other => Err(InternalError::UnexpectedResponseType(format!("{:?}", other)).into()),
        }
    }

    pub(crate) async fn delete_database(&self, database_name: String) -> Result {
        self.request_async(Request::DatabaseDelete { database_name }).await?;
        Ok(())
    }

    pub(crate) async fn open_session(
        &self,
        database_name: String,
        session_type: SessionType,
        options: Options,
    ) -> Result<SessionInfo> {
        let start = Instant::now();
        match self.request_async(Request::SessionOpen { database_name, session_type, options }).await? {
            Response::SessionOpen { session_id, server_duration } => {
                let (shutdown_sink, shutdown_source) = unbounded_async();
                self.open_sessions.lock().unwrap().insert(session_id.clone(), shutdown_sink);
                self.background_runtime.spawn(session_pulse(
                    session_id.clone(),
                    self.request_transmitter.clone(),
                    shutdown_source,
                ));
                Ok(SessionInfo {
                    address: self.address.clone(),
                    session_id,
                    network_latency: start.elapsed() - server_duration,
                })
            }
            other => Err(InternalError::UnexpectedResponseType(format!("{:?}", other)).into()),
        }
    }

    pub(crate) fn close_session(&self, session_id: SessionID) -> Result {
        if let Some(sink) = self.open_sessions.lock().unwrap().remove(&session_id) {
            sink.send(()).ok();
        }
        self.request_blocking(Request::SessionClose { session_id })?;
        Ok(())
    }

    pub(crate) async fn open_transaction(
        &self,
        session_id: SessionID,
        transaction_type: TransactionType,
        options: Options,
        network_latency: Duration,
    ) -> Result<TransactionStream> {
        let response = self
            .request_async(Request::Transaction(TransactionRequest::Open {
                session_id,
                transaction_type,
                options: options.clone(),
                network_latency,
            }))
            .await?;
        let transmitter = TransactionTransmitter::new(&self.background_runtime, response);
        Ok(TransactionStream::new(transaction_type, options, transmitter))
    }
}

async fn session_pulse(
    session_id: SessionID,
    request_transmitter: Arc<RPCTransmitter>,
    mut shutdown_source: UnboundedReceiver<()>,
) {
    const PULSE_INTERVAL: Duration = Duration::from_secs(5);
    let mut next_pulse = Instant::now();
    loop {
        select! {
            _ = sleep_until(next_pulse) => {
                request_transmitter
                    .request_async(Request::SessionPulse { session_id: session_id.clone() })
                    .await
                    .ok();
                next_pulse += PULSE_INTERVAL;
            }
            _ = shutdown_source.recv() => break,
        }
    }
}
