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
    path::{Path, PathBuf},
};

use futures::future::join_all;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

use crate::{common::error::MESSAGES, rpc::client::RpcClusterClient, Result};

#[derive(Clone, Debug)]
pub struct Credential {
    username: String,
    password: String,
    is_tls_enabled: bool,
    tls_root_ca: Option<PathBuf>,
}

impl Credential {
    pub fn new_with_tls(username: &str, password: &str, tls_root_ca: Option<&Path>) -> Self {
        Credential {
            username: username.to_owned(),
            password: password.to_owned(),
            is_tls_enabled: true,
            tls_root_ca: tls_root_ca.map(Path::to_owned),
        }
    }

    pub fn new_without_tls(username: &str, password: &str) -> Self {
        Credential {
            username: username.to_owned(),
            password: password.to_owned(),
            is_tls_enabled: false,
            tls_root_ca: None,
        }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn is_tls_enabled(&self) -> bool {
        self.is_tls_enabled
    }

    pub fn tls_config(&self) -> Result<ClientTlsConfig> {
        if self.tls_root_ca.is_some() {
            Ok(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(
                std::fs::read_to_string(self.tls_root_ca.as_ref().unwrap())?,
            )))
        } else {
            Ok(ClientTlsConfig::new())
        }
    }
}

pub struct Client {
    clients: HashMap<String, ServerClient>,
    parallelisation: u64,
}

impl Client {
    pub async fn new(
        init_addresses: &HashSet<String>,
        credential: Credential,
        parallelisation: u64,
    ) -> Result<Self> {
        Ok(Self {
            clients: create_clients(init_addresses, credential, parallelisation).await?,
            parallelisation,
        })
    }
}

async fn fetch_current_addresses(
    addresses: &HashSet<String>,
    credential: &Credential,
    parallelisation: u64,
) -> Result<HashSet<String>> {
    for address in addresses {
        match ServerClient::new(address, credential.clone(), parallelisation).await {
            Ok(mut client) => return client.servers().await,
            Err(err) if err == MESSAGES.client.unable_to_connect.to_err(vec![]) => (),
            Err(err) => return Err(err),
        }
    }
    Err(MESSAGES.client.unable_to_connect.to_err(vec![]))
}

async fn create_clients(
    init_addresses: &HashSet<String>,
    credential: Credential,
    parallelisation: u64,
) -> Result<HashMap<String, ServerClient>> {
    let addresses = fetch_current_addresses(init_addresses, &credential, parallelisation).await?;
    let mut clients = join_all(
        addresses.iter().map(|addr| ServerClient::new(&addr, credential.clone(), parallelisation)),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?;

    if join_all(clients.iter_mut().map(ServerClient::validate)).await.iter().all(Result::is_err) {
        Err(MESSAGES.client.unable_to_connect.to_err(vec![]))?;
    }

    Ok(addresses.into_iter().zip(clients.into_iter()).collect())
}

pub struct ServerClient {
    // client: TypeDBClient,
    address: String,
    stub: ServerStub,
}

impl ServerClient {
    async fn new(address: &str, credential: Credential, _parallelisation: u64) -> Result<Self> {
        let uri = if address.contains("://") {
            address.parse().unwrap()
        } else {
            format!("http://{}", address).parse().unwrap()
        };

        let channel = if credential.is_tls_enabled() {
            Channel::builder(uri).tls_config(credential.tls_config()?)?.connect().await?
        } else {
            Channel::builder(uri).connect().await.expect("")
        };
        Ok(Self {
            // client: TypeDBClient::new(address).await?,
            address: address.to_owned(),
            stub: ServerStub::new(channel, credential).await?,
        })
    }

    async fn servers(&mut self) -> Result<HashSet<String>> {
        self.stub.servers_all().await
    }

    async fn validate(&mut self) -> Result<()> {
        // self.client.validate().await
        Ok(())
    }
}

struct ServerStub {
    cluster_client: RpcClusterClient,
}

impl ServerStub {
    async fn new(channel: Channel, credential: Credential) -> Result<Self> {
        Ok(Self { cluster_client: RpcClusterClient::new(channel, credential).await? })
    }

    async fn servers_all(&mut self) -> Result<HashSet<String>> {
        self.cluster_client
            .servers_all()
            .await
            .map(|res| res.servers.into_iter().map(|server| server.address).collect())
    }
}
