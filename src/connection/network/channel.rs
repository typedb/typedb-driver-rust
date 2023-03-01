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

use std::sync::{Arc, RwLock};

use tonic::{
    body::BoxBody,
    client::GrpcService,
    service::{
        interceptor::{self, InterceptedService},
        Interceptor,
    },
    transport::{channel, Channel, Error as TonicError},
    Request, Status,
};

use crate::{
    common::{address::Address, Result, StdResult},
    Credential,
};

type ResponseFuture = interceptor::ResponseFuture<channel::ResponseFuture>;

pub(in crate::connection) type PlainTextChannel = InterceptedService<Channel, PlainTextFacade>;
pub(in crate::connection) type CallCredChannel = InterceptedService<Channel, CredentialInjector>;

pub(in crate::connection) trait GRPCChannel:
    GrpcService<BoxBody, Error = TonicError, ResponseBody = BoxBody, Future = ResponseFuture> + Clone + Send + 'static
{
    fn is_plaintext(&self) -> bool;
}

impl GRPCChannel for PlainTextChannel {
    fn is_plaintext(&self) -> bool {
        true
    }
}

impl GRPCChannel for CallCredChannel {
    fn is_plaintext(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug)]
pub(in crate::connection) struct PlainTextFacade;

impl Interceptor for PlainTextFacade {
    fn call(&mut self, request: Request<()>) -> StdResult<Request<()>, Status> {
        Ok(request)
    }
}

pub(in crate::connection) fn open_plaintext_channel(address: Address) -> PlainTextChannel {
    PlainTextChannel::new(Channel::builder(address.into_uri()).connect_lazy(), PlainTextFacade)
}

#[derive(Clone, Debug)]
pub(in crate::connection) struct CredentialInjector {
    call_credentials: Arc<CallCredentials>,
}

impl CredentialInjector {
    pub(in crate::connection) fn new(call_credentials: Arc<CallCredentials>) -> Self {
        Self { call_credentials }
    }
}

impl Interceptor for CredentialInjector {
    fn call(&mut self, request: Request<()>) -> StdResult<Request<()>, Status> {
        Ok(self.call_credentials.inject(request))
    }
}

pub(in crate::connection) fn open_encrypted_channel(
    address: Address,
    credential: Credential,
) -> Result<(CallCredChannel, Arc<CallCredentials>)> {
    let mut builder = Channel::builder(address.into_uri());
    if credential.is_tls_enabled() {
        builder = builder.tls_config(credential.tls_config()?)?;
    }
    let channel = builder.connect_lazy();
    let call_credentials = Arc::new(CallCredentials::new(credential));
    Ok((CallCredChannel::new(channel, CredentialInjector::new(call_credentials.clone())), call_credentials))
}

#[derive(Debug)]
pub(in crate::connection) struct CallCredentials {
    credential: Credential,
    token: RwLock<Option<String>>,
}

impl CallCredentials {
    pub(in crate::connection) fn new(credential: Credential) -> Self {
        Self { credential, token: RwLock::new(None) }
    }

    pub(in crate::connection) fn username(&self) -> &str {
        self.credential.username()
    }

    pub(in crate::connection) fn set_token(&self, token: String) {
        *self.token.write().unwrap() = Some(token);
    }

    pub(in crate::connection) fn reset_token(&self) {
        *self.token.write().unwrap() = None;
    }

    pub(in crate::connection) fn inject(&self, mut request: Request<()>) -> Request<()> {
        request.metadata_mut().insert("username", self.credential.username().try_into().unwrap());
        match &*self.token.read().unwrap() {
            Some(token) => request.metadata_mut().insert("token", token.try_into().unwrap()),
            None => request.metadata_mut().insert("password", self.credential.password().try_into().unwrap()),
        };
        request
    }
}