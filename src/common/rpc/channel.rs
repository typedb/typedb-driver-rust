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

use std::sync::Arc;

use tonic::{
    self,
    body::BoxBody,
    client::GrpcService,
    service::{
        interceptor::{self, InterceptedService},
        Interceptor,
    },
    transport::{channel, Error as TonicError},
    Request, Status,
};

use crate::{
    common::{credential::CallCredentials, Address, Result, StdResult},
    Credential,
};

type TonicChannel = tonic::transport::Channel;
type ResponseFuture = interceptor::ResponseFuture<channel::ResponseFuture>;

pub(super) trait GRPCChannel:
    GrpcService<BoxBody, Error = TonicError, ResponseBody = BoxBody, Future = ResponseFuture>
    + Clone
    + Send
    + 'static
{
}

impl<T> GRPCChannel for T where
    T: GrpcService<BoxBody, Error = TonicError, ResponseBody = BoxBody, Future = ResponseFuture>
        + Clone
        + Send
        + 'static
{
}

#[derive(Clone, Debug)]
pub(crate) struct PlainTextFacade;

impl Interceptor for PlainTextFacade {
    fn call(&mut self, request: Request<()>) -> StdResult<Request<()>, Status> {
        Ok(request)
    }
}

pub(crate) type PlainTextChannel = InterceptedService<TonicChannel, PlainTextFacade>;

pub(crate) fn open_plaintext_channel(address: Address) -> PlainTextChannel {
    PlainTextChannel::new(TonicChannel::builder(address.into_uri()).connect_lazy(), PlainTextFacade)
}
#[derive(Clone, Debug)]
pub(crate) struct CredentialInjector {
    call_credentials: Arc<CallCredentials>,
}

impl CredentialInjector {
    pub(super) fn new(call_credentials: Arc<CallCredentials>) -> Self {
        Self { call_credentials }
    }
}

impl Interceptor for CredentialInjector {
    fn call(&mut self, request: Request<()>) -> StdResult<Request<()>, Status> {
        Ok(self.call_credentials.inject(request))
    }
}

pub(crate) type CallCredChannel = InterceptedService<TonicChannel, CredentialInjector>;

pub(crate) fn open_encrypted_channel(
    address: Address,
    credential: Credential,
) -> Result<(CallCredChannel, Arc<CallCredentials>)> {
    let mut builder = TonicChannel::builder(address.into_uri());
    if credential.is_tls_enabled() {
        builder = builder.tls_config(credential.tls_config()?)?;
    }
    let channel = builder.connect_lazy();
    let call_credentials = Arc::new(CallCredentials::new(credential));
    Ok((
        CallCredChannel::new(channel, CredentialInjector::new(call_credentials.clone())),
        call_credentials,
    ))
}
