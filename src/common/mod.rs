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

mod address;
mod blocking_dispatcher;
pub mod credential;
mod drop_guard;
pub mod error;
mod macros;
pub(crate) mod rpc;

use std::{fmt, time::Duration};

use tonic::{Response, Status};
use typedb_protocol::{session as session_proto, transaction as transaction_proto};

pub use self::{address::Address, credential::Credential, error::Error};
pub(crate) use self::{
    blocking_dispatcher::{BlockingDispatcher, DispatcherThreadHandle},
    drop_guard::DropGuard,
    rpc::{ClusterRPC, ClusterServerRPC, CoreRPC, ServerRPC, TransactionRPC},
};

pub(crate) const POLL_INTERVAL: Duration = Duration::from_millis(3);

pub(crate) type StdResult<T, E> = std::result::Result<T, E>;
pub type Result<T = ()> = StdResult<T, Error>;
pub(crate) type TonicResult<R> = StdResult<Response<R>, Status>;

pub(crate) type TonicChannel = tonic::transport::Channel;

pub(crate) type RequestID = ID;
pub(crate) type SessionID = ID;

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ID(Vec<u8>);

impl From<ID> for Vec<u8> {
    fn from(id: ID) -> Self {
        id.0
    }
}

impl From<Vec<u8>> for ID {
    fn from(vec: Vec<u8>) -> Self {
        Self(vec)
    }
}

impl fmt::Debug for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ID[{}]", self)
    }
}

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|b| write!(f, "{:02x}", b))
    }
}

#[derive(Copy, Clone, Debug)]
pub enum SessionType {
    Data = 0,
    Schema = 1,
}

impl SessionType {
    pub(crate) fn to_proto(self) -> session_proto::Type {
        match self {
            SessionType::Data => session_proto::Type::Data,
            SessionType::Schema => session_proto::Type::Schema,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum TransactionType {
    Read = 0,
    Write = 1,
}

impl TransactionType {
    pub(crate) fn to_proto(self) -> transaction_proto::Type {
        match self {
            TransactionType::Read => transaction_proto::Type::Read,
            TransactionType::Write => transaction_proto::Type::Write,
        }
    }
}
