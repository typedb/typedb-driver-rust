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
mod credential;
pub mod error;
mod id;
mod options;

use typedb_protocol::{session as session_proto, transaction as transaction_proto};

pub(crate) use self::address::Address;
pub use self::{credential::Credential, error::Error, options::Options};

pub(crate) type StdResult<T, E> = std::result::Result<T, E>;
pub type Result<T = ()> = StdResult<T, Error>;

pub(crate) type RequestID = id::ID;
pub(crate) type SessionID = id::ID;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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
