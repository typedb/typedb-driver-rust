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

use std::time::Duration;

use crossbeam::channel::Sender;
use itertools::Itertools;
use tonic::Streaming;
use typedb_protocol::{
    cluster_database_manager, core_database, core_database_manager, server_manager, session, transaction,
};

use super::TransactionRequest;
use crate::{
    common::{address::Address, info::DatabaseInfo, SessionID},
    connection::network::proto::{IntoProto, TryFromProto},
    error::{ConnectionError, InternalError},
    Error, Options, Result, SessionType,
};

#[derive(Debug)]
pub(in crate::connection) enum Request {
    ServersAll,

    DatabasesContains { database_name: String },
    DatabaseCreate { database_name: String },
    DatabaseGet { database_name: String },
    DatabasesAll,

    DatabaseSchema { database_name: String },
    DatabaseTypeSchema { database_name: String },
    DatabaseRuleSchema { database_name: String },
    DatabaseDelete { database_name: String },

    SessionOpen { database_name: String, session_type: SessionType, options: Options },
    SessionClose { session_id: SessionID },
    SessionPulse { session_id: SessionID },

    Transaction(TransactionRequest),
}

impl TryFrom<Request> for server_manager::all::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::ServersAll => Ok(server_manager::all::Req {}),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for core_database_manager::contains::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::DatabasesContains { database_name } => {
                Ok(core_database_manager::contains::Req { name: database_name })
            }
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for core_database_manager::create::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::DatabaseCreate { database_name } => Ok(core_database_manager::create::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for cluster_database_manager::get::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::DatabaseGet { database_name } => Ok(cluster_database_manager::get::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for cluster_database_manager::all::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::DatabasesAll => Ok(cluster_database_manager::all::Req {}),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for core_database::delete::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::DatabaseDelete { database_name } => Ok(core_database::delete::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for core_database::schema::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::DatabaseSchema { database_name } => Ok(core_database::schema::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for core_database::type_schema::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::DatabaseTypeSchema { database_name } => {
                Ok(core_database::type_schema::Req { name: database_name })
            }
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for core_database::rule_schema::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::DatabaseRuleSchema { database_name } => {
                Ok(core_database::rule_schema::Req { name: database_name })
            }
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for session::open::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::SessionOpen { database_name, session_type, options } => Ok(session::open::Req {
                database: database_name,
                r#type: session_type.into_proto().into(),
                options: Some(options.into_proto()),
            }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for session::pulse::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::SessionPulse { session_id } => Ok(session::pulse::Req { session_id: session_id.into() }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for session::close::Req {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::SessionClose { session_id } => Ok(session::close::Req { session_id: session_id.into() }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFrom<Request> for transaction::Client {
    type Error = Error;
    fn try_from(request: Request) -> Result<Self> {
        match request {
            Request::Transaction(transaction_req) => Ok(transaction::Client { reqs: vec![transaction_req.into()] }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

#[derive(Debug)]
pub(in crate::connection) enum Response {
    ServersAll { servers: Vec<Address> },

    DatabasesContains { contains: bool },
    DatabaseCreate,
    DatabaseGet { database: DatabaseInfo },
    DatabasesAll { databases: Vec<DatabaseInfo> },

    DatabaseDelete,
    DatabaseSchema { schema: String },
    DatabaseTypeSchema { schema: String },
    DatabaseRuleSchema { schema: String },

    SessionOpen { session_id: SessionID, server_duration: Duration },
    SessionPulse,
    SessionClose,

    TransactionOpen { request_sink: Sender<transaction::Client>, grpc_stream: Streaming<transaction::Server> },
}

impl TryFrom<server_manager::all::Res> for Response {
    type Error = Error;
    fn try_from(server_manager::all::Res { servers }: server_manager::all::Res) -> Result<Self> {
        let servers = servers.into_iter().map(|server| server.address.parse()).try_collect()?;
        Ok(Response::ServersAll { servers })
    }
}

impl From<core_database_manager::contains::Res> for Response {
    fn from(res: core_database_manager::contains::Res) -> Self {
        Self::DatabasesContains { contains: res.contains }
    }
}

impl From<core_database_manager::create::Res> for Response {
    fn from(_res: core_database_manager::create::Res) -> Self {
        Self::DatabaseCreate
    }
}

impl TryFrom<cluster_database_manager::get::Res> for Response {
    type Error = Error;
    fn try_from(res: cluster_database_manager::get::Res) -> Result<Self> {
        Ok(Response::DatabaseGet {
            database: DatabaseInfo::try_from_proto(
                res.database.ok_or(ConnectionError::MissingResponseField("database"))?,
            )?,
        })
    }
}

impl TryFrom<cluster_database_manager::all::Res> for Response {
    type Error = Error;
    fn try_from(res: cluster_database_manager::all::Res) -> Result<Self> {
        Ok(Response::DatabasesAll {
            databases: res.databases.into_iter().map(DatabaseInfo::try_from_proto).try_collect()?,
        })
    }
}

impl From<core_database::delete::Res> for Response {
    fn from(_res: core_database::delete::Res) -> Self {
        Self::DatabaseDelete
    }
}

impl From<core_database::schema::Res> for Response {
    fn from(res: core_database::schema::Res) -> Self {
        Self::DatabaseSchema { schema: res.schema }
    }
}

impl From<core_database::type_schema::Res> for Response {
    fn from(res: core_database::type_schema::Res) -> Self {
        Self::DatabaseTypeSchema { schema: res.schema }
    }
}

impl From<core_database::rule_schema::Res> for Response {
    fn from(res: core_database::rule_schema::Res) -> Self {
        Self::DatabaseRuleSchema { schema: res.schema }
    }
}

impl From<session::open::Res> for Response {
    fn from(res: session::open::Res) -> Self {
        Self::SessionOpen {
            session_id: res.session_id.into(),
            server_duration: Duration::from_millis(res.server_duration_millis as u64),
        }
    }
}

impl From<session::pulse::Res> for Response {
    fn from(_res: session::pulse::Res) -> Self {
        Self::SessionPulse
    }
}

impl From<session::close::Res> for Response {
    fn from(_res: session::close::Res) -> Self {
        Self::SessionClose
    }
}

impl From<(Sender<transaction::Client>, Streaming<transaction::Server>)> for Response {
    fn from((request_sink, grpc_stream): (Sender<transaction::Client>, Streaming<transaction::Server>)) -> Self {
        Self::TransactionOpen { request_sink, grpc_stream }
    }
}
