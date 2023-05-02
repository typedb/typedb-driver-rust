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

use itertools::Itertools;
use typedb_protocol::{
    concept_manager, database, database_manager, query_manager, r#type, server_manager, session, thing_type,
    transaction,
};

use super::{FromProto, IntoProto, TryFromProto};
use crate::{
    answer::{ConceptMap, Numeric},
    common::{info::DatabaseInfo, RequestID, Result},
    concept::EntityType,
    connection::{
        message::{
            ConceptRequest, ConceptResponse, QueryRequest, QueryResponse, Request, Response, ThingTypeRequest,
            ThingTypeResponse, TransactionRequest, TransactionResponse,
        },
        network::proto::TryIntoProto,
    },
    error::{ConnectionError, InternalError},
};

impl TryIntoProto<server_manager::all::Req> for Request {
    fn try_into_proto(self) -> Result<server_manager::all::Req> {
        match self {
            Request::ServersAll => Ok(server_manager::all::Req {}),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database_manager::contains::Req> for Request {
    fn try_into_proto(self) -> Result<database_manager::contains::Req> {
        match self {
            Request::DatabasesContains { database_name } => Ok(database_manager::contains::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database_manager::create::Req> for Request {
    fn try_into_proto(self) -> Result<database_manager::create::Req> {
        match self {
            Request::DatabaseCreate { database_name } => Ok(database_manager::create::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database_manager::get::Req> for Request {
    fn try_into_proto(self) -> Result<database_manager::get::Req> {
        match self {
            Request::DatabaseGet { database_name } => Ok(database_manager::get::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database_manager::all::Req> for Request {
    fn try_into_proto(self) -> Result<database_manager::all::Req> {
        match self {
            Request::DatabasesAll => Ok(database_manager::all::Req {}),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database::delete::Req> for Request {
    fn try_into_proto(self) -> Result<database::delete::Req> {
        match self {
            Request::DatabaseDelete { database_name } => Ok(database::delete::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database::schema::Req> for Request {
    fn try_into_proto(self) -> Result<database::schema::Req> {
        match self {
            Request::DatabaseSchema { database_name } => Ok(database::schema::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database::type_schema::Req> for Request {
    fn try_into_proto(self) -> Result<database::type_schema::Req> {
        match self {
            Request::DatabaseTypeSchema { database_name } => Ok(database::type_schema::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database::rule_schema::Req> for Request {
    fn try_into_proto(self) -> Result<database::rule_schema::Req> {
        match self {
            Request::DatabaseRuleSchema { database_name } => Ok(database::rule_schema::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<session::open::Req> for Request {
    fn try_into_proto(self) -> Result<session::open::Req> {
        match self {
            Request::SessionOpen { database_name, session_type, options } => Ok(session::open::Req {
                database: database_name,
                r#type: session_type.into_proto().into(),
                options: Some(options.into_proto()),
            }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<session::pulse::Req> for Request {
    fn try_into_proto(self) -> Result<session::pulse::Req> {
        match self {
            Request::SessionPulse { session_id } => Ok(session::pulse::Req { session_id: session_id.into() }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<session::close::Req> for Request {
    fn try_into_proto(self) -> Result<session::close::Req> {
        match self {
            Request::SessionClose { session_id } => Ok(session::close::Req { session_id: session_id.into() }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<transaction::Client> for Request {
    fn try_into_proto(self) -> Result<transaction::Client> {
        match self {
            Request::Transaction(transaction_req) => {
                Ok(transaction::Client { reqs: vec![transaction_req.into_proto()] })
            }
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFromProto<server_manager::all::Res> for Response {
    fn try_from_proto(proto: server_manager::all::Res) -> Result<Self> {
        let servers = proto.servers.into_iter().map(|server| server.address.parse()).try_collect()?;
        Ok(Response::ServersAll { servers })
    }
}

impl FromProto<database_manager::contains::Res> for Response {
    fn from_proto(proto: database_manager::contains::Res) -> Self {
        Self::DatabasesContains { contains: proto.contains }
    }
}

impl FromProto<database_manager::create::Res> for Response {
    fn from_proto(_proto: database_manager::create::Res) -> Self {
        Self::DatabaseCreate
    }
}

impl TryFromProto<database_manager::get::Res> for Response {
    fn try_from_proto(proto: database_manager::get::Res) -> Result<Self> {
        Ok(Response::DatabaseGet {
            database: DatabaseInfo::try_from_proto(
                proto.database.ok_or(ConnectionError::MissingResponseField("database"))?,
            )?,
        })
    }
}

impl TryFromProto<database_manager::all::Res> for Response {
    fn try_from_proto(proto: database_manager::all::Res) -> Result<Self> {
        Ok(Response::DatabasesAll {
            databases: proto.databases.into_iter().map(DatabaseInfo::try_from_proto).try_collect()?,
        })
    }
}

impl FromProto<database::delete::Res> for Response {
    fn from_proto(_proto: database::delete::Res) -> Self {
        Self::DatabaseDelete
    }
}

impl FromProto<database::schema::Res> for Response {
    fn from_proto(proto: database::schema::Res) -> Self {
        Self::DatabaseSchema { schema: proto.schema }
    }
}

impl FromProto<database::type_schema::Res> for Response {
    fn from_proto(proto: database::type_schema::Res) -> Self {
        Self::DatabaseTypeSchema { schema: proto.schema }
    }
}

impl FromProto<database::rule_schema::Res> for Response {
    fn from_proto(proto: database::rule_schema::Res) -> Self {
        Self::DatabaseRuleSchema { schema: proto.schema }
    }
}

impl FromProto<session::open::Res> for Response {
    fn from_proto(proto: session::open::Res) -> Self {
        Self::SessionOpen {
            session_id: proto.session_id.into(),
            server_duration: Duration::from_millis(proto.server_duration_millis as u64),
        }
    }
}

impl FromProto<session::pulse::Res> for Response {
    fn from_proto(_proto: session::pulse::Res) -> Self {
        Self::SessionPulse
    }
}

impl FromProto<session::close::Res> for Response {
    fn from_proto(_proto: session::close::Res) -> Self {
        Self::SessionClose
    }
}

impl IntoProto<transaction::Req> for TransactionRequest {
    fn into_proto(self) -> transaction::Req {
        let mut request_id = None;

        let req = match self {
            TransactionRequest::Open { session_id, transaction_type, options, network_latency } => {
                transaction::req::Req::OpenReq(transaction::open::Req {
                    session_id: session_id.into(),
                    r#type: transaction_type.into_proto().into(),
                    options: Some(options.into_proto()),
                    network_latency_millis: network_latency.as_millis() as i32,
                })
            }
            TransactionRequest::Commit => transaction::req::Req::CommitReq(transaction::commit::Req {}),
            TransactionRequest::Rollback => transaction::req::Req::RollbackReq(transaction::rollback::Req {}),
            TransactionRequest::Query(query_request) => {
                transaction::req::Req::QueryManagerReq(query_request.into_proto())
            }
            TransactionRequest::Concept(concept_request) => {
                transaction::req::Req::ConceptManagerReq(concept_request.into_proto())
            }
            TransactionRequest::ThingType(thing_type_request) => {
                transaction::req::Req::TypeReq(thing_type_request.into_proto())
            }
            TransactionRequest::Stream { request_id: req_id } => {
                request_id = Some(req_id);
                transaction::req::Req::StreamReq(transaction::stream::Req {})
            }
        };

        transaction::Req {
            req_id: request_id.unwrap_or_else(RequestID::generate).into(),
            metadata: Default::default(),
            req: Some(req),
        }
    }
}

impl TryFromProto<transaction::Res> for TransactionResponse {
    fn try_from_proto(proto: transaction::Res) -> Result<Self> {
        match proto.res {
            Some(transaction::res::Res::OpenRes(_)) => Ok(TransactionResponse::Open),
            Some(transaction::res::Res::CommitRes(_)) => Ok(TransactionResponse::Commit),
            Some(transaction::res::Res::RollbackRes(_)) => Ok(TransactionResponse::Rollback),
            Some(transaction::res::Res::QueryManagerRes(res)) => {
                Ok(TransactionResponse::Query(QueryResponse::try_from_proto(res)?))
            }
            Some(transaction::res::Res::ConceptManagerRes(res)) => {
                Ok(TransactionResponse::Concept(ConceptResponse::try_from_proto(res)?))
            }
            Some(transaction::res::Res::TypeRes(r#type::Res { res: Some(r#type::res::Res::ThingTypeRes(res)) })) => {
                Ok(TransactionResponse::ThingType(ThingTypeResponse::try_from_proto(res)?))
            }
            Some(_) => todo!(),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}

impl TryFromProto<transaction::ResPart> for TransactionResponse {
    fn try_from_proto(proto: transaction::ResPart) -> Result<Self> {
        match proto.res {
            Some(transaction::res_part::Res::QueryManagerResPart(res_part)) => {
                Ok(TransactionResponse::Query(QueryResponse::try_from_proto(res_part)?))
            }
            Some(_) => todo!(),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}

impl IntoProto<query_manager::Req> for QueryRequest {
    fn into_proto(self) -> query_manager::Req {
        let (req, options) = match self {
            QueryRequest::Define { query, options } => {
                (query_manager::req::Req::DefineReq(query_manager::define::Req { query }), options)
            }
            QueryRequest::Undefine { query, options } => {
                (query_manager::req::Req::UndefineReq(query_manager::undefine::Req { query }), options)
            }
            QueryRequest::Delete { query, options } => {
                (query_manager::req::Req::DeleteReq(query_manager::delete::Req { query }), options)
            }

            QueryRequest::Match { query, options } => {
                (query_manager::req::Req::MatchReq(query_manager::r#match::Req { query }), options)
            }
            QueryRequest::Insert { query, options } => {
                (query_manager::req::Req::InsertReq(query_manager::insert::Req { query }), options)
            }
            QueryRequest::Update { query, options } => {
                (query_manager::req::Req::UpdateReq(query_manager::update::Req { query }), options)
            }

            QueryRequest::MatchAggregate { query, options } => {
                (query_manager::req::Req::MatchAggregateReq(query_manager::match_aggregate::Req { query }), options)
            }

            _ => todo!(),
        };
        query_manager::Req { req: Some(req), options: Some(options.into_proto()) }
    }
}

impl TryFromProto<query_manager::Res> for QueryResponse {
    fn try_from_proto(proto: query_manager::Res) -> Result<Self> {
        match proto.res {
            Some(query_manager::res::Res::DefineRes(_)) => Ok(QueryResponse::Define),
            Some(query_manager::res::Res::UndefineRes(_)) => Ok(QueryResponse::Undefine),
            Some(query_manager::res::Res::DeleteRes(_)) => Ok(QueryResponse::Delete),
            Some(query_manager::res::Res::MatchAggregateRes(res)) => Ok(QueryResponse::MatchAggregate {
                answer: Numeric::try_from_proto(res.answer.ok_or(ConnectionError::MissingResponseField("answer"))?)?,
            }),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}

impl TryFromProto<query_manager::ResPart> for QueryResponse {
    fn try_from_proto(proto: query_manager::ResPart) -> Result<Self> {
        match proto.res {
            Some(query_manager::res_part::Res::MatchResPart(res)) => Ok(QueryResponse::Match {
                answers: res.answers.into_iter().map(ConceptMap::try_from_proto).try_collect()?,
            }),
            Some(query_manager::res_part::Res::InsertResPart(res)) => Ok(QueryResponse::Insert {
                answers: res.answers.into_iter().map(ConceptMap::try_from_proto).try_collect()?,
            }),
            Some(query_manager::res_part::Res::UpdateResPart(res)) => Ok(QueryResponse::Update {
                answers: res.answers.into_iter().map(ConceptMap::try_from_proto).try_collect()?,
            }),
            Some(_) => todo!(),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}

impl IntoProto<concept_manager::Req> for ConceptRequest {
    fn into_proto(self) -> concept_manager::Req {
        let req = match self {
            Self::GetEntityType { label } => {
                concept_manager::req::Req::GetEntityTypeReq(concept_manager::get_entity_type::Req { label })
            }
        };
        concept_manager::Req { req: Some(req) }
    }
}

impl TryFromProto<concept_manager::Res> for ConceptResponse {
    fn try_from_proto(proto: concept_manager::Res) -> Result<Self> {
        match proto.res {
            Some(concept_manager::res::Res::GetEntityTypeRes(concept_manager::get_entity_type::Res {
                entity_type,
            })) => Ok(Self::GetEntityType { entity_type: entity_type.map(|proto| EntityType::from_proto(proto)) }),
            Some(_) => todo!(),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}

impl IntoProto<r#type::Req> for ThingTypeRequest {
    fn into_proto(self) -> r#type::Req {
        let (req, label) = match self {
            Self::Delete { label } => (thing_type::req::Req::ThingTypeDeleteReq(thing_type::delete::Req {}), label),
        };
        r#type::Req { req: Some(r#type::req::Req::ThingTypeReq(thing_type::Req { label, req: Some(req) })) }
    }
}

impl TryFromProto<thing_type::Res> for ThingTypeResponse {
    fn try_from_proto(proto: thing_type::Res) -> Result<Self> {
        match proto.res {
            Some(thing_type::res::Res::ThingTypeDeleteRes(_)) => Ok(Self::Delete),
            Some(_) => todo!(),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}
