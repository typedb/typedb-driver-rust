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
use tonic::Streaming;
use typedb_protocol::{
    cluster_database::Replica, cluster_database_manager, core_database, core_database_manager,
    query_manager, server_manager, session, transaction, ClusterDatabase,
};

use crate::{
    answer::{ConceptMap, Numeric},
    common::{error::ClientError, Address, RequestID, Result, SessionID, SessionType},
    connection::Options,
    TransactionType,
};

#[derive(Debug)]
pub(crate) struct DatabaseProto {
    pub name: String,
    pub replicas: Vec<ReplicaProto>,
}

impl From<ClusterDatabase> for DatabaseProto {
    fn from(proto: ClusterDatabase) -> Self {
        Self { name: proto.name, replicas: proto.replicas.into_iter().map(Into::into).collect() }
    }
}

#[derive(Debug)]
pub(crate) struct ReplicaProto {
    pub address: Address,
    pub is_primary: bool,
    pub is_preferred: bool,
    pub term: i64,
}

impl From<Replica> for ReplicaProto {
    fn from(proto: Replica) -> Self {
        Self {
            address: proto.address.as_str().parse().unwrap(),
            is_primary: proto.primary,
            is_preferred: proto.preferred,
            term: proto.term,
        }
    }
}

#[derive(Debug)]
pub(super) enum Request {
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

impl From<Request> for server_manager::all::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::ServersAll => server_manager::all::Req {},
            _ => unreachable!(),
        }
    }
}

impl From<Request> for core_database_manager::contains::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::DatabasesContains { database_name } => {
                core_database_manager::contains::Req { name: database_name }
            }
            _ => unreachable!(),
        }
    }
}

impl From<Request> for core_database_manager::create::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::DatabaseCreate { database_name } => {
                core_database_manager::create::Req { name: database_name }
            }
            _ => unreachable!(),
        }
    }
}

impl From<Request> for cluster_database_manager::get::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::DatabaseGet { database_name } => {
                cluster_database_manager::get::Req { name: database_name }
            }
            _ => unreachable!(),
        }
    }
}

impl From<Request> for cluster_database_manager::all::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::DatabasesAll => cluster_database_manager::all::Req {},
            _ => unreachable!(),
        }
    }
}

impl From<Request> for core_database::delete::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::DatabaseDelete { database_name } => {
                core_database::delete::Req { name: database_name }
            }
            _ => unreachable!(),
        }
    }
}

impl From<Request> for core_database::schema::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::DatabaseSchema { database_name } => {
                core_database::schema::Req { name: database_name }
            }
            _ => unreachable!(),
        }
    }
}

impl From<Request> for core_database::type_schema::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::DatabaseTypeSchema { database_name } => {
                core_database::type_schema::Req { name: database_name }
            }
            _ => unreachable!(),
        }
    }
}

impl From<Request> for core_database::rule_schema::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::DatabaseRuleSchema { database_name } => {
                core_database::rule_schema::Req { name: database_name }
            }
            _ => unreachable!(),
        }
    }
}

impl From<Request> for session::open::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::SessionOpen { database_name, session_type, options } => session::open::Req {
                database: database_name,
                r#type: session_type.to_proto().into(),
                options: Some(options.to_proto()),
            },
            _ => unreachable!(),
        }
    }
}

impl From<Request> for session::pulse::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::SessionPulse { session_id } => {
                session::pulse::Req { session_id: session_id.into() }
            }
            _ => unreachable!(),
        }
    }
}

impl From<Request> for session::close::Req {
    fn from(request: Request) -> Self {
        match request {
            Request::SessionClose { session_id } => {
                session::close::Req { session_id: session_id.into() }
            }
            _ => unreachable!(),
        }
    }
}

impl From<Request> for transaction::Client {
    fn from(request: Request) -> Self {
        match request {
            Request::Transaction(transaction_req) => {
                transaction::Client { reqs: vec![transaction_req.into()] }
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub(super) enum Response {
    ServersAll {
        servers: Vec<Address>,
    },

    DatabasesContains {
        contains: bool,
    },
    DatabaseCreate,
    DatabaseGet {
        database: DatabaseProto,
    },
    DatabasesAll {
        databases: Vec<DatabaseProto>,
    },

    DatabaseDelete,
    DatabaseSchema {
        schema: String,
    },
    DatabaseTypeSchema {
        schema: String,
    },
    DatabaseRuleSchema {
        schema: String,
    },

    SessionOpen {
        session_id: SessionID,
        server_duration: Duration,
    },
    SessionPulse,
    SessionClose,

    TransactionOpen {
        request_sink: Sender<transaction::Client>,
        grpc_stream: Streaming<transaction::Server>,
    },
}

impl From<server_manager::all::Res> for Response {
    fn from(server_manager::all::Res { servers }: server_manager::all::Res) -> Self {
        let servers = servers.into_iter().map(|server| server.address.parse().unwrap()).collect();
        Response::ServersAll { servers }
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

impl From<cluster_database_manager::get::Res> for Response {
    fn from(res: cluster_database_manager::get::Res) -> Self {
        Response::DatabaseGet { database: res.database.unwrap().into() }
    }
}

impl From<cluster_database_manager::all::Res> for Response {
    fn from(res: cluster_database_manager::all::Res) -> Self {
        Response::DatabasesAll { databases: res.databases.into_iter().map(Into::into).collect() }
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
    fn from(
        (request_sink, grpc_stream): (Sender<transaction::Client>, Streaming<transaction::Server>),
    ) -> Self {
        Self::TransactionOpen { request_sink, grpc_stream }
    }
}

// *** //

#[derive(Debug)]
pub(super) enum TransactionRequest {
    Open {
        session_id: SessionID,
        transaction_type: TransactionType,
        options: Options,
        network_latency: Duration,
    },
    Commit,
    Rollback,
    Query(QueryRequest),
    Stream {
        request_id: RequestID,
    },
}

impl From<TransactionRequest> for transaction::Req {
    fn from(request: TransactionRequest) -> Self {
        let mut request_id = RequestID::generate(); // FIXME defer

        let req = match request {
            TransactionRequest::Open { session_id, transaction_type, options, network_latency } => {
                transaction::req::Req::OpenReq(transaction::open::Req {
                    session_id: session_id.into(),
                    r#type: transaction_type.to_proto().into(),
                    options: Some(options.to_proto()),
                    network_latency_millis: network_latency.as_millis() as i32,
                })
            }
            TransactionRequest::Commit => {
                transaction::req::Req::CommitReq(transaction::commit::Req {})
            }
            TransactionRequest::Rollback => {
                transaction::req::Req::RollbackReq(transaction::rollback::Req {})
            }
            TransactionRequest::Query(query_request) => {
                transaction::req::Req::QueryManagerReq(query_request.into())
            }
            TransactionRequest::Stream { request_id: req_id } => {
                request_id = req_id;
                transaction::req::Req::StreamReq(transaction::stream::Req {})
            }
        };

        transaction::Req { req_id: request_id.into(), metadata: Default::default(), req: Some(req) }
    }
}

#[derive(Debug)]
pub(super) enum TransactionResponse {
    Open,
    Commit,
    Rollback,
    Query(QueryResponse),
}

impl From<transaction::Res> for TransactionResponse {
    fn from(response: transaction::Res) -> Self {
        match response.res {
            Some(transaction::res::Res::OpenRes(_)) => TransactionResponse::Open,
            Some(transaction::res::Res::CommitRes(_)) => TransactionResponse::Commit,
            Some(transaction::res::Res::RollbackRes(_)) => TransactionResponse::Rollback,
            Some(transaction::res::Res::QueryManagerRes(res)) => {
                TransactionResponse::Query(res.into())
            }
            Some(_) => todo!(),
            None => panic!("{}", ClientError::MissingResponseField("res")),
        }
    }
}

impl From<transaction::ResPart> for TransactionResponse {
    fn from(response: transaction::ResPart) -> Self {
        match response.res {
            Some(transaction::res_part::Res::QueryManagerResPart(res_part)) => {
                TransactionResponse::Query(res_part.into())
            }
            Some(_) => todo!(),
            None => panic!("{}", ClientError::MissingResponseField("res")),
        }
    }
}

// *** //

#[derive(Debug)]
pub(super) enum QueryRequest {
    // TODO options
    Define { query: String, options: Options },
    Undefine { query: String, options: Options },
    Delete { query: String, options: Options },

    Match { query: String, options: Options },
    Insert { query: String, options: Options },
    Update { query: String, options: Options },

    MatchAggregate { query: String, options: Options },

    Explain { explainable_id: i64, options: Options }, // FIXME ID type

    MatchGroup { query: String, options: Options },
    MatchGroupAggregate { query: String, options: Options },
}

#[derive(Debug)]
pub(super) enum QueryResponse {
    // TODO options
    Define,
    Undefine,
    Delete,

    Match { answers: Vec<ConceptMap> },
    Insert { answers: Vec<ConceptMap> },
    Update { answers: Vec<ConceptMap> },

    MatchAggregate { answer: Numeric },

    Explain {}, // FIXME explanations

    MatchGroup {},          // FIXME ConceptMapGroup
    MatchGroupAggregate {}, // FIXME NumericGroup
}

impl From<QueryRequest> for query_manager::Req {
    fn from(value: QueryRequest) -> Self {
        let (req, options) = match value {
            QueryRequest::Define { query, options } => {
                (query_manager::req::Req::DefineReq(query_manager::define::Req { query }), options)
            }
            QueryRequest::Undefine { query, options } => (
                query_manager::req::Req::UndefineReq(query_manager::undefine::Req { query }),
                options,
            ),
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

            QueryRequest::MatchAggregate { query, options } => (
                query_manager::req::Req::MatchAggregateReq(query_manager::match_aggregate::Req {
                    query,
                }),
                options,
            ),

            _ => todo!(),
        };
        query_manager::Req { req: Some(req), options: Some(options.to_proto()) }
    }
}

impl From<query_manager::Res> for QueryResponse {
    fn from(value: query_manager::Res) -> Self {
        match value.res {
            Some(query_manager::res::Res::DefineRes(_)) => QueryResponse::Define,
            Some(query_manager::res::Res::UndefineRes(_)) => QueryResponse::Undefine,
            Some(query_manager::res::Res::DeleteRes(_)) => QueryResponse::Delete,
            Some(query_manager::res::Res::MatchAggregateRes(res)) => {
                QueryResponse::MatchAggregate { answer: res.answer.unwrap().try_into().unwrap() }
            }
            _ => todo!(),
        }
    }
}
impl From<query_manager::ResPart> for QueryResponse {
    fn from(value: query_manager::ResPart) -> Self {
        match value.res {
            Some(query_manager::res_part::Res::MatchResPart(res)) => QueryResponse::Match {
                answers: res
                    .answers
                    .into_iter()
                    .map(ConceptMap::from_proto)
                    .collect::<Result<_>>()
                    .unwrap(),
            },
            Some(query_manager::res_part::Res::InsertResPart(res)) => QueryResponse::Insert {
                answers: res
                    .answers
                    .into_iter()
                    .map(ConceptMap::from_proto)
                    .collect::<Result<_>>()
                    .unwrap(),
            },
            _ => todo!(),
        }
    }
}
