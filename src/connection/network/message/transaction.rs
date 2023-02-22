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

use typedb_protocol::transaction;

use super::{QueryRequest, QueryResponse};
use crate::{
    common::{error::ClientError, RequestID, SessionID},
    connection::network::proto::IntoProto,
    Error, Options, Result, TransactionType,
};

#[derive(Debug)]
pub(in crate::connection) enum TransactionRequest {
    Open { session_id: SessionID, transaction_type: TransactionType, options: Options, network_latency: Duration },
    Commit,
    Rollback,
    Query(QueryRequest),
    Stream { request_id: RequestID },
}

impl From<TransactionRequest> for transaction::Req {
    fn from(request: TransactionRequest) -> Self {
        let mut request_id = None;

        let req = match request {
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
            TransactionRequest::Query(query_request) => transaction::req::Req::QueryManagerReq(query_request.into()),
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

#[derive(Debug)]
pub(in crate::connection) enum TransactionResponse {
    Open,
    Commit,
    Rollback,
    Query(QueryResponse),
}

impl TryFrom<transaction::Res> for TransactionResponse {
    type Error = Error;
    fn try_from(response: transaction::Res) -> Result<Self> {
        match response.res {
            Some(transaction::res::Res::OpenRes(_)) => Ok(TransactionResponse::Open),
            Some(transaction::res::Res::CommitRes(_)) => Ok(TransactionResponse::Commit),
            Some(transaction::res::Res::RollbackRes(_)) => Ok(TransactionResponse::Rollback),
            Some(transaction::res::Res::QueryManagerRes(res)) => Ok(TransactionResponse::Query(res.try_into()?)),
            Some(_) => todo!(),
            None => Err(ClientError::MissingResponseField("res").into()),
        }
    }
}

impl TryFrom<transaction::ResPart> for TransactionResponse {
    type Error = Error;
    fn try_from(response: transaction::ResPart) -> Result<Self> {
        match response.res {
            Some(transaction::res_part::Res::QueryManagerResPart(res_part)) => {
                Ok(TransactionResponse::Query(res_part.try_into()?))
            }
            Some(_) => todo!(),
            None => Err(ClientError::MissingResponseField("res").into()),
        }
    }
}
