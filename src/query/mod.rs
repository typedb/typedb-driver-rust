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

use std::iter::once;

use futures::{stream, Stream, StreamExt};
use query_manager::res::Res::MatchAggregateRes;
use typedb_protocol::{
    query_manager,
    query_manager::res_part::Res::{InsertResPart, MatchResPart, UpdateResPart},
    transaction,
};

use crate::{
    answer::{ConceptMap, Numeric},
    common::{
        error::ClientError,
        rpc::builder::query_manager::{
            define_req, delete_req, insert_req, match_aggregate_req, match_req, undefine_req,
            update_req,
        },
        Result, TransactionRPC,
    },
    connection::core,
};

macro_rules! stream_concept_maps {
    ($self:ident, $req:ident, $res_part_kind:ident, $query_type_str:tt) => {
        Ok($self.stream_answers($req)?.flat_map(|result: Result<query_manager::res_part::Res>| {
            match result {
                Ok(res_part) => match res_part {
                    $res_part_kind(x) => {
                        stream::iter(x.answers.into_iter().map(|cm| ConceptMap::from_proto(cm)))
                            .left_stream()
                    }
                    _ => stream::iter(once(Err(ClientError::MissingResponseField(concat!(
                        "query_manager_res_part.",
                        $query_type_str,
                        "_res_part"
                    ))
                    .into())))
                    .right_stream(),
                },
                Err(err) => stream::iter(once(Err(err))).right_stream(),
            }
        }))
    };
}

#[derive(Clone, Debug)]
pub struct QueryManager {
    tx: TransactionRPC,
}

impl QueryManager {
    pub(crate) fn new(tx: TransactionRPC) -> QueryManager {
        QueryManager { tx }
    }

    pub async fn define(&mut self, query: &str) -> Result {
        self.single_call(define_req(query, None)).await?;
        Ok(())
    }

    pub async fn define_with_options(&mut self, query: &str, options: &core::Options) -> Result {
        self.single_call(define_req(query, Some(options.to_proto()))).await?;
        Ok(())
    }

    pub async fn delete(&mut self, query: &str) -> Result {
        self.single_call(delete_req(query, None)).await?;
        Ok(())
    }

    pub async fn delete_with_options(&mut self, query: &str, options: &core::Options) -> Result {
        self.single_call(delete_req(query, Some(options.to_proto()))).await?;
        Ok(())
    }

    pub fn insert(&mut self, query: &str) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let req = insert_req(query, None);
        stream_concept_maps!(self, req, InsertResPart, "insert")
    }

    pub fn insert_with_options(
        &mut self,
        query: &str,
        options: &core::Options,
    ) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let req = insert_req(query, Some(options.to_proto()));
        stream_concept_maps!(self, req, InsertResPart, "insert")
    }

    // TODO: investigate performance impact of using BoxStream
    pub fn match_(&mut self, query: &str) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let req = match_req(query, None);
        stream_concept_maps!(self, req, MatchResPart, "match")
    }

    pub fn match_with_options(
        &mut self,
        query: &str,
        options: &core::Options,
    ) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let req = match_req(query, Some(options.to_proto()));
        stream_concept_maps!(self, req, MatchResPart, "match")
    }

    pub async fn match_aggregate(&mut self, query: &str) -> Result<Numeric> {
        match self.single_call(match_aggregate_req(query, None)).await? {
            MatchAggregateRes(res) => res.answer.unwrap().try_into(),
            _ => Err(ClientError::MissingResponseField("match_aggregate_res"))?,
        }
    }

    pub async fn match_aggregate_with_options(
        &mut self,
        query: &str,
        options: core::Options,
    ) -> Result<Numeric> {
        match self.single_call(match_aggregate_req(query, Some(options.to_proto()))).await? {
            MatchAggregateRes(res) => res.answer.unwrap().try_into(),
            _ => Err(ClientError::MissingResponseField("match_aggregate_res"))?,
        }
    }

    pub async fn undefine(&mut self, query: &str) -> Result {
        self.single_call(undefine_req(query, None)).await?;
        Ok(())
    }

    pub async fn undefine_with_options(&mut self, query: &str, options: &core::Options) -> Result {
        self.single_call(undefine_req(query, Some(options.to_proto()))).await?;
        Ok(())
    }

    pub fn update(&mut self, query: &str) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let req = update_req(query, None);
        stream_concept_maps!(self, req, UpdateResPart, "update")
    }

    pub fn update_with_options(
        &mut self,
        query: &str,
        options: &core::Options,
    ) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let req = update_req(query, Some(options.to_proto()));
        stream_concept_maps!(self, req, UpdateResPart, "update")
    }

    async fn single_call(&mut self, req: transaction::Req) -> Result<query_manager::res::Res> {
        match self.tx.single(req).await?.res {
            Some(transaction::res::Res::QueryManagerRes(res)) => {
                res.res.ok_or(ClientError::MissingResponseField("res.query_manager_res").into())
            }
            _ => Err(ClientError::MissingResponseField("res.query_manager_res"))?,
        }
    }

    fn stream_answers(
        &mut self,
        req: transaction::Req,
    ) -> Result<impl Stream<Item = Result<query_manager::res_part::Res>>> {
        Ok(self.tx.stream(req)?.map(|result: Result<transaction::ResPart>| match result {
            Ok(tx_res_part) => match tx_res_part.res {
                Some(transaction::res_part::Res::QueryManagerResPart(res_part)) => {
                    res_part.res.ok_or(
                        ClientError::MissingResponseField("res_part.query_manager_res_part").into(),
                    )
                }
                _ => Err(ClientError::MissingResponseField("res_part.query_manager_res_part"))?,
            },
            Err(err) => Err(err),
        }))
    }
}
