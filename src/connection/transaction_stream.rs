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

use std::{fmt, iter};

use futures::{stream, Stream, StreamExt};

use super::network::transmitter::TransactionTransmitter;
use crate::{
    answer::{ConceptMap, Numeric},
    common::{Result, Transitivity},
    concept::{Entity, EntityType},
    connection::message::{
        ConceptRequest, ConceptResponse, QueryRequest, QueryResponse, ThingTypeRequest, ThingTypeResponse,
        TransactionRequest, TransactionResponse,
    },
    error::InternalError,
    Options, TransactionType,
};

pub(crate) struct TransactionStream {
    type_: TransactionType,
    options: Options,
    transaction_transmitter: TransactionTransmitter,
}

impl TransactionStream {
    pub(super) fn new(
        type_: TransactionType,
        options: Options,
        transaction_transmitter: TransactionTransmitter,
    ) -> Self {
        Self { type_, options, transaction_transmitter }
    }

    pub(crate) fn is_open(&self) -> bool {
        self.transaction_transmitter.is_open()
    }

    pub(crate) fn type_(&self) -> TransactionType {
        self.type_
    }

    pub(crate) fn options(&self) -> &Options {
        &self.options
    }

    pub(crate) async fn commit(&self) -> Result {
        self.single(TransactionRequest::Commit).await?;
        Ok(())
    }

    pub(crate) async fn rollback(&self) -> Result {
        self.single(TransactionRequest::Rollback).await?;
        Ok(())
    }

    pub(crate) async fn define(&self, query: String, options: Options) -> Result {
        self.single(TransactionRequest::Query(QueryRequest::Define { query, options })).await?;
        Ok(())
    }

    pub(crate) async fn undefine(&self, query: String, options: Options) -> Result {
        self.single(TransactionRequest::Query(QueryRequest::Undefine { query, options })).await?;
        Ok(())
    }

    pub(crate) async fn delete(&self, query: String, options: Options) -> Result {
        self.single(TransactionRequest::Query(QueryRequest::Delete { query, options })).await?;
        Ok(())
    }

    pub(crate) fn match_(&self, query: String, options: Options) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let stream = self.query_stream(QueryRequest::Match { query, options })?;
        Ok(stream.flat_map(|result| match result {
            Ok(QueryResponse::Match { answers }) => stream_iter(answers.into_iter().map(Ok)),
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) fn insert(&self, query: String, options: Options) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let stream = self.query_stream(QueryRequest::Insert { query, options })?;
        Ok(stream.flat_map(|result| match result {
            Ok(QueryResponse::Insert { answers }) => stream_iter(answers.into_iter().map(Ok)),
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) fn update(&self, query: String, options: Options) -> Result<impl Stream<Item = Result<ConceptMap>>> {
        let stream = self.query_stream(QueryRequest::Update { query, options })?;
        Ok(stream.flat_map(|result| match result {
            Ok(QueryResponse::Update { answers }) => stream_iter(answers.into_iter().map(Ok)),
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) async fn match_aggregate(&self, query: String, options: Options) -> Result<Numeric> {
        match self.query_single(QueryRequest::MatchAggregate { query, options }).await? {
            QueryResponse::MatchAggregate { answer } => Ok(answer),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn get_entity_type(&self, label: String) -> Result<Option<EntityType>> {
        match self.single(TransactionRequest::Concept(ConceptRequest::GetEntityType { label })).await? {
            TransactionResponse::Concept(ConceptResponse::GetEntityType { entity_type }) => Ok(entity_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn put_entity_type(&self, label: String) -> Result<EntityType> {
        match self.single(TransactionRequest::Concept(ConceptRequest::PutEntityType { label })).await? {
            TransactionResponse::Concept(ConceptResponse::PutEntityType { entity_type }) => Ok(entity_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_delete(&self, label: String) -> Result {
        match self.single(TransactionRequest::ThingType(ThingTypeRequest::ThingTypeDelete { label })).await? {
            TransactionResponse::ThingType(ThingTypeResponse::ThingTypeDelete) => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn entity_type_create(&self, label: String) -> Result<Entity> {
        match self.single(TransactionRequest::ThingType(ThingTypeRequest::EntityTypeCreate { label })).await? {
            TransactionResponse::ThingType(ThingTypeResponse::EntityTypeCreate { entity }) => Ok(entity),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn entity_type_get_supertype(&self, label: String) -> Result<EntityType> {
        match self.single(TransactionRequest::ThingType(ThingTypeRequest::EntityTypeGetSupertype { label })).await? {
            TransactionResponse::ThingType(ThingTypeResponse::EntityTypeGetSupertype { entity_type }) => {
                Ok(entity_type)
            }
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) fn entity_type_get_subtypes(
        &self,
        label: String,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<EntityType>>> {
        let stream = self
            .stream(TransactionRequest::ThingType(ThingTypeRequest::EntityTypeGetSubtypes { label, transitivity }))?;
        Ok(stream.flat_map(|result| match result {
            Ok(TransactionResponse::ThingType(ThingTypeResponse::EntityTypeGetSubtypes { entity_types })) => {
                stream_iter(entity_types.into_iter().map(Ok))
            }
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    async fn single(&self, req: TransactionRequest) -> Result<TransactionResponse> {
        self.transaction_transmitter.single(req).await
    }

    async fn query_single(&self, req: QueryRequest) -> Result<QueryResponse> {
        match self.single(TransactionRequest::Query(req)).await? {
            TransactionResponse::Query(query) => Ok(query),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    fn stream(&self, req: TransactionRequest) -> Result<impl Stream<Item = Result<TransactionResponse>>> {
        self.transaction_transmitter.stream(req)
    }

    fn query_stream(&self, req: QueryRequest) -> Result<impl Stream<Item = Result<QueryResponse>>> {
        Ok(self.stream(TransactionRequest::Query(req))?.map(|response| match response {
            Ok(TransactionResponse::Query(query)) => Ok(query),
            Ok(other) => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
            Err(err) => Err(err),
        }))
    }
}

impl fmt::Debug for TransactionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionStream").field("type_", &self.type_).field("options", &self.options).finish()
    }
}

fn stream_once<'a, T: Send + 'a>(value: T) -> stream::BoxStream<'a, T> {
    stream_iter(iter::once(value))
}

fn stream_iter<'a, T: Send + 'a>(iter: impl Iterator<Item = T> + Send + 'a) -> stream::BoxStream<'a, T> {
    Box::pin(stream::iter(iter))
}
