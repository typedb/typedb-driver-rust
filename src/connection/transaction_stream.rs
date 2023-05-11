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
    concept::{AttributeType, Entity, EntityType, Relation, RelationType, ValueType},
    connection::message::{
        ConceptRequest, ConceptResponse, QueryRequest, QueryResponse, ThingTypeRequest, ThingTypeResponse,
        TransactionRequest, TransactionResponse,
    },
    error::InternalError,
    Annotation, Options, TransactionType,
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
        self.query_single(QueryRequest::Define { query, options }).await?;
        Ok(())
    }

    pub(crate) async fn undefine(&self, query: String, options: Options) -> Result {
        self.query_single(QueryRequest::Undefine { query, options }).await?;
        Ok(())
    }

    pub(crate) async fn delete(&self, query: String, options: Options) -> Result {
        self.query_single(QueryRequest::Delete { query, options }).await?;
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
        match self.concept_single(ConceptRequest::GetEntityType { label }).await? {
            ConceptResponse::GetEntityType { entity_type } => Ok(entity_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn get_relation_type(&self, label: String) -> Result<Option<RelationType>> {
        match self.concept_single(ConceptRequest::GetRelationType { label }).await? {
            ConceptResponse::GetRelationType { relation_type } => Ok(relation_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn get_attribute_type(&self, label: String) -> Result<Option<AttributeType>> {
        match self.concept_single(ConceptRequest::GetAttributeType { label }).await? {
            ConceptResponse::GetAttributeType { attribute_type } => Ok(attribute_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn put_entity_type(&self, label: String) -> Result<EntityType> {
        match self.concept_single(ConceptRequest::PutEntityType { label }).await? {
            ConceptResponse::PutEntityType { entity_type } => Ok(entity_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn put_relation_type(&self, label: String) -> Result<RelationType> {
        match self.concept_single(ConceptRequest::PutRelationType { label }).await? {
            ConceptResponse::PutRelationType { relation_type } => Ok(relation_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn put_attribute_type(&self, label: String, value_type: ValueType) -> Result<AttributeType> {
        match self.concept_single(ConceptRequest::PutAttributeType { label, value_type }).await? {
            ConceptResponse::PutAttributeType { attribute_type } => Ok(attribute_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_delete(&self, label: String) -> Result {
        match self.thing_type_single(ThingTypeRequest::ThingTypeDelete { label }).await? {
            ThingTypeResponse::ThingTypeDelete => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_set_label(&self, old_label: String, new_label: String) -> Result {
        match self.thing_type_single(ThingTypeRequest::ThingTypeSetLabel { old_label, new_label }).await? {
            ThingTypeResponse::ThingTypeSetLabel => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_set_abstract(&self, label: String) -> Result {
        match self.thing_type_single(ThingTypeRequest::ThingTypeSetAbstract { label }).await? {
            ThingTypeResponse::ThingTypeSetAbstract => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_unset_abstract(&self, label: String) -> Result {
        match self.thing_type_single(ThingTypeRequest::ThingTypeUnsetAbstract { label }).await? {
            ThingTypeResponse::ThingTypeUnsetAbstract => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) fn thing_type_get_owns(
        &self,
        label: String,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotation_filter: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::ThingTypeGetOwns {
            label,
            value_type,
            transitivity,
            annotation_filter,
        })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::ThingTypeGetOwns { attribute_types }) => {
                stream_iter(attribute_types.into_iter().map(Ok))
            }
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) async fn thing_type_get_owns_overridden(
        &self,
        label: String,
        overridden_attribute_label: String,
    ) -> Result<Option<AttributeType>> {
        match self
            .thing_type_single(ThingTypeRequest::ThingTypeGetOwnsOverridden { label, overridden_attribute_label })
            .await?
        {
            ThingTypeResponse::ThingTypeGetOwnsOverridden { attribute_type } => Ok(attribute_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_set_owns(
        &self,
        label: String,
        attribute_label: String,
        overridden_attribute_label: Option<String>,
        annotations: Vec<Annotation>,
    ) -> Result {
        match self
            .thing_type_single(ThingTypeRequest::ThingTypeSetOwns {
                label,
                attribute_label,
                overridden_attribute_label,
                annotations,
            })
            .await?
        {
            ThingTypeResponse::ThingTypeSetOwns => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_unset_owns(&self, label: String, attribute_label: String) -> Result {
        match self.thing_type_single(ThingTypeRequest::ThingTypeUnsetOwns { label, attribute_label }).await? {
            ThingTypeResponse::ThingTypeUnsetOwns => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn entity_type_create(&self, label: String) -> Result<Entity> {
        match self.thing_type_single(ThingTypeRequest::EntityTypeCreate { label }).await? {
            ThingTypeResponse::EntityTypeCreate { entity } => Ok(entity),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn entity_type_get_supertype(&self, label: String) -> Result<EntityType> {
        match self.thing_type_single(ThingTypeRequest::EntityTypeGetSupertype { label }).await? {
            ThingTypeResponse::EntityTypeGetSupertype { supertype: entity_type } => Ok(entity_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn entity_type_set_supertype(&self, label: String, supertype_label: String) -> Result {
        match self.thing_type_single(ThingTypeRequest::EntityTypeSetSupertype { label, supertype_label }).await? {
            ThingTypeResponse::EntityTypeSetSupertype => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) fn entity_type_get_supertypes(&self, label: String) -> Result<impl Stream<Item = Result<EntityType>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::EntityTypeGetSupertypes { label })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::EntityTypeGetSupertypes { supertypes: entity_types }) => {
                stream_iter(entity_types.into_iter().map(Ok))
            }
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) fn entity_type_get_subtypes(
        &self,
        label: String,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<EntityType>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::EntityTypeGetSubtypes { label, transitivity })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::EntityTypeGetSubtypes { subtypes: entity_types }) => {
                stream_iter(entity_types.into_iter().map(Ok))
            }
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) fn entity_type_get_instances(
        &self,
        label: String,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<Entity>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::EntityTypeGetInstances { label, transitivity })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::EntityTypeGetInstances { entities }) => stream_iter(entities.into_iter().map(Ok)),
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) async fn relation_type_create(&self, label: String) -> Result<Relation> {
        match self.thing_type_single(ThingTypeRequest::RelationTypeCreate { label }).await? {
            ThingTypeResponse::RelationTypeCreate { relation } => Ok(relation),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn relation_type_get_supertype(&self, label: String) -> Result<RelationType> {
        match self.thing_type_single(ThingTypeRequest::RelationTypeGetSupertype { label }).await? {
            ThingTypeResponse::RelationTypeGetSupertype { supertype: relation_type } => Ok(relation_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn relation_type_set_supertype(&self, label: String, supertype_label: String) -> Result {
        match self.thing_type_single(ThingTypeRequest::RelationTypeSetSupertype { label, supertype_label }).await? {
            ThingTypeResponse::RelationTypeSetSupertype => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) fn relation_type_get_supertypes(
        &self,
        label: String,
    ) -> Result<impl Stream<Item = Result<RelationType>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::RelationTypeGetSupertypes { label })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::RelationTypeGetSupertypes { supertypes: relation_types }) => {
                stream_iter(relation_types.into_iter().map(Ok))
            }
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) fn relation_type_get_subtypes(
        &self,
        label: String,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<RelationType>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::RelationTypeGetSubtypes { label, transitivity })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::RelationTypeGetSubtypes { subtypes: relation_types }) => {
                stream_iter(relation_types.into_iter().map(Ok))
            }
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) fn relation_type_get_instances(
        &self,
        label: String,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<Relation>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::RelationTypeGetInstances { label, transitivity })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::RelationTypeGetInstances { relations }) => stream_iter(relations.into_iter().map(Ok)),
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    async fn single(&self, req: TransactionRequest) -> Result<TransactionResponse> {
        self.transaction_transmitter.single(req).await
    }

    async fn query_single(&self, req: QueryRequest) -> Result<QueryResponse> {
        match self.single(TransactionRequest::Query(req)).await? {
            TransactionResponse::Query(res) => Ok(res),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    async fn concept_single(&self, req: ConceptRequest) -> Result<ConceptResponse> {
        match self.single(TransactionRequest::Concept(req)).await? {
            TransactionResponse::Concept(res) => Ok(res),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    async fn thing_type_single(&self, req: ThingTypeRequest) -> Result<ThingTypeResponse> {
        match self.single(TransactionRequest::ThingType(req)).await? {
            TransactionResponse::ThingType(res) => Ok(res),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    fn stream(&self, req: TransactionRequest) -> Result<impl Stream<Item = Result<TransactionResponse>>> {
        self.transaction_transmitter.stream(req)
    }

    fn query_stream(&self, req: QueryRequest) -> Result<impl Stream<Item = Result<QueryResponse>>> {
        Ok(self.stream(TransactionRequest::Query(req))?.map(|response| match response {
            Ok(TransactionResponse::Query(res)) => Ok(res),
            Ok(other) => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
            Err(err) => Err(err),
        }))
    }

    fn thing_type_stream(&self, req: ThingTypeRequest) -> Result<impl Stream<Item = Result<ThingTypeResponse>>> {
        Ok(self.stream(TransactionRequest::ThingType(req))?.map(|response| match response {
            Ok(TransactionResponse::ThingType(res)) => Ok(res),
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
