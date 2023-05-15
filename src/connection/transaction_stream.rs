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
    common::{Result, Transitivity, IID},
    concept::{
        Attribute, AttributeType, Entity, EntityType, Relation, RelationType, RoleType, ThingType, Value, ValueType,
    },
    connection::message::{
        ConceptRequest, ConceptResponse, QueryRequest, QueryResponse, ThingTypeRequest, ThingTypeResponse,
        TransactionRequest, TransactionResponse,
    },
    error::InternalError,
    Annotation, Options, SchemaException, TransactionType,
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

    pub(crate) async fn get_entity(&self, iid: IID) -> Result<Option<Entity>> {
        match self.concept_single(ConceptRequest::GetEntity { iid }).await? {
            ConceptResponse::GetEntity { entity } => Ok(entity),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn get_relation(&self, iid: IID) -> Result<Option<Relation>> {
        match self.concept_single(ConceptRequest::GetRelation { iid }).await? {
            ConceptResponse::GetRelation { relation } => Ok(relation),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn get_attribute(&self, iid: IID) -> Result<Option<Attribute>> {
        match self.concept_single(ConceptRequest::GetAttribute { iid }).await? {
            ConceptResponse::GetAttribute { attribute } => Ok(attribute),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) fn get_schema_exceptions(&self) -> Result<impl Stream<Item = Result<SchemaException>>> {
        let stream = self.concept_stream(ConceptRequest::GetSchemaExceptions)?;
        Ok(stream.flat_map(|result| match result {
            Ok(ConceptResponse::GetSchemaExceptions { exceptions }) => stream_iter(exceptions.into_iter().map(Ok)),
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) async fn thing_type_delete(&self, thing_type: ThingType) -> Result {
        match self.thing_type_single(ThingTypeRequest::ThingTypeDelete { thing_type }).await? {
            ThingTypeResponse::ThingTypeDelete => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_set_label(&self, thing_type: ThingType, new_label: String) -> Result {
        match self.thing_type_single(ThingTypeRequest::ThingTypeSetLabel { thing_type, new_label }).await? {
            ThingTypeResponse::ThingTypeSetLabel => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_set_abstract(&self, thing_type: ThingType) -> Result {
        match self.thing_type_single(ThingTypeRequest::ThingTypeSetAbstract { thing_type }).await? {
            ThingTypeResponse::ThingTypeSetAbstract => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_unset_abstract(&self, thing_type: ThingType) -> Result {
        match self.thing_type_single(ThingTypeRequest::ThingTypeUnsetAbstract { thing_type }).await? {
            ThingTypeResponse::ThingTypeUnsetAbstract => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) fn thing_type_get_owns(
        &self,
        thing_type: ThingType,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotation_filter: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::ThingTypeGetOwns {
            thing_type,
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
        thing_type: ThingType,
        overridden_attribute_type: AttributeType,
    ) -> Result<Option<AttributeType>> {
        match self
            .thing_type_single(ThingTypeRequest::ThingTypeGetOwnsOverridden { thing_type, overridden_attribute_type })
            .await?
        {
            ThingTypeResponse::ThingTypeGetOwnsOverridden { attribute_type } => Ok(attribute_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_set_owns(
        &self,
        thing_type: ThingType,
        attribute_type: AttributeType,
        overridden_attribute_type: Option<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result {
        match self
            .thing_type_single(ThingTypeRequest::ThingTypeSetOwns {
                thing_type,
                attribute_type,
                overridden_attribute_type,
                annotations,
            })
            .await?
        {
            ThingTypeResponse::ThingTypeSetOwns => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn thing_type_unset_owns(&self, thing_type: ThingType, attribute_type: AttributeType) -> Result {
        match self.thing_type_single(ThingTypeRequest::ThingTypeUnsetOwns { thing_type, attribute_type }).await? {
            ThingTypeResponse::ThingTypeUnsetOwns => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn entity_type_create(&self, entity_type: EntityType) -> Result<Entity> {
        match self.thing_type_single(ThingTypeRequest::EntityTypeCreate { entity_type }).await? {
            ThingTypeResponse::EntityTypeCreate { entity } => Ok(entity),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn entity_type_get_supertype(&self, entity_type: EntityType) -> Result<EntityType> {
        match self.thing_type_single(ThingTypeRequest::EntityTypeGetSupertype { entity_type }).await? {
            ThingTypeResponse::EntityTypeGetSupertype { supertype: entity_type } => Ok(entity_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn entity_type_set_supertype(&self, entity_type: EntityType, supertype: EntityType) -> Result {
        match self.thing_type_single(ThingTypeRequest::EntityTypeSetSupertype { entity_type, supertype }).await? {
            ThingTypeResponse::EntityTypeSetSupertype => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) fn entity_type_get_supertypes(
        &self,
        entity_type: EntityType,
    ) -> Result<impl Stream<Item = Result<EntityType>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::EntityTypeGetSupertypes { entity_type })?;
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
        entity_type: EntityType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<EntityType>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::EntityTypeGetSubtypes { entity_type, transitivity })?;
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
        entity_type: EntityType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<Entity>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::EntityTypeGetInstances { entity_type, transitivity })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::EntityTypeGetInstances { entities }) => stream_iter(entities.into_iter().map(Ok)),
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) async fn relation_type_create(&self, relation_type: RelationType) -> Result<Relation> {
        match self.thing_type_single(ThingTypeRequest::RelationTypeCreate { relation_type }).await? {
            ThingTypeResponse::RelationTypeCreate { relation } => Ok(relation),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn relation_type_get_supertype(&self, relation_type: RelationType) -> Result<RelationType> {
        match self.thing_type_single(ThingTypeRequest::RelationTypeGetSupertype { relation_type }).await? {
            ThingTypeResponse::RelationTypeGetSupertype { supertype: relation_type } => Ok(relation_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn relation_type_set_supertype(
        &self,
        relation_type: RelationType,
        supertype: RelationType,
    ) -> Result {
        match self.thing_type_single(ThingTypeRequest::RelationTypeSetSupertype { relation_type, supertype }).await? {
            ThingTypeResponse::RelationTypeSetSupertype => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) fn relation_type_get_supertypes(
        &self,
        relation_type: RelationType,
    ) -> Result<impl Stream<Item = Result<RelationType>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::RelationTypeGetSupertypes { relation_type })?;
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
        relation_type: RelationType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<RelationType>>> {
        let stream =
            self.thing_type_stream(ThingTypeRequest::RelationTypeGetSubtypes { relation_type, transitivity })?;
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
        relation_type: RelationType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<Relation>>> {
        let stream =
            self.thing_type_stream(ThingTypeRequest::RelationTypeGetInstances { relation_type, transitivity })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::RelationTypeGetInstances { relations }) => stream_iter(relations.into_iter().map(Ok)),
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) async fn attribute_type_put(&self, attribute_type: AttributeType, value: Value) -> Result<Attribute> {
        match self.thing_type_single(ThingTypeRequest::AttributeTypePut { attribute_type, value }).await? {
            ThingTypeResponse::AttributeTypePut { attribute } => Ok(attribute),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn attribute_type_get(
        &self,
        attribute_type: AttributeType,
        value: Value,
    ) -> Result<Option<Attribute>> {
        match self.thing_type_single(ThingTypeRequest::AttributeTypeGet { attribute_type, value }).await? {
            ThingTypeResponse::AttributeTypeGet { attribute } => Ok(attribute),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn attribute_type_get_supertype(&self, attribute_type: AttributeType) -> Result<AttributeType> {
        match self.thing_type_single(ThingTypeRequest::AttributeTypeGetSupertype { attribute_type }).await? {
            ThingTypeResponse::AttributeTypeGetSupertype { supertype: attribute_type } => Ok(attribute_type),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) async fn attribute_type_set_supertype(
        &self,
        attribute_type: AttributeType,
        supertype: AttributeType,
    ) -> Result {
        match self.thing_type_single(ThingTypeRequest::AttributeTypeSetSupertype { attribute_type, supertype }).await? {
            ThingTypeResponse::AttributeTypeSetSupertype => Ok(()),
            other => Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into()),
        }
    }

    pub(crate) fn attribute_type_get_supertypes(
        &self,
        attribute_type: AttributeType,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::AttributeTypeGetSupertypes { attribute_type })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::AttributeTypeGetSupertypes { supertypes: attribute_types }) => {
                stream_iter(attribute_types.into_iter().map(Ok))
            }
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) fn attribute_type_get_subtypes(
        &self,
        attribute_type: AttributeType,
        transitivity: Transitivity,
        value_type: Option<ValueType>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::AttributeTypeGetSubtypes {
            attribute_type,
            transitivity,
            value_type,
        })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::AttributeTypeGetSubtypes { subtypes: attribute_types }) => {
                stream_iter(attribute_types.into_iter().map(Ok))
            }
            Ok(other) => stream_once(Err(InternalError::UnexpectedResponseType(format!("{other:?}")).into())),
            Err(err) => stream_once(Err(err)),
        }))
    }

    pub(crate) fn attribute_type_get_instances(
        &self,
        attribute_type: AttributeType,
        transitivity: Transitivity,
        value_type: Option<ValueType>,
    ) -> Result<impl Stream<Item = Result<Attribute>>> {
        let stream = self.thing_type_stream(ThingTypeRequest::AttributeTypeGetInstances {
            attribute_type,
            transitivity,
            value_type,
        })?;
        Ok(stream.flat_map(|result| match result {
            Ok(ThingTypeResponse::AttributeTypeGetInstances { attributes }) => {
                stream_iter(attributes.into_iter().map(Ok))
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

    fn concept_stream(&self, req: ConceptRequest) -> Result<impl Stream<Item = Result<ConceptResponse>>> {
        Ok(self.stream(TransactionRequest::Concept(req))?.map(|response| match response {
            Ok(TransactionResponse::Concept(res)) => Ok(res),
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
