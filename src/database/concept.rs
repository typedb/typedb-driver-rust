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

use std::sync::Arc;

use futures::Stream;

use crate::{
    common::Transitivity,
    concept::{AttributeType, Entity, EntityType, ValueType},
    connection::TransactionStream,
    Annotation, Result,
};

#[derive(Debug)]
pub struct ConceptManager {
    transaction_stream: Arc<TransactionStream>,
}

impl ConceptManager {
    pub(crate) fn new(transaction_stream: Arc<TransactionStream>) -> Self {
        Self { transaction_stream }
    }

    pub async fn get_entity_type(&self, label: String) -> Result<Option<EntityType>> {
        self.transaction_stream.get_entity_type(label).await
    }

    pub async fn get_attribute_type(&self, label: String) -> Result<Option<AttributeType>> {
        self.transaction_stream.get_attribute_type(label).await
    }

    pub async fn put_entity_type(&self, label: String) -> Result<EntityType> {
        self.transaction_stream.put_entity_type(label).await
    }

    pub async fn put_attribute_type(&self, label: String, value_type: ValueType) -> Result<AttributeType> {
        self.transaction_stream.put_attribute_type(label, value_type).await
    }

    pub(crate) async fn entity_type_delete(&self, entity_type: EntityType) -> Result {
        self.transaction_stream.thing_type_delete(entity_type.label).await
    }

    pub(crate) async fn entity_type_set_label(&self, entity_type: EntityType, new_label: String) -> Result {
        self.transaction_stream.thing_type_set_label(entity_type.label, new_label).await
    }

    pub(crate) async fn entity_type_set_abstract(&self, entity_type: EntityType) -> Result {
        self.transaction_stream.thing_type_set_abstract(entity_type.label).await
    }

    pub(crate) async fn entity_type_unset_abstract(&self, entity_type: EntityType) -> Result {
        self.transaction_stream.thing_type_unset_abstract(entity_type.label).await
    }

    pub fn entity_type_get_owns(
        &self,
        entity_type: EntityType,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotation_filter: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        self.transaction_stream.thing_type_get_owns(entity_type.label, value_type, transitivity, annotation_filter)
    }

    pub async fn entity_type_get_owns_overridden(
        &self,
        entity_type: EntityType,
        overridden_attribute_type: AttributeType,
    ) -> Result<Option<AttributeType>> {
        self.transaction_stream.thing_type_get_owns_overridden(entity_type.label, overridden_attribute_type.label).await
    }

    pub async fn entity_type_set_owns(
        &self,
        entity_type: EntityType,
        attribute_type: AttributeType,
        overridden_attribute_type: Option<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result {
        self.transaction_stream
            .thing_type_set_owns(
                entity_type.label,
                attribute_type.label,
                overridden_attribute_type.map(|at| at.label),
                annotations,
            )
            .await
    }

    pub async fn entity_type_unset_owns(&self, entity_type: EntityType, attribute_type: AttributeType) -> Result {
        self.transaction_stream.thing_type_unset_owns(entity_type.label, attribute_type.label).await
    }

    pub(crate) async fn entity_type_create(&self, entity_type: EntityType) -> Result<Entity> {
        self.transaction_stream.entity_type_create(entity_type.label).await
    }

    pub(crate) async fn entity_type_get_supertype(&self, entity_type: EntityType) -> Result<EntityType> {
        self.transaction_stream.entity_type_get_supertype(entity_type.label).await
    }

    pub(crate) async fn entity_type_set_supertype(&self, entity_type: EntityType, supertype: EntityType) -> Result {
        self.transaction_stream.entity_type_set_supertype(entity_type.label, supertype.label).await
    }

    pub(crate) fn entity_type_get_supertypes(
        &self,
        entity_type: EntityType,
    ) -> Result<impl Stream<Item = Result<EntityType>>> {
        self.transaction_stream.entity_type_get_supertypes(entity_type.label)
    }

    pub(crate) fn entity_type_get_subtypes(
        &self,
        entity_type: EntityType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<EntityType>>> {
        self.transaction_stream.entity_type_get_subtypes(entity_type.label, transitivity)
    }
}
