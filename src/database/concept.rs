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
    common::{Transitivity, IID},
    concept::{
        Attribute, AttributeType, Entity, EntityType, Relation, RelationType, RoleType, ThingType, Value, ValueType,
    },
    connection::TransactionStream,
    Annotation, Result, SchemaException,
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

    pub async fn get_relation_type(&self, label: String) -> Result<Option<RelationType>> {
        self.transaction_stream.get_relation_type(label).await
    }

    pub async fn get_attribute_type(&self, label: String) -> Result<Option<AttributeType>> {
        self.transaction_stream.get_attribute_type(label).await
    }

    pub async fn put_entity_type(&self, label: String) -> Result<EntityType> {
        self.transaction_stream.put_entity_type(label).await
    }

    pub async fn put_relation_type(&self, label: String) -> Result<RelationType> {
        self.transaction_stream.put_relation_type(label).await
    }

    pub async fn put_attribute_type(&self, label: String, value_type: ValueType) -> Result<AttributeType> {
        self.transaction_stream.put_attribute_type(label, value_type).await
    }

    pub async fn get_entity(&self, iid: IID) -> Result<Option<Entity>> {
        self.transaction_stream.get_entity(iid).await
    }

    pub async fn get_relation(&self, iid: IID) -> Result<Option<Relation>> {
        self.transaction_stream.get_relation(iid).await
    }

    pub async fn get_attribute(&self, iid: IID) -> Result<Option<Attribute>> {
        self.transaction_stream.get_attribute(iid).await
    }

    pub fn get_schema_exceptions(&self) -> Result<impl Stream<Item = Result<SchemaException>>> {
        self.transaction_stream.get_schema_exceptions()
    }

    pub(crate) async fn entity_type_delete(&self, entity_type: EntityType) -> Result {
        self.transaction_stream.thing_type_delete(ThingType::EntityType(entity_type)).await
    }

    pub(crate) async fn entity_type_set_label(&self, entity_type: EntityType, new_label: String) -> Result {
        self.transaction_stream.thing_type_set_label(ThingType::EntityType(entity_type), new_label).await
    }

    pub(crate) async fn entity_type_set_abstract(&self, entity_type: EntityType) -> Result {
        self.transaction_stream.thing_type_set_abstract(ThingType::EntityType(entity_type)).await
    }

    pub(crate) async fn entity_type_unset_abstract(&self, entity_type: EntityType) -> Result {
        self.transaction_stream.thing_type_unset_abstract(ThingType::EntityType(entity_type)).await
    }

    pub(crate) fn entity_type_get_owns(
        &self,
        entity_type: EntityType,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotation_filter: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        self.transaction_stream.thing_type_get_owns(
            ThingType::EntityType(entity_type),
            value_type,
            transitivity,
            annotation_filter,
        )
    }

    pub(crate) async fn entity_type_get_owns_overridden(
        &self,
        entity_type: EntityType,
        overridden_attribute_type: AttributeType,
    ) -> Result<Option<AttributeType>> {
        self.transaction_stream
            .thing_type_get_owns_overridden(ThingType::EntityType(entity_type), overridden_attribute_type)
            .await
    }

    pub(crate) async fn entity_type_set_owns(
        &self,
        entity_type: EntityType,
        attribute_type: AttributeType,
        overridden_attribute_type: Option<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result {
        self.transaction_stream
            .thing_type_set_owns(
                ThingType::EntityType(entity_type),
                attribute_type,
                overridden_attribute_type,
                annotations,
            )
            .await
    }

    pub(crate) async fn entity_type_unset_owns(
        &self,
        entity_type: EntityType,
        attribute_type: AttributeType,
    ) -> Result {
        self.transaction_stream.thing_type_unset_owns(ThingType::EntityType(entity_type), attribute_type).await
    }

    pub(crate) async fn entity_type_create(&self, entity_type: EntityType) -> Result<Entity> {
        self.transaction_stream.entity_type_create(entity_type).await
    }

    pub(crate) async fn entity_type_get_supertype(&self, entity_type: EntityType) -> Result<EntityType> {
        self.transaction_stream.entity_type_get_supertype(entity_type).await
    }

    pub(crate) async fn entity_type_set_supertype(&self, entity_type: EntityType, supertype: EntityType) -> Result {
        self.transaction_stream.entity_type_set_supertype(entity_type, supertype).await
    }

    pub(crate) fn entity_type_get_supertypes(
        &self,
        entity_type: EntityType,
    ) -> Result<impl Stream<Item = Result<EntityType>>> {
        self.transaction_stream.entity_type_get_supertypes(entity_type)
    }

    pub(crate) fn entity_type_get_subtypes(
        &self,
        entity_type: EntityType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<EntityType>>> {
        self.transaction_stream.entity_type_get_subtypes(entity_type, transitivity)
    }

    pub(crate) fn entity_type_get_instances(
        &self,
        entity_type: EntityType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<Entity>>> {
        self.transaction_stream.entity_type_get_instances(entity_type, transitivity)
    }

    pub(crate) async fn relation_type_delete(&self, relation_type: RelationType) -> Result {
        self.transaction_stream.thing_type_delete(ThingType::RelationType(relation_type)).await
    }

    pub(crate) async fn relation_type_set_label(&self, relation_type: RelationType, new_label: String) -> Result {
        self.transaction_stream.thing_type_set_label(ThingType::RelationType(relation_type), new_label).await
    }

    pub(crate) async fn relation_type_set_abstract(&self, relation_type: RelationType) -> Result {
        self.transaction_stream.thing_type_set_abstract(ThingType::RelationType(relation_type)).await
    }

    pub(crate) async fn relation_type_unset_abstract(&self, relation_type: RelationType) -> Result {
        self.transaction_stream.thing_type_unset_abstract(ThingType::RelationType(relation_type)).await
    }

    pub(crate) fn relation_type_get_owns(
        &self,
        relation_type: RelationType,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotation_filter: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        self.transaction_stream.thing_type_get_owns(
            ThingType::RelationType(relation_type),
            value_type,
            transitivity,
            annotation_filter,
        )
    }

    pub(crate) async fn relation_type_get_owns_overridden(
        &self,
        relation_type: RelationType,
        overridden_attribute_type: AttributeType,
    ) -> Result<Option<AttributeType>> {
        self.transaction_stream
            .thing_type_get_owns_overridden(ThingType::RelationType(relation_type), overridden_attribute_type)
            .await
    }

    pub(crate) async fn relation_type_set_owns(
        &self,
        relation_type: RelationType,
        attribute_type: AttributeType,
        overridden_attribute_type: Option<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result {
        self.transaction_stream
            .thing_type_set_owns(
                ThingType::RelationType(relation_type),
                attribute_type,
                overridden_attribute_type,
                annotations,
            )
            .await
    }

    pub(crate) async fn relation_type_unset_owns(
        &self,
        relation_type: RelationType,
        attribute_type: AttributeType,
    ) -> Result {
        self.transaction_stream.thing_type_unset_owns(ThingType::RelationType(relation_type), attribute_type).await
    }

    pub(crate) async fn relation_type_create(&self, relation_type: RelationType) -> Result<Relation> {
        self.transaction_stream.relation_type_create(relation_type).await
    }

    pub(crate) async fn relation_type_get_supertype(&self, relation_type: RelationType) -> Result<RelationType> {
        self.transaction_stream.relation_type_get_supertype(relation_type).await
    }

    pub(crate) async fn relation_type_set_supertype(
        &self,
        relation_type: RelationType,
        supertype: RelationType,
    ) -> Result {
        self.transaction_stream.relation_type_set_supertype(relation_type, supertype).await
    }

    pub(crate) fn relation_type_get_supertypes(
        &self,
        relation_type: RelationType,
    ) -> Result<impl Stream<Item = Result<RelationType>>> {
        self.transaction_stream.relation_type_get_supertypes(relation_type)
    }

    pub(crate) fn relation_type_get_subtypes(
        &self,
        relation_type: RelationType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<RelationType>>> {
        self.transaction_stream.relation_type_get_subtypes(relation_type, transitivity)
    }

    pub(crate) fn relation_type_get_instances(
        &self,
        relation_type: RelationType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<Relation>>> {
        self.transaction_stream.relation_type_get_instances(relation_type, transitivity)
    }

    pub(crate) async fn attribute_type_delete(&self, attribute_type: AttributeType) -> Result {
        self.transaction_stream.thing_type_delete(ThingType::AttributeType(attribute_type)).await
    }

    pub(crate) async fn attribute_type_set_label(&self, attribute_type: AttributeType, new_label: String) -> Result {
        self.transaction_stream.thing_type_set_label(ThingType::AttributeType(attribute_type), new_label).await
    }

    pub(crate) async fn attribute_type_set_abstract(&self, attribute_type: AttributeType) -> Result {
        self.transaction_stream.thing_type_set_abstract(ThingType::AttributeType(attribute_type)).await
    }

    pub(crate) async fn attribute_type_unset_abstract(&self, attribute_type: AttributeType) -> Result {
        self.transaction_stream.thing_type_unset_abstract(ThingType::AttributeType(attribute_type)).await
    }

    pub(crate) fn attribute_type_get_owns(
        &self,
        attribute_type: AttributeType,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotation_filter: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        self.transaction_stream.thing_type_get_owns(
            ThingType::AttributeType(attribute_type),
            value_type,
            transitivity,
            annotation_filter,
        )
    }

    pub(crate) async fn attribute_type_get_owns_overridden(
        &self,
        attribute_type: AttributeType,
        overridden_attribute_type: AttributeType,
    ) -> Result<Option<AttributeType>> {
        self.transaction_stream
            .thing_type_get_owns_overridden(ThingType::AttributeType(attribute_type), overridden_attribute_type)
            .await
    }

    pub(crate) async fn attribute_type_set_owns(
        &self,
        owner_attribute_type: AttributeType,
        attribute_type: AttributeType,
        overridden_attribute_type: Option<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result {
        self.transaction_stream
            .thing_type_set_owns(
                ThingType::AttributeType(owner_attribute_type),
                attribute_type,
                overridden_attribute_type,
                annotations,
            )
            .await
    }

    pub(crate) async fn attribute_type_unset_owns(
        &self,
        owner_attribute_type: AttributeType,
        attribute_type: AttributeType,
    ) -> Result {
        self.transaction_stream
            .thing_type_unset_owns(ThingType::AttributeType(owner_attribute_type), attribute_type)
            .await
    }

    pub(crate) async fn attribute_type_put(&self, attribute_type: AttributeType, value: Value) -> Result<Attribute> {
        self.transaction_stream.attribute_type_put(attribute_type, value).await
    }

    pub(crate) async fn attribute_type_get(
        &self,
        attribute_type: AttributeType,
        value: Value,
    ) -> Result<Option<Attribute>> {
        self.transaction_stream.attribute_type_get(attribute_type, value).await
    }

    pub(crate) async fn attribute_type_get_supertype(&self, attribute_type: AttributeType) -> Result<AttributeType> {
        self.transaction_stream.attribute_type_get_supertype(attribute_type).await
    }

    pub(crate) async fn attribute_type_set_supertype(
        &self,
        attribute_type: AttributeType,
        supertype: AttributeType,
    ) -> Result {
        self.transaction_stream.attribute_type_set_supertype(attribute_type, supertype).await
    }

    pub(crate) fn attribute_type_get_supertypes(
        &self,
        attribute_type: AttributeType,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        self.transaction_stream.attribute_type_get_supertypes(attribute_type)
    }

    pub(crate) fn attribute_type_get_subtypes(
        &self,
        attribute_type: AttributeType,
        transitivity: Transitivity,
        value_type: Option<ValueType>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        self.transaction_stream.attribute_type_get_subtypes(attribute_type, transitivity, value_type)
    }

    pub(crate) fn attribute_type_get_instances(
        &self,
        attribute_type: AttributeType,
        transitivity: Transitivity,
        value_type: Option<ValueType>,
    ) -> Result<impl Stream<Item = Result<Attribute>>> {
        self.transaction_stream.attribute_type_get_instances(attribute_type, transitivity, value_type)
    }
}
