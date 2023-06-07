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

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::{
    common::box_stream,
    concept::{
        Annotation, Attribute, AttributeType, Entity, EntityType, Relation, RelationType, RoleType, Thing, ThingType,
        Transitivity, Value, ValueType,
    },
    Result, Transaction,
};

#[async_trait]
pub trait ThingTypeAPI: Clone + Sync + Send {
    fn label(&self) -> &String;

    fn into_thing_type(self) -> ThingType;

    async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool>;

    async fn delete(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_delete(self.clone().into_thing_type()).await
    }

    async fn set_label(&mut self, transaction: &Transaction<'_>, new_label: String) -> Result {
        transaction.concept().thing_type_set_label(self.clone().into_thing_type(), new_label).await
    }

    async fn set_abstract(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_set_abstract(self.clone().into_thing_type()).await
    }

    async fn unset_abstract(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_unset_abstract(self.clone().into_thing_type()).await
    }

    fn get_owns(
        &self,
        transaction: &Transaction<'_>,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotations: Vec<Annotation>,
    ) -> Result<BoxStream<Result<AttributeType>>> {
        transaction
            .concept()
            .thing_type_get_owns(self.clone().into_thing_type(), value_type, transitivity, annotations)
            .map(box_stream)
    }

    async fn get_owns_overridden(
        &self,
        transaction: &Transaction<'_>,
        overridden_attribute_type: AttributeType,
    ) -> Result<Option<AttributeType>> {
        transaction
            .concept()
            .thing_type_get_owns_overridden(self.clone().into_thing_type(), overridden_attribute_type)
            .await
    }

    async fn set_owns(
        &mut self,
        transaction: &Transaction<'_>,
        attribute_type: AttributeType,
        overridden_attribute_type: Option<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result {
        transaction
            .concept()
            .thing_type_set_owns(self.clone().into_thing_type(), attribute_type, overridden_attribute_type, annotations)
            .await
    }

    async fn unset_owns(&mut self, transaction: &Transaction<'_>, attribute_type: AttributeType) -> Result {
        transaction.concept().thing_type_unset_owns(self.clone().into_thing_type(), attribute_type).await
    }

    fn get_plays(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<BoxStream<Result<RoleType>>> {
        transaction.concept().thing_type_get_plays(self.clone().into_thing_type(), transitivity).map(box_stream)
    }

    async fn get_plays_overridden(
        &self,
        transaction: &Transaction<'_>,
        overridden_role_type: RoleType,
    ) -> Result<Option<RoleType>> {
        transaction
            .concept()
            .thing_type_get_plays_overridden(self.clone().into_thing_type(), overridden_role_type)
            .await
    }

    async fn set_plays(
        &mut self,
        transaction: &Transaction<'_>,
        role_type: RoleType,
        overridden_role_type: Option<RoleType>,
    ) -> Result {
        transaction
            .concept()
            .thing_type_set_plays(self.clone().into_thing_type(), role_type, overridden_role_type)
            .await
    }

    async fn unset_plays(&mut self, transaction: &Transaction<'_>, role_type: RoleType) -> Result {
        transaction.concept().thing_type_unset_plays(self.clone().into_thing_type(), role_type).await
    }

    async fn get_syntax(&mut self, transaction: &Transaction<'_>) -> Result<String> {
        transaction.concept().thing_type_get_syntax(self.clone().into_thing_type()).await
    }
}

#[async_trait]
impl ThingTypeAPI for EntityType {
    fn label(&self) -> &String {
        &self.label
    }

    fn into_thing_type(self) -> ThingType {
        ThingType::EntityType(self)
    }

    async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_entity_type(self.label().clone()).await.map(|res| res.is_none())
    }
}

#[async_trait]
pub trait EntityTypeAPI: ThingTypeAPI + Into<EntityType> {
    async fn create(&self, transaction: &Transaction<'_>) -> Result<Entity> {
        transaction.concept().entity_type_create(self.clone().into()).await
    }

    async fn get_supertype(&self, transaction: &Transaction<'_>) -> Result<EntityType> {
        transaction.concept().entity_type_get_supertype(self.clone().into()).await
    }

    async fn set_supertype(&mut self, transaction: &Transaction<'_>, supertype: EntityType) -> Result {
        transaction.concept().entity_type_set_supertype(self.clone().into(), supertype).await
    }

    fn get_supertypes(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<EntityType>>> {
        transaction.concept().entity_type_get_supertypes(self.clone().into()).map(box_stream)
    }

    fn get_subtypes(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<EntityType>>> {
        transaction.concept().entity_type_get_subtypes(self.clone().into(), Transitivity::Transitive).map(box_stream)
    }

    fn get_instances(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<Entity>>> {
        transaction.concept().entity_type_get_instances(self.clone().into(), Transitivity::Transitive).map(box_stream)
    }
}

#[async_trait]
impl EntityTypeAPI for EntityType {}

#[async_trait]
impl ThingTypeAPI for RelationType {
    fn label(&self) -> &String {
        &self.label
    }

    fn into_thing_type(self) -> ThingType {
        ThingType::RelationType(self)
    }

    async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_relation_type(self.label().clone()).await.map(|res| res.is_none())
    }
}

#[async_trait]
pub trait RelationTypeAPI: ThingTypeAPI + Into<RelationType> {
    async fn create(&self, transaction: &Transaction<'_>) -> Result<Relation> {
        transaction.concept().relation_type_create(self.clone().into()).await
    }

    async fn get_supertype(&self, transaction: &Transaction<'_>) -> Result<RelationType> {
        transaction.concept().relation_type_get_supertype(self.clone().into()).await
    }

    async fn set_supertype(&mut self, transaction: &Transaction<'_>, supertype: RelationType) -> Result {
        transaction.concept().relation_type_set_supertype(self.clone().into(), supertype).await
    }

    fn get_supertypes(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<RelationType>>> {
        transaction.concept().relation_type_get_supertypes(self.clone().into()).map(box_stream)
    }

    fn get_subtypes(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<RelationType>>> {
        transaction.concept().relation_type_get_subtypes(self.clone().into(), Transitivity::Transitive).map(box_stream)
    }

    fn get_instances(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<Relation>>> {
        transaction.concept().relation_type_get_instances(self.clone().into(), Transitivity::Transitive).map(box_stream)
    }

    fn get_relates(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<BoxStream<Result<RoleType>>> {
        transaction.concept().relation_type_get_relates(self.clone().into(), transitivity).map(box_stream)
    }

    async fn get_relates_for_role_label(
        &self,
        transaction: &Transaction<'_>,
        role_label: String,
    ) -> Result<Option<RoleType>> {
        transaction.concept().relation_type_get_relates_for_role_label(self.clone().into(), role_label).await
    }

    async fn get_relates_overridden(
        &self,
        transaction: &Transaction<'_>,
        overridden_role_label: String,
    ) -> Result<Option<RoleType>> {
        transaction.concept().relation_type_get_relates_overridden(self.clone().into(), overridden_role_label).await
    }

    async fn set_relates(
        &mut self,
        transaction: &Transaction<'_>,
        role_label: String,
        overridden_role_label: Option<String>,
    ) -> Result {
        transaction.concept().relation_type_set_relates(self.clone().into(), role_label, overridden_role_label).await
    }

    async fn unset_relates(&mut self, transaction: &Transaction<'_>, role_label: String) -> Result {
        transaction.concept().relation_type_unset_relates(self.clone().into(), role_label).await
    }
}

#[async_trait]
impl RelationTypeAPI for RelationType {}

#[async_trait]
impl ThingTypeAPI for AttributeType {
    fn label(&self) -> &String {
        &self.label
    }

    fn into_thing_type(self) -> ThingType {
        ThingType::AttributeType(self)
    }

    async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_attribute_type(self.label().clone()).await.map(|res| res.is_none())
    }
}

#[async_trait]
pub trait AttributeTypeAPI: ThingTypeAPI + Into<AttributeType> {
    fn value_type(&self) -> ValueType;

    async fn put(&self, transaction: &Transaction<'_>, value: Value) -> Result<Attribute> {
        transaction.concept().attribute_type_put(self.clone().into(), value).await
    }

    async fn get(&self, transaction: &Transaction<'_>, value: Value) -> Result<Option<Attribute>> {
        transaction.concept().attribute_type_get(self.clone().into(), value).await
    }

    async fn get_supertype(&self, transaction: &Transaction<'_>) -> Result<AttributeType> {
        transaction.concept().attribute_type_get_supertype(self.clone().into()).await
    }

    async fn set_supertype(&mut self, transaction: &Transaction<'_>, supertype: AttributeType) -> Result {
        transaction.concept().attribute_type_set_supertype(self.clone().into(), supertype).await
    }

    fn get_supertypes(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<AttributeType>>> {
        transaction.concept().attribute_type_get_supertypes(self.clone().into()).map(box_stream)
    }

    fn get_subtypes(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<AttributeType>>> {
        // FIXME when None?
        transaction
            .concept()
            .attribute_type_get_subtypes(self.clone().into(), Transitivity::Transitive, Some(self.value_type()))
            .map(box_stream)
    }

    fn get_subtypes_with_value_type(
        &self,
        transaction: &Transaction<'_>,
        value_type: ValueType,
    ) -> Result<BoxStream<Result<AttributeType>>> {
        transaction
            .concept()
            .attribute_type_get_subtypes(self.clone().into(), Transitivity::Transitive, Some(value_type))
            .map(box_stream)
    }

    fn get_instances(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<Attribute>>> {
        transaction
            .concept()
            .attribute_type_get_instances(self.clone().into(), Transitivity::Transitive, Some(self.value_type()))
            .map(box_stream)
    }

    async fn get_regex(&self, transaction: &Transaction<'_>) -> Result<String> {
        transaction.concept().attribute_type_get_regex(self.clone().into()).await
    }

    async fn set_regex(&self, transaction: &Transaction<'_>, regex: String) -> Result {
        transaction.concept().attribute_type_set_regex(self.clone().into(), regex).await
    }

    async fn unset_regex(&self, transaction: &Transaction<'_>) -> Result {
        self.set_regex(transaction, String::new()).await
    }

    fn get_owners(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
        annotations: Vec<Annotation>,
    ) -> Result<BoxStream<Result<ThingType>>> {
        transaction.concept().attribute_type_get_owners(self.clone().into(), transitivity, annotations).map(box_stream)
    }
}

#[async_trait]
impl AttributeTypeAPI for AttributeType {
    fn value_type(&self) -> ValueType {
        self.value_type
    }
}

#[async_trait]
pub trait RoleTypeAPI: Clone + Into<RoleType> + Sync + Send {
    async fn delete(&self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().role_type_delete(self.clone().into()).await
    }

    async fn set_label(&self, transaction: &Transaction<'_>, new_label: String) -> Result {
        transaction.concept().role_type_set_label(self.clone().into(), new_label).await
    }

    async fn get_supertype(&self, transaction: &Transaction<'_>) -> Result<RoleType> {
        transaction.concept().role_type_get_supertype(self.clone().into()).await
    }

    fn get_supertypes(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<RoleType>>> {
        transaction.concept().role_type_get_supertypes(self.clone().into()).map(box_stream)
    }

    fn get_subtypes(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<BoxStream<Result<RoleType>>> {
        transaction.concept().role_type_get_subtypes(self.clone().into(), transitivity).map(box_stream)
    }

    fn get_relation_types(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<RelationType>>> {
        transaction.concept().role_type_get_relation_types(self.clone().into()).map(box_stream)
    }

    fn get_player_types(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<BoxStream<Result<ThingType>>> {
        transaction.concept().role_type_get_player_types(self.clone().into(), transitivity).map(box_stream)
    }

    fn get_relation_instances(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<BoxStream<Result<Relation>>> {
        transaction.concept().role_type_get_relation_instances(self.clone().into(), transitivity).map(box_stream)
    }

    fn get_player_instances(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<BoxStream<Result<Thing>>> {
        transaction.concept().role_type_get_player_instances(self.clone().into(), transitivity).map(box_stream)
    }
}

#[async_trait]
impl RoleTypeAPI for RoleType {}
