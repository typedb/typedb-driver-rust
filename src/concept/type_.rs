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

use std::fmt;

use futures::Stream;

use super::{Attribute, Entity, Relation, Thing, Value};
use crate::{common::Transitivity, Annotation, Result, Transaction};

#[derive(Clone, Debug)]
pub enum ThingType {
    RootThingType(RootThingType),
    EntityType(EntityType),
    RelationType(RelationType),
    AttributeType(AttributeType),
}

impl ThingType {
    pub fn label(&self) -> &str {
        match self {
            Self::RootThingType(_) => RootThingType::LABEL,
            Self::EntityType(entity_type) => &entity_type.label,
            Self::RelationType(relation_type) => &relation_type.label,
            Self::AttributeType(attribute_type) => &attribute_type.label,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RootThingType;

impl RootThingType {
    const LABEL: &'static str = "thing";

    pub fn new() -> Self {
        Self
    }
}

#[derive(Clone, Debug)]
pub struct EntityType {
    pub label: String,
    pub is_root: bool,
    pub is_abstract: bool,
}

impl EntityType {
    pub fn new(label: String, is_root: bool, is_abstract: bool) -> Self {
        Self { label, is_root, is_abstract }
    }

    pub async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_entity_type(self.label.clone()).await.map(|res| res.is_some())
    }

    pub async fn delete(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_delete(ThingType::EntityType(self.clone())).await
    }

    pub async fn set_label(&mut self, transaction: &Transaction<'_>, new_label: String) -> Result {
        transaction.concept().thing_type_set_label(ThingType::EntityType(self.clone()), new_label).await
    }

    pub async fn set_abstract(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_set_abstract(ThingType::EntityType(self.clone())).await
    }

    pub async fn unset_abstract(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_unset_abstract(ThingType::EntityType(self.clone())).await
    }

    pub fn get_owns(
        &self,
        transaction: &Transaction<'_>,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotation_filter: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        transaction.concept().thing_type_get_owns(
            ThingType::EntityType(self.clone()),
            value_type,
            transitivity,
            annotation_filter,
        )
    }

    pub async fn get_owns_overridden(
        &self,
        transaction: &Transaction<'_>,
        overridden_attribute_type: AttributeType,
    ) -> Result<Option<AttributeType>> {
        transaction
            .concept()
            .thing_type_get_owns_overridden(ThingType::EntityType(self.clone()), overridden_attribute_type)
            .await
    }

    pub async fn set_owns(
        &mut self,
        transaction: &Transaction<'_>,
        attribute_type: AttributeType,
        overridden_attribute_type: Option<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result {
        transaction
            .concept()
            .thing_type_set_owns(
                ThingType::EntityType(self.clone()),
                attribute_type,
                overridden_attribute_type,
                annotations,
            )
            .await
    }

    pub async fn unset_owns(&mut self, transaction: &Transaction<'_>, attribute_type: AttributeType) -> Result {
        transaction.concept().thing_type_unset_owns(ThingType::EntityType(self.clone()), attribute_type).await
    }

    pub fn get_plays(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<RoleType>>> {
        transaction.concept().thing_type_get_plays(ThingType::EntityType(self.clone()), transitivity)
    }

    pub async fn get_plays_overridden(
        &self,
        transaction: &Transaction<'_>,
        overridden_role_type: RoleType,
    ) -> Result<Option<RoleType>> {
        transaction
            .concept()
            .thing_type_get_plays_overridden(ThingType::EntityType(self.clone()), overridden_role_type)
            .await
    }

    pub async fn set_plays(
        &mut self,
        transaction: &Transaction<'_>,
        role_type: RoleType,
        overridden_role_type: Option<RoleType>,
    ) -> Result {
        transaction
            .concept()
            .thing_type_set_plays(ThingType::EntityType(self.clone()), role_type, overridden_role_type)
            .await
    }

    pub async fn unset_plays(&mut self, transaction: &Transaction<'_>, role_type: RoleType) -> Result {
        transaction.concept().thing_type_unset_plays(ThingType::EntityType(self.clone()), role_type).await
    }

    pub async fn get_syntax(&mut self, transaction: &Transaction<'_>) -> Result<String> {
        transaction.concept().thing_type_get_syntax(ThingType::EntityType(self.clone())).await
    }

    pub async fn create(&self, transaction: &Transaction<'_>) -> Result<Entity> {
        transaction.concept().entity_type_create(self.clone()).await
    }

    pub async fn get_supertype(&self, transaction: &Transaction<'_>) -> Result<Self> {
        transaction.concept().entity_type_get_supertype(self.clone()).await
    }

    pub async fn set_supertype(&mut self, transaction: &Transaction<'_>, supertype: EntityType) -> Result {
        transaction.concept().entity_type_set_supertype(self.clone(), supertype).await
    }

    pub fn get_supertypes(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<Self>>> {
        transaction.concept().entity_type_get_supertypes(self.clone())
    }

    pub fn get_subtypes(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<Self>>> {
        transaction.concept().entity_type_get_subtypes(self.clone(), Transitivity::Transitive)
    }

    pub fn get_instances(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<Entity>>> {
        transaction.concept().entity_type_get_instances(self.clone(), Transitivity::Transitive)
    }
}

#[derive(Clone, Debug)]
pub struct RelationType {
    pub label: String,
    pub is_root: bool,
    pub is_abstract: bool,
}

impl RelationType {
    pub fn new(label: String, is_root: bool, is_abstract: bool) -> Self {
        Self { label, is_root, is_abstract }
    }

    pub async fn delete(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_delete(ThingType::RelationType(self.clone())).await
    }

    pub async fn set_label(&mut self, transaction: &Transaction<'_>, new_label: String) -> Result {
        transaction.concept().thing_type_set_label(ThingType::RelationType(self.clone()), new_label).await
    }

    pub async fn set_abstract(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_set_abstract(ThingType::RelationType(self.clone())).await
    }

    pub async fn unset_abstract(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_unset_abstract(ThingType::RelationType(self.clone())).await
    }

    pub fn get_owns(
        &self,
        transaction: &Transaction<'_>,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotation_filter: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        transaction.concept().thing_type_get_owns(
            ThingType::RelationType(self.clone()),
            value_type,
            transitivity,
            annotation_filter,
        )
    }

    pub async fn get_owns_overridden(
        &self,
        transaction: &Transaction<'_>,
        overridden_attribute_type: AttributeType,
    ) -> Result<Option<AttributeType>> {
        transaction
            .concept()
            .thing_type_get_owns_overridden(ThingType::RelationType(self.clone()), overridden_attribute_type)
            .await
    }

    pub async fn set_owns(
        &mut self,
        transaction: &Transaction<'_>,
        attribute_type: AttributeType,
        overridden_attribute_type: Option<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result {
        transaction
            .concept()
            .thing_type_set_owns(
                ThingType::RelationType(self.clone()),
                attribute_type,
                overridden_attribute_type,
                annotations,
            )
            .await
    }

    pub async fn unset_owns(&mut self, transaction: &Transaction<'_>, attribute_type: AttributeType) -> Result {
        transaction.concept().thing_type_unset_owns(ThingType::RelationType(self.clone()), attribute_type).await
    }

    pub fn get_plays(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<RoleType>>> {
        transaction.concept().thing_type_get_plays(ThingType::RelationType(self.clone()), transitivity)
    }

    pub async fn get_plays_overridden(
        &self,
        transaction: &Transaction<'_>,
        overridden_role_type: RoleType,
    ) -> Result<Option<RoleType>> {
        transaction
            .concept()
            .thing_type_get_plays_overridden(ThingType::RelationType(self.clone()), overridden_role_type)
            .await
    }

    pub async fn set_plays(
        &mut self,
        transaction: &Transaction<'_>,
        role_type: RoleType,
        overridden_role_type: Option<RoleType>,
    ) -> Result {
        transaction
            .concept()
            .thing_type_set_plays(ThingType::RelationType(self.clone()), role_type, overridden_role_type)
            .await
    }

    pub async fn unset_plays(&mut self, transaction: &Transaction<'_>, role_type: RoleType) -> Result {
        transaction.concept().thing_type_unset_plays(ThingType::RelationType(self.clone()), role_type).await
    }

    pub async fn get_syntax(&mut self, transaction: &Transaction<'_>) -> Result<String> {
        transaction.concept().thing_type_get_syntax(ThingType::RelationType(self.clone())).await
    }

    pub async fn create(&self, transaction: &Transaction<'_>) -> Result<Relation> {
        transaction.concept().relation_type_create(self.clone()).await
    }

    pub async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_relation_type(self.label.clone()).await.map(|res| res.is_some())
    }

    pub async fn get_supertype(&self, transaction: &Transaction<'_>) -> Result<Self> {
        transaction.concept().relation_type_get_supertype(self.clone()).await
    }

    pub async fn set_supertype(&mut self, transaction: &Transaction<'_>, supertype: RelationType) -> Result {
        transaction.concept().relation_type_set_supertype(self.clone(), supertype).await
    }

    pub fn get_supertypes(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<Self>>> {
        transaction.concept().relation_type_get_supertypes(self.clone())
    }

    pub fn get_subtypes(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<Self>>> {
        transaction.concept().relation_type_get_subtypes(self.clone(), Transitivity::Transitive)
    }

    pub fn get_instances(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<Relation>>> {
        transaction.concept().relation_type_get_instances(self.clone(), Transitivity::Transitive)
    }

    pub fn get_relates(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<RoleType>>> {
        transaction.concept().relation_type_get_relates(self.clone(), transitivity)
    }

    pub async fn get_relates_for_role_label(
        &self,
        transaction: &Transaction<'_>,
        role_label: String,
    ) -> Result<Option<RoleType>> {
        transaction.concept().relation_type_get_relates_for_role_label(self.clone(), role_label).await
    }

    pub async fn get_relates_overridden(
        &self,
        transaction: &Transaction<'_>,
        overridden_role_label: String,
    ) -> Result<Option<RoleType>> {
        transaction.concept().relation_type_get_relates_overridden(self.clone(), overridden_role_label).await
    }

    pub async fn set_relates(
        &mut self,
        transaction: &Transaction<'_>,
        role_label: String,
        overridden_role_label: Option<String>,
    ) -> Result {
        transaction.concept().relation_type_set_relates(self.clone(), role_label, overridden_role_label).await
    }

    pub async fn unset_relates(&mut self, transaction: &Transaction<'_>, role_label: String) -> Result {
        transaction.concept().relation_type_unset_relates(self.clone(), role_label).await
    }
}

#[derive(Clone, Debug)]
pub struct AttributeType {
    pub label: String,
    pub is_root: bool,
    pub is_abstract: bool,
    pub value_type: ValueType,
}

impl AttributeType {
    pub fn new(label: String, is_root: bool, is_abstract: bool, value_type: ValueType) -> Self {
        Self { label, is_root, is_abstract, value_type }
    }

    pub async fn delete(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_delete(ThingType::AttributeType(self.clone())).await
    }

    pub async fn set_label(&mut self, transaction: &Transaction<'_>, new_label: String) -> Result {
        transaction.concept().thing_type_set_label(ThingType::AttributeType(self.clone()), new_label).await
    }

    pub async fn set_abstract(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_set_abstract(ThingType::AttributeType(self.clone())).await
    }

    pub async fn unset_abstract(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_type_unset_abstract(ThingType::AttributeType(self.clone())).await
    }

    pub fn get_owns(
        &self,
        transaction: &Transaction<'_>,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotation_filter: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        transaction.concept().thing_type_get_owns(
            ThingType::AttributeType(self.clone()),
            value_type,
            transitivity,
            annotation_filter,
        )
    }

    pub async fn get_owns_overridden(
        &self,
        transaction: &Transaction<'_>,
        overridden_attribute_type: AttributeType,
    ) -> Result<Option<AttributeType>> {
        transaction
            .concept()
            .thing_type_get_owns_overridden(ThingType::AttributeType(self.clone()), overridden_attribute_type)
            .await
    }

    pub async fn set_owns(
        &mut self,
        transaction: &Transaction<'_>,
        attribute_type: AttributeType,
        overridden_attribute_type: Option<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result {
        transaction
            .concept()
            .thing_type_set_owns(
                ThingType::AttributeType(self.clone()),
                attribute_type,
                overridden_attribute_type,
                annotations,
            )
            .await
    }

    pub async fn unset_owns(&mut self, transaction: &Transaction<'_>, attribute_type: AttributeType) -> Result {
        transaction.concept().thing_type_unset_owns(ThingType::AttributeType(self.clone()), attribute_type).await
    }

    pub fn get_plays(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<RoleType>>> {
        transaction.concept().thing_type_get_plays(ThingType::AttributeType(self.clone()), transitivity)
    }

    pub async fn get_plays_overridden(
        &self,
        transaction: &Transaction<'_>,
        overridden_role_type: RoleType,
    ) -> Result<Option<RoleType>> {
        transaction
            .concept()
            .thing_type_get_plays_overridden(ThingType::AttributeType(self.clone()), overridden_role_type)
            .await
    }

    pub async fn set_plays(
        &mut self,
        transaction: &Transaction<'_>,
        role_type: RoleType,
        overridden_role_type: Option<RoleType>,
    ) -> Result {
        transaction
            .concept()
            .thing_type_set_plays(ThingType::AttributeType(self.clone()), role_type, overridden_role_type)
            .await
    }

    pub async fn unset_plays(&mut self, transaction: &Transaction<'_>, role_type: RoleType) -> Result {
        transaction.concept().thing_type_unset_plays(ThingType::AttributeType(self.clone()), role_type).await
    }

    pub async fn get_syntax(&mut self, transaction: &Transaction<'_>) -> Result<String> {
        transaction.concept().thing_type_get_syntax(ThingType::AttributeType(self.clone())).await
    }

    pub async fn put(&self, transaction: &Transaction<'_>, value: Value) -> Result<Attribute> {
        transaction.concept().attribute_type_put(self.clone(), value).await
    }

    pub async fn get(&self, transaction: &Transaction<'_>, value: Value) -> Result<Option<Attribute>> {
        transaction.concept().attribute_type_get(self.clone(), value).await
    }

    pub async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_attribute_type(self.label.clone()).await.map(|res| res.is_some())
    }

    pub async fn get_supertype(&self, transaction: &Transaction<'_>) -> Result<Self> {
        transaction.concept().attribute_type_get_supertype(self.clone()).await
    }

    pub async fn set_supertype(&mut self, transaction: &Transaction<'_>, supertype: AttributeType) -> Result {
        transaction.concept().attribute_type_set_supertype(self.clone(), supertype).await
    }

    pub fn get_supertypes(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<Self>>> {
        transaction.concept().attribute_type_get_supertypes(self.clone())
    }

    pub fn get_subtypes(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<Self>>> {
        // FIXME when None?
        transaction.concept().attribute_type_get_subtypes(self.clone(), Transitivity::Transitive, Some(self.value_type))
    }

    pub fn get_instances(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<Attribute>>> {
        transaction.concept().attribute_type_get_instances(
            self.clone(),
            Transitivity::Transitive,
            Some(self.value_type),
        )
    }

    pub async fn get_regex(&self, transaction: &Transaction<'_>) -> Result<String> {
        transaction.concept().attribute_type_get_regex(self.clone()).await
    }

    pub async fn set_regex(&self, transaction: &Transaction<'_>, regex: String) -> Result {
        transaction.concept().attribute_type_set_regex(self.clone(), regex).await
    }

    pub fn get_owners(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
        annotations: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<ThingType>>> {
        transaction.concept().attribute_type_get_owners(self.clone(), transitivity, annotations)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ValueType {
    Object,
    Boolean,
    Long,
    Double,
    String,
    DateTime,
}

#[derive(Clone, Debug)]
pub struct RoleType {
    pub label: ScopedLabel,
    pub is_root: bool,
    pub is_abstract: bool,
}

impl RoleType {
    pub fn new(label: ScopedLabel, is_root: bool, is_abstract: bool) -> Self {
        Self { label, is_root, is_abstract }
    }

    pub async fn delete(&self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().role_type_delete(self.clone()).await
    }

    pub async fn set_label(&self, transaction: &Transaction<'_>, new_label: String) -> Result {
        transaction.concept().role_type_set_label(self.clone(), new_label).await
    }

    pub async fn get_supertype(&self, transaction: &Transaction<'_>) -> Result<RoleType> {
        transaction.concept().role_type_get_supertype(self.clone()).await
    }

    pub fn get_supertypes(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<RoleType>>> {
        transaction.concept().role_type_get_supertypes(self.clone())
    }

    pub fn get_subtypes(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<RoleType>>> {
        transaction.concept().role_type_get_subtypes(self.clone(), transitivity)
    }

    pub fn get_relation_types(
        &self,
        transaction: &Transaction<'_>,
    ) -> Result<impl Stream<Item = Result<RelationType>>> {
        transaction.concept().role_type_get_relation_types(self.clone())
    }

    pub fn get_player_types(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<ThingType>>> {
        transaction.concept().role_type_get_player_types(self.clone(), transitivity)
    }

    pub fn get_relation_instances(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<Relation>>> {
        transaction.concept().role_type_get_relation_instances(self.clone(), transitivity)
    }

    pub fn get_player_instances(
        &self,
        transaction: &Transaction<'_>,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<Thing>>> {
        transaction.concept().role_type_get_player_instances(self.clone(), transitivity)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedLabel {
    pub scope: String,
    pub name: String,
}

impl ScopedLabel {
    pub fn new(scope: String, name: String) -> Self {
        Self { scope, name }
    }
}

impl fmt::Display for ScopedLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.scope, self.name)
    }
}
