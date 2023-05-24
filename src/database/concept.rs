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
        Attribute, AttributeType, Entity, EntityType, Relation, RelationType, RoleType, Thing, ThingType, Value,
        ValueType,
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

    pub(crate) async fn thing_type_delete(&self, thing_type: ThingType) -> Result {
        self.transaction_stream.thing_type_delete(thing_type).await
    }

    pub(crate) async fn thing_type_set_label(&self, thing_type: ThingType, new_label: String) -> Result {
        self.transaction_stream.thing_type_set_label(thing_type, new_label).await
    }

    pub(crate) async fn thing_type_set_abstract(&self, thing_type: ThingType) -> Result {
        self.transaction_stream.thing_type_set_abstract(thing_type).await
    }

    pub(crate) async fn thing_type_unset_abstract(&self, thing_type: ThingType) -> Result {
        self.transaction_stream.thing_type_unset_abstract(thing_type).await
    }

    pub(crate) fn thing_type_get_owns(
        &self,
        thing_type: ThingType,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotations: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<AttributeType>>> {
        self.transaction_stream.thing_type_get_owns(thing_type, value_type, transitivity, annotations)
    }

    pub(crate) async fn thing_type_get_owns_overridden(
        &self,
        thing_type: ThingType,
        overridden_attribute_type: AttributeType,
    ) -> Result<Option<AttributeType>> {
        self.transaction_stream.thing_type_get_owns_overridden(thing_type, overridden_attribute_type).await
    }

    pub(crate) async fn thing_type_set_owns(
        &self,
        thing_type: ThingType,
        attribute_type: AttributeType,
        overridden_attribute_type: Option<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result {
        self.transaction_stream
            .thing_type_set_owns(thing_type, attribute_type, overridden_attribute_type, annotations)
            .await
    }

    pub(crate) async fn thing_type_unset_owns(&self, thing_type: ThingType, attribute_type: AttributeType) -> Result {
        self.transaction_stream.thing_type_unset_owns(thing_type, attribute_type).await
    }

    pub(crate) fn thing_type_get_plays(
        &self,
        thing_type: ThingType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<RoleType>>> {
        self.transaction_stream.thing_type_get_plays(thing_type, transitivity)
    }

    pub(crate) async fn thing_type_get_plays_overridden(
        &self,
        thing_type: ThingType,
        overridden_role_type: RoleType,
    ) -> Result<Option<RoleType>> {
        self.transaction_stream.thing_type_get_plays_overridden(thing_type, overridden_role_type).await
    }

    pub(crate) async fn thing_type_set_plays(
        &self,
        thing_type: ThingType,
        role_type: RoleType,
        overridden_role_type: Option<RoleType>,
    ) -> Result {
        self.transaction_stream.thing_type_set_plays(thing_type, role_type, overridden_role_type).await
    }

    pub(crate) async fn thing_type_unset_plays(&self, thing_type: ThingType, role_type: RoleType) -> Result {
        self.transaction_stream.thing_type_unset_plays(thing_type, role_type).await
    }

    pub(crate) async fn thing_type_get_syntax(&self, thing_type: ThingType) -> Result<String> {
        self.transaction_stream.thing_type_get_syntax(thing_type).await
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

    pub(crate) fn relation_type_get_relates(
        &self,
        relation_type: RelationType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<RoleType>>> {
        self.transaction_stream.relation_type_get_relates(relation_type, transitivity)
    }

    pub(crate) async fn relation_type_get_relates_for_role_label(
        &self,
        relation_type: RelationType,
        role_label: String,
    ) -> Result<Option<RoleType>> {
        self.transaction_stream.relation_type_get_relates_for_role_label(relation_type, role_label).await
    }

    pub(crate) async fn relation_type_get_relates_overridden(
        &self,
        relation_type: RelationType,
        overridden_role_label: String,
    ) -> Result<Option<RoleType>> {
        self.transaction_stream.relation_type_get_relates_overridden(relation_type, overridden_role_label).await
    }

    pub(crate) async fn relation_type_set_relates(
        &self,
        relation_type: RelationType,
        role_label: String,
        overridden_role_label: Option<String>,
    ) -> Result {
        self.transaction_stream.relation_type_set_relates(relation_type, role_label, overridden_role_label).await
    }

    pub(crate) async fn relation_type_unset_relates(&self, relation_type: RelationType, role_label: String) -> Result {
        self.transaction_stream.relation_type_unset_relates(relation_type, role_label).await
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

    pub(crate) async fn attribute_type_get_regex(&self, attribute_type: AttributeType) -> Result<String> {
        self.transaction_stream.attribute_type_get_regex(attribute_type).await
    }

    pub(crate) async fn attribute_type_set_regex(&self, attribute_type: AttributeType, regex: String) -> Result {
        self.transaction_stream.attribute_type_set_regex(attribute_type, regex).await
    }

    pub(crate) fn attribute_type_get_owners(
        &self,
        attribute_type: AttributeType,
        transitivity: Transitivity,
        annotations: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<ThingType>>> {
        self.transaction_stream.attribute_type_get_owners(attribute_type, transitivity, annotations)
    }

    pub(crate) async fn role_type_delete(&self, role_type: RoleType) -> Result {
        self.transaction_stream.role_type_delete(role_type).await
    }

    pub(crate) async fn role_type_set_label(&self, role_type: RoleType, new_label: String) -> Result {
        self.transaction_stream.role_type_set_label(role_type, new_label).await
    }

    pub(crate) async fn role_type_get_supertype(&self, role_type: RoleType) -> Result<RoleType> {
        self.transaction_stream.role_type_get_supertype(role_type).await
    }

    pub(crate) fn role_type_get_supertypes(&self, role_type: RoleType) -> Result<impl Stream<Item = Result<RoleType>>> {
        self.transaction_stream.role_type_get_supertypes(role_type)
    }

    pub(crate) fn role_type_get_subtypes(
        &self,
        role_type: RoleType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<RoleType>>> {
        self.transaction_stream.role_type_get_subtypes(role_type, transitivity)
    }

    pub(crate) fn role_type_get_relation_types(
        &self,
        role_type: RoleType,
    ) -> Result<impl Stream<Item = Result<RelationType>>> {
        self.transaction_stream.role_type_get_relation_types(role_type)
    }

    pub(crate) fn role_type_get_player_types(
        &self,
        role_type: RoleType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<ThingType>>> {
        self.transaction_stream.role_type_get_player_types(role_type, transitivity)
    }

    pub(crate) fn role_type_get_relation_instances(
        &self,
        role_type: RoleType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<Relation>>> {
        self.transaction_stream.role_type_get_relation_instances(role_type, transitivity)
    }

    pub(crate) fn role_type_get_player_instances(
        &self,
        role_type: RoleType,
        transitivity: Transitivity,
    ) -> Result<impl Stream<Item = Result<Thing>>> {
        self.transaction_stream.role_type_get_player_instances(role_type, transitivity)
    }

    pub(crate) async fn thing_delete(&self, thing: Thing) -> Result {
        self.transaction_stream.thing_delete(thing).await
    }

    pub(crate) fn thing_get_has(
        &self,
        thing: Thing,
        attribute_types: Vec<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<Attribute>>> {
        self.transaction_stream.thing_get_has(thing, attribute_types, annotations)
    }

    pub(crate) async fn thing_set_has(&self, thing: Thing, attribute: Attribute) -> Result {
        self.transaction_stream.thing_set_has(thing, attribute).await
    }

    pub(crate) async fn thing_unset_has(&self, thing: Thing, attribute: Attribute) -> Result {
        self.transaction_stream.thing_unset_has(thing, attribute).await
    }

    pub(crate) fn thing_get_relations(
        &self,
        thing: Thing,
        role_types: Vec<RoleType>,
    ) -> Result<impl Stream<Item = Result<Relation>>> {
        self.transaction_stream.thing_get_relations(thing, role_types)
    }

    pub(crate) fn thing_get_playing(&self, thing: Thing) -> Result<impl Stream<Item = Result<RoleType>>> {
        self.transaction_stream.thing_get_playing(thing)
    }

    pub(crate) async fn relation_add_role_player(
        &self,
        relation: Relation,
        role_type: RoleType,
        player: Thing,
    ) -> Result {
        self.transaction_stream.relation_add_role_player(relation, role_type, player).await
    }

    pub(crate) async fn relation_remove_role_player(
        &self,
        relation: Relation,
        role_type: RoleType,
        player: Thing,
    ) -> Result {
        self.transaction_stream.relation_remove_role_player(relation, role_type, player).await
    }

    pub(crate) fn relation_get_players_by_role_type(
        &self,
        relation: Relation,
        role_types: Vec<RoleType>,
    ) -> Result<impl Stream<Item = Result<Thing>>> {
        self.transaction_stream.relation_get_players_by_role_type(relation, role_types)
    }

    pub(crate) fn relation_get_role_players(
        &self,
        relation: Relation,
    ) -> Result<impl Stream<Item = Result<(RoleType, Thing)>>> {
        self.transaction_stream.relation_get_role_players(relation)
    }

    pub(crate) fn relation_get_relating(&self, relation: Relation) -> Result<impl Stream<Item = Result<RoleType>>> {
        self.transaction_stream.relation_get_relating(relation)
    }

    pub(crate) fn attribute_get_owners(
        &self,
        attribute: Attribute,
        thing_type: Option<ThingType>,
    ) -> Result<impl Stream<Item = Result<Thing>>> {
        self.transaction_stream.attribute_get_owners(attribute, thing_type)
    }
}
