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

use futures::Stream;
use typeql_lang::pattern::Annotation;

use crate::{
    concept::{Attribute, AttributeType, Entity, Relation, RoleType, Thing, ThingType},
    Result, Transaction,
};

impl Entity {
    pub async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_entity(self.iid.clone()).await.map(|res| res.is_none())
    }

    pub async fn delete(&self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_delete(Thing::Entity(self.clone())).await
    }

    pub fn get_has(
        &self,
        transaction: &Transaction<'_>,
        attribute_types: Vec<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<Attribute>>> {
        transaction.concept().thing_get_has(Thing::Entity(self.clone()), attribute_types, annotations)
    }

    pub async fn set_has(&self, transaction: &Transaction<'_>, attribute: Attribute) -> Result {
        transaction.concept().thing_set_has(Thing::Entity(self.clone()), attribute).await
    }

    pub async fn unset_has(&self, transaction: &Transaction<'_>, attribute: Attribute) -> Result {
        transaction.concept().thing_unset_has(Thing::Entity(self.clone()), attribute).await
    }

    pub fn get_relations(
        &self,
        transaction: &Transaction<'_>,
        role_types: Vec<RoleType>,
    ) -> Result<impl Stream<Item = Result<Relation>>> {
        transaction.concept().thing_get_relations(Thing::Entity(self.clone()), role_types)
    }

    pub fn get_playing(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<RoleType>>> {
        transaction.concept().thing_get_playing(Thing::Entity(self.clone()))
    }
}

impl Relation {
    pub async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_relation(self.iid.clone()).await.map(|res| res.is_none())
    }

    pub async fn delete(&self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_delete(Thing::Relation(self.clone())).await
    }

    pub fn get_has(
        &self,
        transaction: &Transaction<'_>,
        attribute_types: Vec<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<Attribute>>> {
        transaction.concept().thing_get_has(Thing::Relation(self.clone()), attribute_types, annotations)
    }

    pub async fn set_has(&self, transaction: &Transaction<'_>, attribute: Attribute) -> Result {
        transaction.concept().thing_set_has(Thing::Relation(self.clone()), attribute).await
    }

    pub async fn unset_has(&self, transaction: &Transaction<'_>, attribute: Attribute) -> Result {
        transaction.concept().thing_unset_has(Thing::Relation(self.clone()), attribute).await
    }

    pub fn get_relations(
        &self,
        transaction: &Transaction<'_>,
        role_types: Vec<RoleType>,
    ) -> Result<impl Stream<Item = Result<Relation>>> {
        transaction.concept().thing_get_relations(Thing::Relation(self.clone()), role_types)
    }

    pub fn get_playing(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<RoleType>>> {
        transaction.concept().thing_get_playing(Thing::Relation(self.clone()))
    }

    pub async fn add_role_player(&self, transaction: &Transaction<'_>, role_type: RoleType, player: Thing) -> Result {
        transaction.concept().relation_add_role_player(self.clone(), role_type, player).await
    }

    pub async fn remove_role_player(
        &self,
        transaction: &Transaction<'_>,
        role_type: RoleType,
        player: Thing,
    ) -> Result {
        transaction.concept().relation_remove_role_player(self.clone(), role_type, player).await
    }

    pub fn get_players_by_role_type(
        &self,
        transaction: &Transaction<'_>,
        role_types: Vec<RoleType>,
    ) -> Result<impl Stream<Item = Result<Thing>>> {
        transaction.concept().relation_get_players_by_role_type(self.clone(), role_types)
    }

    pub fn get_role_players(
        &self,
        transaction: &Transaction<'_>,
    ) -> Result<impl Stream<Item = Result<(RoleType, Thing)>>> {
        transaction.concept().relation_get_role_players(self.clone())
    }

    pub fn get_relating(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<RoleType>>> {
        transaction.concept().relation_get_relating(self.clone())
    }
}

impl Attribute {
    pub async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_attribute(self.iid.clone()).await.map(|res| res.is_none())
    }

    pub async fn delete(&self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_delete(Thing::Attribute(self.clone())).await
    }

    pub fn get_has(
        &self,
        transaction: &Transaction<'_>,
        attribute_types: Vec<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result<impl Stream<Item = Result<Attribute>>> {
        transaction.concept().thing_get_has(Thing::Attribute(self.clone()), attribute_types, annotations)
    }

    pub async fn set_has(&self, transaction: &Transaction<'_>, attribute: Attribute) -> Result {
        transaction.concept().thing_set_has(Thing::Attribute(self.clone()), attribute).await
    }

    pub async fn unset_has(&self, transaction: &Transaction<'_>, attribute: Attribute) -> Result {
        transaction.concept().thing_unset_has(Thing::Attribute(self.clone()), attribute).await
    }

    pub fn get_relations(
        &self,
        transaction: &Transaction<'_>,
        role_types: Vec<RoleType>,
    ) -> Result<impl Stream<Item = Result<Relation>>> {
        transaction.concept().thing_get_relations(Thing::Attribute(self.clone()), role_types)
    }

    pub fn get_playing(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<RoleType>>> {
        transaction.concept().thing_get_playing(Thing::Attribute(self.clone()))
    }

    pub fn get_owners(
        &self,
        transaction: &Transaction<'_>,
        thing_type: Option<ThingType>,
    ) -> Result<impl Stream<Item = Result<Thing>>> {
        transaction.concept().attribute_get_owners(self.clone(), thing_type)
    }
}
