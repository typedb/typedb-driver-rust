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
use typeql_lang::pattern::Annotation;

use crate::{
    common::{box_stream, IID},
    concept::{Attribute, AttributeType, Entity, Relation, RoleType, Thing, ThingType},
    Result, Transaction,
};

#[async_trait]
pub trait ThingAPI: Clone + Sync + Send {
    fn iid(&self) -> &IID;

    fn into_thing(self) -> Thing;

    async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool>;

    async fn delete(&self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().thing_delete(self.clone().into_thing()).await
    }

    fn get_has(
        &self,
        transaction: &Transaction<'_>,
        attribute_types: Vec<AttributeType>,
        annotations: Vec<Annotation>,
    ) -> Result<BoxStream<Result<Attribute>>> {
        transaction.concept().thing_get_has(self.clone().into_thing(), attribute_types, annotations).map(box_stream)
    }

    async fn set_has(&self, transaction: &Transaction<'_>, attribute: Attribute) -> Result {
        transaction.concept().thing_set_has(self.clone().into_thing(), attribute).await
    }

    async fn unset_has(&self, transaction: &Transaction<'_>, attribute: Attribute) -> Result {
        transaction.concept().thing_unset_has(self.clone().into_thing(), attribute).await
    }

    fn get_relations(
        &self,
        transaction: &Transaction<'_>,
        role_types: Vec<RoleType>,
    ) -> Result<BoxStream<Result<Relation>>> {
        transaction.concept().thing_get_relations(self.clone().into_thing(), role_types).map(box_stream)
    }

    fn get_playing(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<RoleType>>> {
        transaction.concept().thing_get_playing(self.clone().into_thing()).map(box_stream)
    }
}

#[async_trait]
impl ThingAPI for Entity {
    fn iid(&self) -> &IID {
        &self.iid
    }

    fn into_thing(self) -> Thing {
        Thing::Entity(self)
    }

    async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_entity(self.iid().clone()).await.map(|res| res.is_none())
    }
}

#[async_trait]
pub trait EntityAPI: ThingAPI + Into<Entity> {}

#[async_trait]
impl EntityAPI for Entity {}

#[async_trait]
impl ThingAPI for Relation {
    fn iid(&self) -> &IID {
        &self.iid
    }

    fn into_thing(self) -> Thing {
        Thing::Relation(self)
    }

    async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_relation(self.iid().clone()).await.map(|res| res.is_none())
    }
}

#[async_trait]
pub trait RelationAPI: ThingAPI + Into<Relation> {
    async fn add_role_player(&self, transaction: &Transaction<'_>, role_type: RoleType, player: Thing) -> Result {
        transaction.concept().relation_add_role_player(self.clone().into(), role_type, player).await
    }

    async fn remove_role_player(&self, transaction: &Transaction<'_>, role_type: RoleType, player: Thing) -> Result {
        transaction.concept().relation_remove_role_player(self.clone().into(), role_type, player).await
    }

    fn get_players_by_role_type(
        &self,
        transaction: &Transaction<'_>,
        role_types: Vec<RoleType>,
    ) -> Result<BoxStream<Result<Thing>>> {
        transaction.concept().relation_get_players_by_role_type(self.clone().into(), role_types).map(box_stream)
    }

    fn get_role_players(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<(RoleType, Thing)>>> {
        transaction.concept().relation_get_role_players(self.clone().into()).map(box_stream)
    }

    fn get_relating(&self, transaction: &Transaction<'_>) -> Result<BoxStream<Result<RoleType>>> {
        transaction.concept().relation_get_relating(self.clone().into()).map(box_stream)
    }
}

#[async_trait]
impl RelationAPI for Relation {}

#[async_trait]
impl ThingAPI for Attribute {
    fn iid(&self) -> &IID {
        &self.iid
    }

    fn into_thing(self) -> Thing {
        Thing::Attribute(self)
    }

    async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_attribute(self.iid().clone()).await.map(|res| res.is_none())
    }
}

#[async_trait]
pub trait AttributeAPI: ThingAPI + Into<Attribute> {
    fn get_owners(
        &self,
        transaction: &Transaction<'_>,
        thing_type: Option<ThingType>,
    ) -> Result<BoxStream<Result<Thing>>> {
        transaction.concept().attribute_get_owners(self.clone().into(), thing_type).map(box_stream)
    }
}

#[async_trait]
impl AttributeAPI for Attribute {}
