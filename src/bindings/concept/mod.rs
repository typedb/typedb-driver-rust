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

mod manager;
mod thing;
mod type_;

use std::ptr::addr_of_mut;

use itertools::Itertools;

use super::{
    common::{borrow, free, release},
    iterator::iterator_try_next,
};
use crate::{
    bindings::iterator::CIterator,
    common::{box_stream, stream::BoxStream},
    concept::{Attribute, Concept, Relation, RoleType, Thing},
    Result,
};

#[no_mangle]
pub extern "C" fn concept_drop(concept: *mut Concept) {
    free(concept);
}

#[no_mangle]
pub extern "C" fn concept_is_entity(concept: *const Concept) -> bool {
    matches!(borrow(concept), Concept::Entity(_))
}

#[no_mangle]
pub extern "C" fn concept_is_relation(concept: *const Concept) -> bool {
    matches!(borrow(concept), Concept::Relation(_))
}

#[no_mangle]
pub extern "C" fn concept_is_attribute(concept: *const Concept) -> bool {
    matches!(borrow(concept), Concept::Attribute(_))
}

#[no_mangle]
pub extern "C" fn concept_is_entity_type(concept: *const Concept) -> bool {
    matches!(borrow(concept), Concept::EntityType(_))
}

#[no_mangle]
pub extern "C" fn concept_is_relation_type(concept: *const Concept) -> bool {
    matches!(borrow(concept), Concept::RelationType(_))
}

#[no_mangle]
pub extern "C" fn concept_is_attribute_type(concept: *const Concept) -> bool {
    matches!(borrow(concept), Concept::AttributeType(_))
}

#[no_mangle]
pub extern "C" fn concept_is_role_type(concept: *const Concept) -> bool {
    matches!(borrow(concept), Concept::RoleType(_))
}

type ConceptIteratorInner = CIterator<Result<Concept>, BoxStream<'static, Result<Concept>>>;

pub struct ConceptIterator(ConceptIteratorInner);

impl ConceptIterator {
    fn things(it: BoxStream<'static, Result<Thing>>) -> Self {
        Self(CIterator(box_stream(it.map_ok(|thing| match thing {
            Thing::Entity(entity) => Concept::Entity(entity),
            Thing::Relation(relation) => Concept::Relation(relation),
            Thing::Attribute(attribute) => Concept::Attribute(attribute),
        }))))
    }

    fn relations(it: BoxStream<'static, Result<Relation>>) -> Self {
        Self(CIterator(box_stream(it.map_ok(Concept::Relation))))
    }

    fn attributes(it: BoxStream<'static, Result<Attribute>>) -> Self {
        Self(CIterator(box_stream(it.map_ok(Concept::Attribute))))
    }

    fn role_types(it: BoxStream<'static, Result<RoleType>>) -> Self {
        Self(CIterator(box_stream(it.map_ok(Concept::RoleType))))
    }
}

#[no_mangle]
pub extern "C" fn concept_iterator_next(it: *mut ConceptIterator) -> *mut Concept {
    unsafe { iterator_try_next(addr_of_mut!((*it).0)) }
}

#[no_mangle]
pub extern "C" fn concept_iterator_drop(it: *mut ConceptIterator) {
    free(it);
}

type RolePlayerIteratorInner = CIterator<Result<RolePlayer>, BoxStream<'static, Result<RolePlayer>>>;

pub struct RolePlayerIterator(RolePlayerIteratorInner);

impl RolePlayerIterator {
    fn new(it: BoxStream<'static, Result<(RoleType, Thing)>>) -> Self {
        Self(CIterator(box_stream(it.map_ok(|(role_type, thing)| RolePlayer {
            role_type: Concept::RoleType(role_type),
            player: match thing {
                Thing::Entity(entity) => Concept::Entity(entity),
                Thing::Relation(relation) => Concept::Relation(relation),
                Thing::Attribute(attribute) => Concept::Attribute(attribute),
            },
        }))))
    }
}

#[no_mangle]
pub extern "C" fn role_player_iterator_next(it: *mut RolePlayerIterator) -> *mut RolePlayer {
    unsafe { iterator_try_next(addr_of_mut!((*it).0)) }
}

#[no_mangle]
pub extern "C" fn role_player_iterator_drop(it: *mut RolePlayerIterator) {
    free(it);
}

pub struct RolePlayer {
    role_type: Concept,
    player: Concept,
}

#[no_mangle]
pub extern "C" fn role_player_drop(role_player: *mut RolePlayer) {
    free(role_player);
}

#[no_mangle]
pub extern "C" fn role_player_get_role_type(role_player: *const RolePlayer) -> *mut Concept {
    release(borrow(role_player).role_type.clone())
}

#[no_mangle]
pub extern "C" fn role_player_get_player(role_player: *const RolePlayer) -> *mut Concept {
    release(borrow(role_player).player.clone())
}
