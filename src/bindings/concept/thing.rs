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

use std::ffi::c_char;

use typeql_lang::pattern::Annotation;

use super::{ConceptIterator, RolePlayerIterator};
use crate::{
    bindings::common::{
        borrow, borrow_mut, release_string, string_array_view, take_ownership, unwrap_or_default, unwrap_or_null,
        unwrap_void,
    },
    concept::{AttributeType, Concept, RoleType, ThingType},
    transaction::concept::api::{AttributeAPI, RelationAPI, ThingAPI},
    Transaction,
};

#[no_mangle]
pub extern "C" fn thing_get_iid(thing: *mut Concept) -> *mut c_char {
    release_string(
        match borrow(thing) {
            Concept::Entity(entity) => &entity.iid,
            Concept::Relation(relation) => &relation.iid,
            Concept::Attribute(attribute) => &attribute.iid,
            _ => unreachable!(),
        }
        .to_string(),
    )
}

#[no_mangle]
pub extern "C" fn thing_get_is_inferred(thing: *mut Concept) -> bool {
    match borrow(thing) {
        Concept::Entity(entity) => entity.is_inferred,
        Concept::Relation(relation) => relation.is_inferred,
        Concept::Attribute(attribute) => attribute.is_inferred,
        _ => unreachable!(),
    }
}

#[no_mangle]
pub extern "C" fn thing_delete(transaction: *mut Transaction<'static>, thing: *mut Concept) {
    let transaction = borrow(transaction);
    unwrap_void(match take_ownership(thing) {
        Concept::Entity(entity) => entity.delete(transaction),
        Concept::Relation(relation) => relation.delete(transaction),
        Concept::Attribute(attribute) => attribute.delete(transaction),
        _ => unreachable!(),
    })
}

#[no_mangle]
pub extern "C" fn thing_is_deleted(transaction: *mut Transaction<'static>, thing: *const Concept) -> bool {
    let transaction = borrow(transaction);
    unwrap_or_default(match borrow(thing) {
        Concept::Entity(entity) => entity.is_deleted(transaction),
        Concept::Relation(relation) => relation.is_deleted(transaction),
        Concept::Attribute(attribute) => attribute.is_deleted(transaction),
        _ => unreachable!(),
    })
}

#[no_mangle]
pub extern "C" fn thing_get_has(
    transaction: *mut Transaction<'static>,
    thing: *const Concept,
    attribute_types: *const *const c_char,
    annotations: *const *const c_char,
) -> *mut ConceptIterator {
    let transaction = borrow(transaction);
    let attribute_types = string_array_view(attribute_types).map(AttributeType::from_label).collect();
    let annotations = string_array_view(annotations)
        .map(|anno| match anno {
            "@key" => Annotation::Key,
            "@unique" => Annotation::Unique,
            _ => unreachable!(),
        })
        .collect();
    unwrap_or_null(
        match borrow(thing) {
            Concept::Entity(entity) => entity.get_has(transaction, attribute_types, annotations),
            Concept::Relation(relation) => relation.get_has(transaction, attribute_types, annotations),
            Concept::Attribute(attribute) => attribute.get_has(transaction, attribute_types, annotations),
            _ => unreachable!(),
        }
        .map(ConceptIterator::attributes),
    )
}

#[no_mangle]
pub extern "C" fn thing_set_has(
    transaction: *mut Transaction<'static>,
    thing: *mut Concept,
    attribute: *const Concept,
) {
    let transaction = borrow(transaction);
    let Concept::Attribute(attribute) = borrow(attribute).clone() else {unreachable!()};
    unwrap_void(match borrow_mut(thing) {
        Concept::Entity(entity) => entity.set_has(transaction, attribute),
        Concept::Relation(relation) => relation.set_has(transaction, attribute),
        Concept::Attribute(attribute_owner) => attribute_owner.set_has(transaction, attribute),
        _ => unreachable!(),
    })
}

#[no_mangle]
pub extern "C" fn thing_unset_has(
    transaction: *mut Transaction<'static>,
    thing: *mut Concept,
    attribute: *const Concept,
) {
    let transaction = borrow(transaction);
    let Concept::Attribute(attribute) = borrow(attribute).clone() else {unreachable!()};
    unwrap_void(match borrow_mut(thing) {
        Concept::Entity(entity) => entity.unset_has(transaction, attribute),
        Concept::Relation(relation) => relation.unset_has(transaction, attribute),
        Concept::Attribute(attribute_owner) => attribute_owner.unset_has(transaction, attribute),
        _ => unreachable!(),
    })
}

#[no_mangle]
pub extern "C" fn thing_get_relations(
    transaction: *mut Transaction<'static>,
    thing: *const Concept,
    role_types: *const *const c_char,
) -> *mut ConceptIterator {
    let transaction = borrow(transaction);
    let role_types = string_array_view(role_types).map(RoleType::from_label).collect();
    unwrap_or_null(
        match borrow(thing) {
            Concept::Entity(entity) => entity.get_relations(transaction, role_types),
            Concept::Relation(relation) => relation.get_relations(transaction, role_types),
            Concept::Attribute(attribute) => attribute.get_relations(transaction, role_types),
            _ => unreachable!(),
        }
        .map(ConceptIterator::relations),
    )
}

#[no_mangle]
pub extern "C" fn thing_get_playing(
    transaction: *mut Transaction<'static>,
    thing: *const Concept,
) -> *mut ConceptIterator {
    let transaction = borrow(transaction);
    unwrap_or_null(
        match borrow(thing) {
            Concept::Entity(entity) => entity.get_playing(transaction),
            Concept::Relation(relation) => relation.get_playing(transaction),
            Concept::Attribute(attribute) => attribute.get_playing(transaction),
            _ => unreachable!(),
        }
        .map(ConceptIterator::role_types),
    )
}

#[no_mangle]
pub extern "C" fn relation_add_role_player(
    transaction: *mut Transaction<'static>,
    relation: *mut Concept,
    role_type: *const Concept,
    player: *const Concept,
) {
    let transaction = borrow(transaction);
    let Concept::RoleType(role_type) = borrow(role_type).clone() else { unreachable!() };
    let player = match borrow(player).clone() {
        Concept::Entity(entity) => entity.into_thing(),
        Concept::Relation(relation) => relation.into_thing(),
        Concept::Attribute(attribute) => attribute.into_thing(),
        _ => unreachable!(),
    };
    unwrap_void(match borrow(relation) {
        Concept::Relation(relation) => relation.add_role_player(transaction, role_type, player),
        _ => unreachable!(),
    })
}

#[no_mangle]
pub extern "C" fn relation_remove_role_player(
    transaction: *mut Transaction<'static>,
    relation: *mut Concept,
    role_type: *const Concept,
    player: *const Concept,
) {
    let transaction = borrow(transaction);
    let Concept::RoleType(role_type) = borrow(role_type).clone() else { unreachable!() };
    let player = match borrow(player).clone() {
        Concept::Entity(entity) => entity.into_thing(),
        Concept::Relation(relation) => relation.into_thing(),
        Concept::Attribute(attribute) => attribute.into_thing(),
        _ => unreachable!(),
    };
    unwrap_void(match borrow(relation) {
        Concept::Relation(relation) => relation.remove_role_player(transaction, role_type, player),
        _ => unreachable!(),
    })
}

#[no_mangle]
pub extern "C" fn relation_get_players_by_role_type(
    transaction: *mut Transaction<'static>,
    relation: *const Concept,
    role_types: *const *const c_char,
) -> *mut ConceptIterator {
    let transaction = borrow(transaction);
    let role_types = string_array_view(role_types).map(RoleType::from_label).collect();
    unwrap_or_null(
        match borrow(relation) {
            Concept::Relation(relation) => relation.get_players_by_role_type(transaction, role_types),
            _ => unreachable!(),
        }
        .map(ConceptIterator::things),
    )
}

#[no_mangle]
pub extern "C" fn relation_get_role_players(
    transaction: *mut Transaction<'static>,
    relation: *const Concept,
) -> *mut RolePlayerIterator {
    let transaction = borrow(transaction);
    unwrap_or_null(
        match borrow(relation) {
            Concept::Relation(relation) => relation.get_role_players(transaction),
            _ => unreachable!(),
        }
        .map(RolePlayerIterator::new),
    )
}

#[no_mangle]
pub extern "C" fn relation_get_relating(
    transaction: *mut Transaction<'static>,
    relation: *const Concept,
) -> *mut ConceptIterator {
    let transaction = borrow(transaction);
    unwrap_or_null(
        match borrow(relation) {
            Concept::Relation(relation) => relation.get_relating(transaction),
            _ => unreachable!(),
        }
        .map(ConceptIterator::role_types),
    )
}

#[no_mangle]
pub extern "C" fn attribute_get_owners(
    transaction: *mut Transaction<'static>,
    attribute: *const Concept,
    thing_type: *const Concept,
) -> *mut ConceptIterator {
    let transaction = borrow(transaction);
    let thing_type = unsafe { thing_type.as_ref() }.cloned().map(|tt| match tt {
        Concept::EntityType(entity_type) => ThingType::EntityType(entity_type),
        Concept::RelationType(relation_type) => ThingType::RelationType(relation_type),
        Concept::AttributeType(attribute_type) => ThingType::AttributeType(attribute_type),
        Concept::RootThingType(root_thing_type) => ThingType::RootThingType(root_thing_type),
        _ => unreachable!(),
    });
    unwrap_or_null(
        match borrow(attribute) {
            Concept::Attribute(attribute) => attribute.get_owners(transaction, thing_type),
            _ => unreachable!(),
        }
        .map(ConceptIterator::things),
    )
}
