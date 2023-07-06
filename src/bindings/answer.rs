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

use std::{ffi::c_char, ptr::null_mut};

use super::{
    concept::ConceptIterator,
    iterator::CIterator,
    memory::{borrow, borrow_mut, free, release, release_optional, release_string, string_free, string_view},
    query::ConceptMapIterator,
};
use crate::{
    answer::{ConceptMap, ConceptMapGroup, Explainable, Explainables, Numeric, NumericGroup},
    common::{box_stream, stream::BoxStream},
    concept::Concept,
    logic::{Explanation, Rule},
};

#[no_mangle]
pub extern "C" fn concept_map_drop(concept_map: *mut ConceptMap) {
    free(concept_map);
}

type StringIteratorInner = CIterator<String, BoxStream<'static, String>>;

pub struct StringIterator(StringIteratorInner);

#[no_mangle]
pub extern "C" fn string_iterator_next(it: *mut StringIterator) -> *mut c_char {
    borrow_mut(it).0 .0.next().map(release_string).unwrap_or(null_mut())
}

#[no_mangle]
pub extern "C" fn string_iterator_drop(it: *mut StringIterator) {
    free(it);
}

#[repr(C)]
pub struct StringPair(*mut c_char, *mut c_char);

impl From<(String, String)> for StringPair {
    fn from((left, right): (String, String)) -> Self {
        Self(release_string(left), release_string(right))
    }
}

impl Drop for StringPair {
    fn drop(&mut self) {
        string_free(self.0);
        string_free(self.1);
    }
}

#[no_mangle]
pub extern "C" fn string_pair_drop(string_pair: *mut StringPair) {
    free(string_pair);
}

type StringPairIteratorInner = CIterator<(String, String), BoxStream<'static, (String, String)>>;

pub struct StringPairIterator(StringPairIteratorInner);

#[no_mangle]
pub extern "C" fn string_pair_iterator_next(it: *mut StringPairIterator) -> *mut StringPair {
    release_optional(borrow_mut(it).0 .0.next().map(Into::into))
}

#[no_mangle]
pub extern "C" fn string_pair_iterator_drop(it: *mut StringPairIterator) {
    free(it);
}

#[no_mangle]
pub extern "C" fn concept_map_get_variables(concept_map: *const ConceptMap) -> *mut StringIterator {
    release(StringIterator(CIterator(box_stream(borrow(concept_map).map.clone().into_keys()))))
}

#[no_mangle]
pub extern "C" fn concept_map_get_values(concept_map: *const ConceptMap) -> *mut ConceptIterator {
    release(ConceptIterator(CIterator(box_stream(borrow(concept_map).map.clone().into_values().map(Ok)))))
}

#[no_mangle]
pub extern "C" fn concept_map_get(concept_map: *const ConceptMap, var: *const c_char) -> *mut Concept {
    release_optional(borrow(concept_map).get(string_view(var)).cloned())
}

#[no_mangle]
pub extern "C" fn concept_map_get_explainables(concept_map: *const ConceptMap) -> *mut Explainables {
    release(borrow(concept_map).explainables.clone())
}

#[no_mangle]
pub extern "C" fn explainables_drop(explainables: *mut Explainables) {
    free(explainables);
}

#[no_mangle]
pub extern "C" fn explainables_get_relation(explainables: *const Explainables, var: *const c_char) -> *mut Explainable {
    release_optional(borrow(explainables).relations.get(string_view(var)).cloned())
}

#[no_mangle]
pub extern "C" fn explainables_get_attribute(
    explainables: *const Explainables,
    var: *const c_char,
) -> *mut Explainable {
    release_optional(borrow(explainables).attributes.get(string_view(var)).cloned())
}

#[no_mangle]
pub extern "C" fn explainables_get_ownership(
    explainables: *const Explainables,
    owner: *const c_char,
    attribute: *const c_char,
) -> *mut Explainable {
    release_optional(
        borrow(explainables)
            .ownerships
            .get(&(string_view(owner).to_owned(), string_view(attribute).to_owned()))
            .cloned(),
    )
}

#[no_mangle]
pub extern "C" fn explainables_get_relations_keys(explainables: *const Explainables) -> *mut StringIterator {
    release(StringIterator(CIterator(box_stream(borrow(explainables).relations.clone().into_keys()))))
}

#[no_mangle]
pub extern "C" fn explainables_get_attributes_keys(explainables: *const Explainables) -> *mut StringIterator {
    release(StringIterator(CIterator(box_stream(borrow(explainables).attributes.clone().into_keys()))))
}

#[no_mangle]
pub extern "C" fn explainables_get_ownerships_keys(explainables: *const Explainables) -> *mut StringPairIterator {
    release(StringPairIterator(CIterator(box_stream(borrow(explainables).ownerships.clone().into_keys()))))
}

#[no_mangle]
pub extern "C" fn explainable_drop(explainable: *mut Explainable) {
    free(explainable);
}

#[no_mangle]
pub extern "C" fn explainable_get_id(explainable: *const Explainable) -> i64 {
    borrow(explainable).id
}

#[no_mangle]
pub extern "C" fn explainable_get_conjunction(explainable: *const Explainable) -> *mut c_char {
    release_string(borrow(explainable).conjunction.clone())
}

#[no_mangle]
pub extern "C" fn explanation_drop(explanation: *mut Explanation) {
    free(explanation);
}

#[no_mangle]
pub extern "C" fn explanation_get_rule(explanation: *const Explanation) -> *mut Rule {
    release(borrow(explanation).rule.clone())
}

#[no_mangle]
pub extern "C" fn explanation_get_conclusion(explanation: *const Explanation) -> *mut ConceptMap {
    release(borrow(explanation).conclusion.clone())
}

#[no_mangle]
pub extern "C" fn explanation_get_condition(explanation: *const Explanation) -> *mut ConceptMap {
    release(borrow(explanation).condition.clone())
}

#[no_mangle]
pub extern "C" fn concept_map_group_drop(concept_map_group: *mut ConceptMapGroup) {
    free(concept_map_group);
}

#[no_mangle]
pub extern "C" fn concept_map_group_get_owner(concept_map_group: *const ConceptMapGroup) -> *mut Concept {
    release(borrow(concept_map_group).owner.clone())
}

#[no_mangle]
pub extern "C" fn concept_map_group_get_concept_maps(
    concept_map_group: *const ConceptMapGroup,
) -> *mut ConceptMapIterator {
    release(ConceptMapIterator(CIterator(box_stream(
        borrow(concept_map_group).concept_maps.clone().into_iter().map(Ok),
    ))))
}

#[no_mangle]
pub extern "C" fn numeric_drop(numeric: *mut Numeric) {
    free(numeric);
}

#[no_mangle]
pub extern "C" fn numeric_get_double(numeric: *const Numeric) -> f64 {
    if let Numeric::Double(value) = borrow(numeric) {
        *value
    } else {
        unreachable!()
    }
}

#[no_mangle]
pub extern "C" fn numeric_get_long(numeric: *const Numeric) -> i64 {
    if let Numeric::Long(value) = borrow(numeric) {
        *value
    } else {
        unreachable!()
    }
}

#[no_mangle]
pub extern "C" fn numeric_is_double(numeric: *const Numeric) -> bool {
    matches!(borrow(numeric), Numeric::Double(_))
}

#[no_mangle]
pub extern "C" fn numeric_is_long(numeric: *const Numeric) -> bool {
    matches!(borrow(numeric), Numeric::Long(_))
}

#[no_mangle]
pub extern "C" fn numeric_is_nan(numeric: *const Numeric) -> bool {
    matches!(borrow(numeric), Numeric::NaN)
}

#[no_mangle]
pub extern "C" fn numeric_to_string(numeric: *const Numeric) -> *const c_char {
    release_string(borrow(numeric).to_string())
}

#[no_mangle]
pub extern "C" fn numeric_group_drop(numeric_group: *mut NumericGroup) {
    free(numeric_group);
}

#[no_mangle]
pub extern "C" fn numeric_group_get_owner(numeric_group: *mut NumericGroup) -> *mut Concept {
    release(borrow(numeric_group).owner.clone())
}

#[no_mangle]
pub extern "C" fn numeric_group_get_numeric(numeric_group: *mut NumericGroup) -> *mut Numeric {
    release(borrow(numeric_group).numeric.clone())
}
