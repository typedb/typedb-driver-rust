/*
 * Copyright (C) 2022 Vaticle
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * d*const c_chaributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software d*const c_chaributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::{ffi::c_char, ptr::addr_of_mut};

use super::{
    common::{borrow, free, string_view, unwrap_or_null, unwrap_void},
    iterator::{iterator_try_next, CIterator},
};
use crate::{
    answer::{ConceptMap, ConceptMapGroup, Numeric, NumericGroup},
    common::{box_stream, stream::BoxStream},
    logic::Explanation,
    Options, Result, Transaction,
};

#[no_mangle]
pub extern "C" fn query_define(transaction: *mut Transaction<'static>, query: *const c_char, options: *const Options) {
    unwrap_void(borrow(transaction).query().define_with_options(string_view(query), borrow(options).clone()))
}

#[no_mangle]
pub extern "C" fn query_undefine(
    transaction: *mut Transaction<'static>,
    query: *const c_char,
    options: *const Options,
) {
    unwrap_void(borrow(transaction).query().undefine_with_options(string_view(query), borrow(options).clone()))
}

#[no_mangle]
pub extern "C" fn query_delete(transaction: *mut Transaction<'static>, query: *const c_char, options: *const Options) {
    unwrap_void(borrow(transaction).query().delete_with_options(string_view(query), borrow(options).clone()))
}

type ConceptMapIteratorInner = CIterator<Result<ConceptMap>, BoxStream<'static, Result<ConceptMap>>>;

pub struct ConceptMapIterator(pub ConceptMapIteratorInner);

#[no_mangle]
pub extern "C" fn concept_map_iterator_next(it: *mut ConceptMapIterator) -> *mut ConceptMap {
    unsafe { iterator_try_next(addr_of_mut!((*it).0)) }
}

#[no_mangle]
pub extern "C" fn concept_map_iterator_drop(it: *mut ConceptMapIterator) {
    free(it);
}

#[no_mangle]
pub extern "C" fn query_match(
    transaction: *mut Transaction<'static>,
    query: *const c_char,
    options: *const Options,
) -> *mut ConceptMapIterator {
    unwrap_or_null(
        borrow(transaction)
            .query()
            .match_with_options(string_view(query), borrow(options).clone())
            .map(|it| ConceptMapIterator(CIterator(box_stream(it)))),
    )
}

#[no_mangle]
pub extern "C" fn query_insert(
    transaction: *mut Transaction<'static>,
    query: *const c_char,
    options: *const Options,
) -> *mut ConceptMapIterator {
    unwrap_or_null(
        borrow(transaction)
            .query()
            .insert_with_options(string_view(query), borrow(options).clone())
            .map(|it| ConceptMapIterator(CIterator(box_stream(it)))),
    )
}

#[no_mangle]
pub extern "C" fn query_update(
    transaction: *mut Transaction<'static>,
    query: *const c_char,
    options: *const Options,
) -> *mut ConceptMapIterator {
    unwrap_or_null(
        borrow(transaction)
            .query()
            .update_with_options(string_view(query), borrow(options).clone())
            .map(|it| ConceptMapIterator(CIterator(box_stream(it)))),
    )
}

#[no_mangle]
pub extern "C" fn query_match_aggregate(
    transaction: *mut Transaction<'static>,
    query: *const c_char,
    options: *const Options,
) -> *mut Numeric {
    unwrap_or_null(
        borrow(transaction).query().match_aggregate_with_options(string_view(query), borrow(options).clone()),
    )
}

type ConceptMapGroupIteratorInner = CIterator<Result<ConceptMapGroup>, BoxStream<'static, Result<ConceptMapGroup>>>;

pub struct ConceptMapGroupIterator(ConceptMapGroupIteratorInner);

#[no_mangle]
pub extern "C" fn concept_map_group_iterator_next(it: *mut ConceptMapGroupIterator) -> *mut ConceptMapGroup {
    unsafe { iterator_try_next(addr_of_mut!((*it).0)) }
}

#[no_mangle]
pub extern "C" fn concept_map_group_iterator_drop(it: *mut ConceptMapGroupIterator) {
    free(it);
}

#[no_mangle]
pub extern "C" fn query_match_group(
    transaction: *mut Transaction<'static>,
    query: *const c_char,
    options: *const Options,
) -> *mut ConceptMapGroupIterator {
    unwrap_or_null(
        borrow(transaction)
            .query()
            .match_group_with_options(string_view(query), borrow(options).clone())
            .map(|it| ConceptMapGroupIterator(CIterator(box_stream(it)))),
    )
}

type NumericGroupIteratorInner = CIterator<Result<NumericGroup>, BoxStream<'static, Result<NumericGroup>>>;

pub struct NumericGroupIterator(NumericGroupIteratorInner);

#[no_mangle]
pub extern "C" fn numeric_group_iterator_next(it: *mut NumericGroupIterator) -> *mut NumericGroup {
    unsafe { iterator_try_next(addr_of_mut!((*it).0)) }
}

#[no_mangle]
pub extern "C" fn numeric_group_iterator_drop(it: *mut NumericGroupIterator) {
    free(it);
}

#[no_mangle]
pub extern "C" fn query_match_group_aggregate(
    transaction: *mut Transaction<'static>,
    query: *const c_char,
    options: *const Options,
) -> *mut NumericGroupIterator {
    unwrap_or_null(
        borrow(transaction)
            .query()
            .match_group_aggregate_with_options(string_view(query), borrow(options).clone())
            .map(|it| NumericGroupIterator(CIterator(box_stream(it)))),
    )
}

type ExplanationIteratorInner = CIterator<Result<Explanation>, BoxStream<'static, Result<Explanation>>>;

pub struct ExplanationIterator(ExplanationIteratorInner);

#[no_mangle]
pub extern "C" fn explanation_iterator_next(it: *mut ExplanationIterator) -> *mut Explanation {
    unsafe { iterator_try_next(addr_of_mut!((*it).0)) }
}

#[no_mangle]
pub extern "C" fn explanation_iterator_drop(it: *mut ExplanationIterator) {
    free(it);
}

#[no_mangle]
pub extern "C" fn query_explain(
    transaction: *mut Transaction<'static>,
    explainable_id: i64,
    options: *const Options,
) -> *mut ExplanationIterator {
    unwrap_or_null(
        borrow(transaction)
            .query()
            .explain_with_options(explainable_id, borrow(options).clone())
            .map(|it| ExplanationIterator(CIterator(box_stream(it)))),
    )
}
