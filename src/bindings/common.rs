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

use std::{
    cell::RefCell,
    ffi::{c_char, CStr, CString},
};

use crate::{Error, Result};

thread_local! {
    static LAST_ERROR: RefCell<Option<Error>> = RefCell::new(None);
}

pub(super) fn release<T>(t: T) -> *mut T {
    Box::into_raw(Box::new(t))
}

pub(super) fn release_optional<T>(t: Option<T>) -> *mut T {
    t.map(release).unwrap_or(std::ptr::null_mut())
}

pub(super) fn release_string(str: String) -> *mut c_char {
    CString::new(str).unwrap().into_raw()
}

pub(super) fn borrow<T>(raw: *const T) -> &'static T {
    assert!(!raw.is_null());
    unsafe { &*raw }
}

pub(super) fn borrow_mut<T>(raw: *mut T) -> &'static mut T {
    assert!(!raw.is_null());
    unsafe { &mut *raw }
}

pub(super) fn borrow_optional<T>(raw: *const T) -> Option<&'static T> {
    unsafe { raw.as_ref() }
}

pub(super) fn take_ownership<T>(raw: *mut T) -> T {
    assert!(!raw.is_null());
    unsafe { *Box::from_raw(raw) }
}

pub(super) fn free<T>(raw: *mut T) {
    if !raw.is_null() {
        unsafe { drop(Box::from_raw(raw)) }
    }
}

pub(super) fn string_view(str: *const c_char) -> &'static str {
    assert!(!str.is_null());
    unsafe { CStr::from_ptr(str).to_str().unwrap() }
}

#[no_mangle]
pub extern "C" fn string_free(str: *mut c_char) {
    if !str.is_null() {
        unsafe { drop(CString::from_raw(str)) }
    }
}

pub(super) fn array_view<T>(ts: *const *const T) -> impl Iterator<Item = &'static T> {
    assert!(!ts.is_null());
    unsafe { (0..).map_while(move |i| (*ts.add(i)).as_ref()) }
}

pub(super) fn string_array_view(strs: *const *const c_char) -> impl Iterator<Item = &'static str> {
    assert!(!strs.is_null());
    unsafe { (0..).map_while(move |i| (*strs.add(i)).as_ref()).map(|p| string_view(p)) }
}

pub(super) fn ok_record<T>(result: Result<T>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(err) => {
            record_error(err);
            None
        }
    }
}

pub(super) fn ok_record_flatten<T>(result: Result<Option<T>>) -> Option<T> {
    match result {
        Ok(value) => value,
        Err(err) => {
            record_error(err);
            None
        }
    }
}

pub(super) fn unwrap_or_null<T>(result: Result<T>) -> *mut T {
    release_optional(ok_record(result))
}

pub(super) fn unwrap_optional_or_null<T>(result: Option<Result<T>>) -> *mut T {
    release_optional(ok_record_flatten(result.transpose()))
}

pub(super) fn unwrap_to_c_string(result: Result<String>) -> *mut c_char {
    ok_record(result).map(release_string).unwrap_or_else(std::ptr::null_mut)
}

pub(super) fn unwrap_or_default<T: Copy + Default>(result: Result<T>) -> T {
    ok_record(result).unwrap_or_default()
}

pub(super) fn unwrap_void(result: Result) {
    ok_record(result);
}

fn record_error(err: Error) {
    LAST_ERROR.with(|prev| *prev.borrow_mut() = Some(err));
}

#[no_mangle]
pub extern "C" fn check_error() -> bool {
    LAST_ERROR.with(|prev| prev.borrow().is_some())
}

#[no_mangle]
pub extern "C" fn get_last_error() -> *mut Error {
    LAST_ERROR.with(|prev| release_optional(prev.borrow_mut().take()))
}

#[no_mangle]
pub extern "C" fn error_drop(error: *mut Error) {
    free(error);
}

#[no_mangle]
pub extern "C" fn error_code(error: *const Error) -> *mut c_char {
    unsafe { release_string((*error).code()) }
}

#[no_mangle]
pub extern "C" fn error_message(error: *const Error) -> *mut c_char {
    unsafe { release_string((*error).message()) }
}
