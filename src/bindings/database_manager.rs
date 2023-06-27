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

use std::{ffi::c_char, ptr::addr_of_mut};

use super::{
    common::{free, release, string_view},
    iterator::{iterator_next, CIterator},
};
use crate::{
    bindings::common::{borrow, borrow_mut, unwrap_or_default, unwrap_or_null, unwrap_void},
    Connection, Database, DatabaseManager,
};

#[no_mangle]
pub extern "C" fn database_manager_new(connection: *const Connection) -> *mut DatabaseManager {
    let connection = borrow(connection).clone();
    release(DatabaseManager::new(connection))
}

#[no_mangle]
pub extern "C" fn database_manager_drop(databases: *mut DatabaseManager) {
    free(databases);
}

type DatabaseIteratorInner = CIterator<Database, <Vec<Database> as IntoIterator>::IntoIter>;

pub struct DatabaseIterator(DatabaseIteratorInner);

#[no_mangle]
pub extern "C" fn database_iterator_next(it: *mut DatabaseIterator) -> *mut Database {
    unsafe { iterator_next(addr_of_mut!((*it).0)) }
}

#[no_mangle]
pub extern "C" fn database_iterator_drop(it: *mut DatabaseIterator) {
    free(it);
}

#[no_mangle]
pub extern "C" fn databases_all(databases: *mut DatabaseManager) -> *mut DatabaseIterator {
    unwrap_or_null(borrow_mut(databases).all().map(|dbs| DatabaseIterator(CIterator(dbs.into_iter()))))
}

#[no_mangle]
pub extern "C" fn databases_create(databases: *mut DatabaseManager, name: *const c_char) {
    unwrap_void(borrow_mut(databases).create(string_view(name)));
}

#[no_mangle]
pub extern "C" fn databases_contains(databases: *mut DatabaseManager, name: *const c_char) -> bool {
    unwrap_or_default(borrow_mut(databases).contains(string_view(name)))
}

#[no_mangle]
pub extern "C" fn databases_get(databases: *mut DatabaseManager, name: *const c_char) -> *mut Database {
    unwrap_or_null(borrow_mut(databases).get(string_view(name)))
}