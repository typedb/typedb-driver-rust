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

use super::common::{borrow, borrow_mut, free, release_string, take_ownership, unwrap_to_c_string, unwrap_void};
use crate::Database;

#[no_mangle]
pub extern "C" fn database_drop(database: *mut Database) {
    free(database);
}

#[no_mangle]
pub extern "C" fn database_get_name(database: *const Database) -> *mut c_char {
    release_string(borrow(database).name().to_owned())
}

#[no_mangle]
pub extern "C" fn database_delete(database: *mut Database) {
    unwrap_void(take_ownership(database).delete());
}

#[no_mangle]
pub extern "C" fn database_schema(database: *mut Database) -> *mut c_char {
    unwrap_to_c_string(borrow_mut(database).schema())
}

#[no_mangle]
pub extern "C" fn database_type_schema(database: *mut Database) -> *mut c_char {
    unwrap_to_c_string(borrow_mut(database).type_schema())
}

#[no_mangle]
pub extern "C" fn database_rule_schema(database: *mut Database) -> *mut c_char {
    unwrap_to_c_string(borrow_mut(database).rule_schema())
}
