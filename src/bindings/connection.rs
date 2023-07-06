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

use super::{
    error::{unwrap_or_null, unwrap_void},
    memory::{free, string_array_view, string_view, take_ownership},
};
use crate::{Connection, Credential};

#[no_mangle]
pub extern "C" fn connection_open_plaintext(address: *const c_char) -> *mut Connection {
    unwrap_or_null(Connection::new_plaintext(string_view(address)))
}

#[no_mangle]
pub extern "C" fn connection_open_encrypted(addresses: *const *const c_char) -> *mut Connection {
    let addresses: Vec<&str> = string_array_view(addresses).collect();
    unwrap_or_null(Connection::new_encrypted(&addresses, Credential::without_tls("admin", "password")))
}

#[no_mangle]
pub extern "C" fn connection_force_close(connection: *mut Connection) {
    unwrap_void(take_ownership(connection).force_close());
}

#[no_mangle]
pub extern "C" fn connection_close(connection: *mut Connection) {
    free(connection);
}
