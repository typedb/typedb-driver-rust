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

use super::common::{borrow_mut, release_optional, unwrap_optional_or_null};
use crate::Result;

pub struct CIterator<T: 'static, U: Iterator<Item = T> + 'static>(pub(super) U);

pub(super) fn iterator_next<T: 'static, U: Iterator<Item = T> + 'static>(it: *mut CIterator<T, U>) -> *mut T {
    release_optional(borrow_mut(it).0.next())
}

pub(super) fn iterator_try_next<T: 'static, U: Iterator<Item = Result<T>> + 'static>(
    it: *mut CIterator<Result<T>, U>,
) -> *mut T {
    unwrap_optional_or_null(borrow_mut(it).0.next())
}
