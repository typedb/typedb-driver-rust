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

use std::sync::Mutex;

pub(crate) struct DropGuard {
    callback: Mutex<Option<Box<dyn FnOnce() -> () + Send>>>,
}

impl DropGuard {
    pub(crate) fn new(callback: impl FnOnce() -> () + 'static + Send) -> Self {
        Self { callback: Mutex::new(Some(Box::new(callback))) }
    }

    pub(crate) fn release(&self) {
        let mut callback = self.callback.lock().unwrap();
        if callback.is_some() {
            (callback.take().unwrap())()
        }
    }
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.release();
    }
}
