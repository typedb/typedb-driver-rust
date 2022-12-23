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

use crossbeam::{atomic::AtomicCell, channel::Sender};

#[derive(Debug)]
pub(crate) struct DropGuard<T: Clone = ()> {
    sink: Sender<T>,
    message: T,
    is_armed: AtomicCell<bool>,
}

impl<T: Clone> DropGuard<T> {
    pub(crate) fn new(sink: Sender<T>, message: T) -> Self {
        Self { sink, message, is_armed: AtomicCell::new(true) }
    }

    pub(crate) fn release(&self) {
        if self.is_armed.compare_exchange(true, false).is_ok() {
            self.sink.send(self.message.clone()).unwrap()
        }
    }
}

impl<T: Clone> Drop for DropGuard<T> {
    fn drop(&mut self) {
        self.release()
    }
}
