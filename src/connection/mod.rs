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

mod connection;
pub(crate) mod network;
mod runtime;
mod transaction;

use crossbeam::channel::Sender as SyncSender;
use tokio::sync::oneshot::Sender as AsyncOneshotSender;

pub use self::connection::Connection;
pub(crate) use self::{connection::ServerConnection, transaction::TransactionStream};
use crate::common::{error::InternalError, Result};

#[derive(Debug)]
enum OneShotSender<T> {
    Async(AsyncOneshotSender<Result<T>>),
    Blocking(SyncSender<Result<T>>),
}

impl<T> OneShotSender<T> {
    fn send(self, response: Result<T>) -> Result {
        match self {
            Self::Async(sink) => sink.send(response).map_err(|_| InternalError::SendError().into()),
            Self::Blocking(sink) => sink.send(response).map_err(Into::into),
        }
    }
}
