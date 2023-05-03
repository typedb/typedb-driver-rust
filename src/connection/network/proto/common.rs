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

use typedb_protocol::{r#type::Transitivity as TransitivityProto, session, transaction, Options as OptionsProto};

use super::IntoProto;
use crate::{common::Transitivity, Options, SessionType, TransactionType};

impl IntoProto<session::Type> for SessionType {
    fn into_proto(self) -> session::Type {
        match self {
            Self::Data => session::Type::Data,
            Self::Schema => session::Type::Schema,
        }
    }
}

impl IntoProto<transaction::Type> for TransactionType {
    fn into_proto(self) -> transaction::Type {
        match self {
            Self::Read => transaction::Type::Read,
            Self::Write => transaction::Type::Write,
        }
    }
}

impl IntoProto<OptionsProto> for Options {
    fn into_proto(self) -> OptionsProto {
        OptionsProto {
            infer: self.infer,
            trace_inference: self.trace_inference,
            explain: self.explain,
            parallel: self.parallel,
            prefetch_size: self.prefetch_size,
            prefetch: self.prefetch,
            session_idle_timeout_millis: self.session_idle_timeout.map(|val| val.as_millis() as i32),
            transaction_timeout_millis: self.transaction_timeout.map(|val| val.as_millis() as i32),
            schema_lock_acquire_timeout_millis: self.schema_lock_acquire_timeout.map(|val| val.as_millis() as i32),
            read_any_replica: self.read_any_replica,
        }
    }
}

impl IntoProto<TransitivityProto> for Transitivity {
    fn into_proto(self) -> TransitivityProto {
        match self {
            Self::Explicit => TransitivityProto::Explicit,
            Self::Transitive => TransitivityProto::Transitive,
        }
    }
}
