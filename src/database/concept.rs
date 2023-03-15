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

use std::sync::Arc;

use futures::stream::BoxStream;

use crate::{
    common::{OwnsFilter, IID},
    concept::{Attribute, AttributeType, Relation, RoleType},
    connection::TransactionStream,
    Result,
};

#[derive(Debug)]
pub(crate) struct ConceptManager {
    transaction_stream: Arc<TransactionStream>,
}

impl ConceptManager {
    pub(super) fn new(transaction_stream: Arc<TransactionStream>) -> ConceptManager {
        ConceptManager { transaction_stream }
    }

    pub(crate) async fn set_has(&self, owner: IID, attribute: IID) {
        todo!()
    }

    pub(crate) async fn unset_has(&self, owner: IID, attribute: IID) {
        todo!()
    }

    pub(crate) fn get_has_keys(&self, owner: IID, owns_filter: OwnsFilter) -> BoxStream<Result<Attribute>> {
        todo!()
    }

    pub(crate) fn get_has_type(&self, owner: IID, attribute_type: AttributeType) -> BoxStream<Result<Attribute>> {
        todo!()
    }

    pub(crate) fn get_relations(&self, player: IID, role: RoleType) -> BoxStream<Result<Relation>> {
        todo!()
    }

    pub(crate) fn get_playing(&self, player: IID) -> BoxStream<Result<RoleType>> {
        todo!()
    }
}
