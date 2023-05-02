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

use cucumber::{given, then, when};
use futures::TryFutureExt;

use crate::{behaviour::Context, generic_step_impl};

generic_step_impl! {
    #[step(expr = "delete entity type: {word}")]
    async fn delete_thing_type(context: &mut Context, type_label: String) {
        let tx = context.transaction();
        assert!(tx.concept().get_entity_type(type_label).and_then(|entity_type| async move {
            assert!(entity_type.is_some());
            entity_type.unwrap().delete(tx).await
        }).await.is_ok());
   }

    #[step(expr = "delete entity type: {word}; throws exception")]
    async fn delete_thing_type_throws_exception(context: &mut Context, type_label: String) {
        let tx = context.transaction();
        assert!(tx.concept().get_entity_type(type_label).and_then(|entity_type| async move {
            assert!(entity_type.is_some());
            entity_type.unwrap().delete(tx).await
        }).await.is_err()); // FIXME WET
    }
}
