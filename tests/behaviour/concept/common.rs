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

use typedb_client::{
    concept::{Attribute, AttributeType, EntityType, RelationType, Thing},
    Result as TypeDBResult, Transaction,
};

use crate::behaviour::Context;

pub(super) async fn get_entity_type(tx: &Transaction<'_>, type_label: String) -> TypeDBResult<EntityType> {
    tx.concept().get_entity_type(type_label).await.map(|entity_type| {
        assert!(entity_type.is_some());
        entity_type.unwrap()
    })
}

pub(super) async fn get_relation_type(tx: &Transaction<'_>, type_label: String) -> TypeDBResult<RelationType> {
    tx.concept().get_relation_type(type_label).await.map(|relation_type| {
        assert!(relation_type.is_some());
        relation_type.unwrap()
    })
}

pub(super) async fn get_attribute_type(tx: &Transaction<'_>, type_label: String) -> TypeDBResult<AttributeType> {
    tx.concept().get_attribute_type(type_label).await.map(|attribute_type| {
        assert!(attribute_type.is_some());
        attribute_type.unwrap()
    })
}

pub(super) fn get_attribute(context: &Context, var_name: String) -> &Attribute {
    assert!(context.things.contains_key(&var_name));
    let thing = context.things.get(&var_name).unwrap();
    assert!(matches!(thing, Some(Thing::Attribute(_))));
    let Some(Thing::Attribute(attribute)) = thing else { unreachable!() };
    attribute
}
