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
use typedb_client::{
    concept::{Thing, Value},
    Result as TypeDBResult,
};

use crate::{
    behaviour::{
        concept::common::{get_attribute_type, get_entity_type},
        parameter::LabelParse,
        Context,
    },
    generic_step_impl,
};

generic_step_impl! {
    #[step(regex = r"^(\$\S+) = entity\( ?(\S+) ?\) create new instance$")]
    async fn entity_type_create_new_instance(context: &mut Context, var: String, type_label: String) -> TypeDBResult {
        let tx = context.transaction();
        let entity = get_entity_type(tx, type_label).await?.create(tx).await?;
        context.things.insert(var, Some(Thing::Entity(entity)));
        Ok(())
    }

    #[step(expr = r"entity\(( ){label}( )\) create new instance; throws exception")]
    async fn entity_type_create_new_instance_throws(context: &mut Context, type_label: LabelParse) {
        // FIXME ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~v
        assert!(entity_type_create_new_instance(context, "".to_owned(), type_label.name).await.is_err());
    }

    #[step(regex = r"^(\$\S+) = entity\( ?(\S+) ?\) create new instance with key\((\S+)\): (\S+)$")]
    async fn entity_type_create_new_instance_with_key(
        context: &mut Context,
        var: String,
        type_label: String,
        attribute_type_label: String,
        attribute_value: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let entity = get_entity_type(tx, type_label).await?.create(tx).await?;
        let attribute =
            get_attribute_type(tx, attribute_type_label).await?.put(tx, Value::String(attribute_value)).await?;
        entity.set_has(tx, attribute).await?;
        context.things.insert(var, Some(Thing::Entity(entity)));
        Ok(())
    }
}
