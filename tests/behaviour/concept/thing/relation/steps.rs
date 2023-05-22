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
use futures::TryStreamExt;
use typedb_client::{
    concept::{Attribute, Thing, Value},
    Result as TypeDBResult,
};

use crate::{
    behaviour::{
        concept::common::{get_attribute_type, get_relation, get_relation_type, get_thing},
        parameter::{ContainmentParse, LabelParse, ValueParse, ValueTypeParse, VarParse},
        Context,
    },
    generic_step_impl,
};

generic_step_impl! {
    #[step(expr = "relation {var} is deleted: {word}")]
    async fn relation_is_deleted(context: &mut Context, var: VarParse, is_deleted: bool) -> TypeDBResult {
        assert_eq!(get_relation(context, var.name).is_deleted(context.transaction()).await?, is_deleted);
        Ok(())
    }

    #[step(expr = r"{var} = relation\(( ){label}( )\) create new instance with key\({label}\): {value}")]
    async fn relation_type_create_new_instance_with_key(
        context: &mut Context,
        var: VarParse,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
        value: ValueParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation = get_relation_type(tx, type_label.name).await?.create(tx).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label.name).await?;
        let attribute = attribute_type.put(tx, value.into_value(attribute_type.value_type)).await?;
        relation.set_has(tx, attribute).await?;
        context.things.insert(var.name, Some(Thing::Relation(relation)));
        Ok(())
    }

    #[step(
        expr = r"{var} = relation\(( ){label}( )\) create new instance with key\({label}\): {value}; throws exception"
    )]
    async fn relation_type_create_new_instance_with_key_throws(
        context: &mut Context,
        var: VarParse,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
        value: ValueParse,
    ) {
        assert!(relation_type_create_new_instance_with_key(context, var, type_label, attribute_type_label, value)
            .await
            .is_err());
    }

    #[step(expr = r"relation {var} add player for role\(( ){label}( )\): {var}")]
    async fn relation_type_add_player_for_role(
        context: &mut Context,
        var: VarParse,
        role_name: LabelParse,
        player_var: VarParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation = get_relation(context, var.name);
        let role_type = relation.type_.get_relates_for_role_label(tx, role_name.name).await?.unwrap();
        let player = get_thing(context, player_var.name);
        relation.add_player(tx, role_type, player.clone()).await
    }

    #[step(expr = r"relation {var} remove player for role\(( ){label}( )\): {var}")]
    async fn relation_type_remove_player_for_role(
        context: &mut Context,
        var: VarParse,
        role_name: LabelParse,
        player_var: VarParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation = get_relation(context, var.name);
        let role_type = relation.type_.get_relates_for_role_label(tx, role_name.name).await?.unwrap();
        let player = get_thing(context, player_var.name);
        relation.remove_player(tx, role_type, player.clone()).await
    }
}
