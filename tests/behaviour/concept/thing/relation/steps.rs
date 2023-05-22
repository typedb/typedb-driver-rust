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

use cucumber::{gherkin::Step, given, then, when};
use futures::{future::try_join_all, StreamExt, TryStreamExt};
use typedb_client::{
    concept::{Relation, Thing, ThingType},
    Result as TypeDBResult,
};

use crate::{
    assert_err,
    behaviour::{
        parameter::{ContainmentParse, LabelParse, RoleParse, ValueParse, VarParse},
        Context,
    },
    generic_step_impl,
};

generic_step_impl! {
    #[step(expr = "relation {var} is deleted: {word}")]
    async fn relation_is_deleted(context: &mut Context, var: VarParse, is_deleted: bool) -> TypeDBResult {
        assert_eq!(context.get_relation(var.name).is_deleted(context.transaction()).await?, is_deleted);
        Ok(())
    }

    #[step(expr = "relation {var} has type: {label}")]
    async fn relation_has_type(context: &mut Context, var: VarParse, type_label: LabelParse) {
        assert_eq!(context.get_relation(var.name).type_.label, type_label.name);
    }

    #[step(expr = "delete relation: {var}")]
    async fn delete_relation(context: &mut Context, var: VarParse) -> TypeDBResult {
        context.get_relation(var.name).delete(context.transaction()).await
    }

    #[step(expr = r"relation\(( ){label}( )\) get instances is empty")]
    async fn relation_type_get_instances_is_empty(context: &mut Context, type_label: LabelParse) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        assert!(relation_type.get_instances(tx)?.next().await.is_none());
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )\) get instances {maybe_contain}: {var}")]
    async fn relation_type_get_instances_contain(
        context: &mut Context,
        type_label: LabelParse,
        containment: ContainmentParse,
        var: VarParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let actuals: Vec<Relation> = relation_type.get_instances(tx)?.try_collect().await?;
        let relation = context.get_relation(var.name);
        containment.assert(&actuals, relation);
        Ok(())
    }

    #[step(regex = r"^(\$\S+) = relation\( ?(\S+) ?\) create new instance$")]
    async fn relation_type_create_new_instance(context: &mut Context, var: String, type_label: String) -> TypeDBResult {
        let tx = context.transaction();
        let relation = context.get_relation_type(type_label).await?.create(tx).await?;
        context.insert_relation(var, Some(relation));
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )\) create new instance; throws exception")]
    async fn relation_type_create_new_instance_throws(context: &mut Context, type_label: LabelParse) {
        // FIXME ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~v
        assert_err!(relation_type_create_new_instance(context, "".to_owned(), type_label.name).await);
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
        let relation = context.get_relation_type(type_label.name).await?.create(tx).await?;
        let attribute_type = context.get_attribute_type(attribute_type_label.name).await?;
        let attribute = attribute_type.put(tx, value.into_value(attribute_type.value_type)).await?;
        relation.set_has(tx, attribute).await?;
        context.insert_relation(var.name, Some(relation));
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
        assert_err!(
            relation_type_create_new_instance_with_key(context, var, type_label, attribute_type_label, value).await
        );
    }

    #[step(expr = r"{var} = relation\(( ){label}( )\) get instance with key\({label}\): {value}")]
    async fn relation_type_get_instance_with_key(
        context: &mut Context,
        var: VarParse,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
        value: ValueParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let attribute_type = context.get_attribute_type(attribute_type_label.name).await?;
        let attribute = attribute_type.get(tx, value.into_value(attribute_type.value_type)).await?.unwrap();
        let relation_type = context.get_relation_type(type_label.name).await?;
        let relations: Vec<Thing> =
            attribute.get_owners(tx, Some(ThingType::RelationType(relation_type)))?.try_collect().await?;
        context.insert_thing(var.name, relations.into_iter().next());
        Ok(())
    }

    #[step(expr = r"relation {var} add player for role\(( ){label}( )\): {var}")]
    async fn relation_add_player_for_role(
        context: &mut Context,
        var: VarParse,
        role_name: LabelParse,
        player_var: VarParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation = context.get_relation(var.name);
        let role_type = relation.type_.get_relates_for_role_label(tx, role_name.name).await?.unwrap();
        let player = context.get_thing(player_var.name);
        relation.add_player(tx, role_type, player.clone()).await
    }

    #[step(expr = r"relation {var} add player for role\(( ){label}( )\): {var}; throws exception")]
    async fn relation_add_player_for_role_throws(
        context: &mut Context,
        var: VarParse,
        role_name: LabelParse,
        player_var: VarParse,
    ) {
        assert_err!(relation_add_player_for_role(context, var, role_name, player_var,).await);
    }

    #[step(expr = r"relation {var} remove player for role\(( ){label}( )\): {var}")]
    async fn relation_remove_player_for_role(
        context: &mut Context,
        var: VarParse,
        role_name: LabelParse,
        player_var: VarParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation = context.get_relation(var.name);
        let role_type = relation.type_.get_relates_for_role_label(tx, role_name.name).await?.unwrap();
        let player = context.get_thing(player_var.name);
        relation.remove_player(tx, role_type, player.clone()).await
    }

    #[step(expr = r"relation {var} get players{maybe_role} {maybe_contain}: {var}")]
    async fn relation_get_players_contain(
        context: &mut Context,
        var: VarParse,
        role: RoleParse,
        containment: ContainmentParse,
        player_var: VarParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation = context.get_relation(var.name);
        let player = context.get_thing(player_var.name);
        let roles = try_join_all(
            role.role
                .into_iter()
                .map(|role| async { relation.type_.get_relates_for_role_label(tx, role).await.transpose().unwrap() }),
        )
        .await?;
        let actuals: Vec<Thing> = relation.get_players(tx, roles)?.try_collect().await?;
        containment.assert(&actuals, player);
        Ok(())
    }

    #[step(expr = r"relation {var} get players {maybe_contain}:")]
    async fn relation_get_players_contain_table(
        context: &mut Context,
        step: &Step,
        var: VarParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation = context.get_relation(var.name);
        let actuals: Vec<(String, Thing)> = relation
            .get_players_by_role_type(tx)?
            .map_ok(|(role, player)| (role.label.name, player))
            .try_collect()
            .await?;
        for row in &step.table().unwrap().rows {
            let [role, player_var] = &row[..] else { unreachable!() };
            let player = context.get_thing(player_var.to_owned());
            containment.assert(&actuals, (role, player));
        }
        Ok(())
    }
}
