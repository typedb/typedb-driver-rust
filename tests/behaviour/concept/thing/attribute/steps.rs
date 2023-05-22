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
    concept::{Attribute, Thing},
    Result as TypeDBResult,
};

use crate::{
    behaviour::{
        concept::common::{get_attribute, get_attribute_type, get_thing},
        parameter::{ContainmentParse, LabelParse, ValueParse, ValueTypeParse, VarParse},
        Context,
    },
    generic_step_impl,
};

generic_step_impl! {
    #[step(expr = "attribute {var} is deleted: {word}")]
    async fn attribute_is_deleted(context: &mut Context, var: VarParse, is_deleted: bool) -> TypeDBResult {
        assert_eq!(get_attribute(context, var.name).is_deleted(context.transaction()).await?, is_deleted);
        Ok(())
    }

    #[step(expr = "attribute {var} has type: {label}")]
    async fn attribute_has_type(context: &mut Context, var: VarParse, type_label: LabelParse) {
        assert_eq!(get_attribute(context, var.name).type_.label, type_label.name);
    }

    #[step(expr = "delete attribute: {var}")]
    async fn delete_attribute(context: &mut Context, var: VarParse) -> TypeDBResult {
        get_attribute(context, var.name).delete(context.transaction()).await
    }

    #[step(expr = r"attribute\(( ){label}( )\) get instances {maybe_contain}: {var}")]
    async fn attribute_get_instances_contain(
        context: &mut Context,
        type_label: LabelParse,
        containment: ContainmentParse,
        var: VarParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let attribute_type = get_attribute_type(tx, type_label.into()).await?;
        let actuals: Vec<Attribute> = attribute_type.get_instances(tx)?.try_collect().await?;
        let attribute = get_attribute(context, var.name);
        containment.assert(&actuals, attribute);
        Ok(())
    }

    #[step(expr = "attribute {var} get owners {maybe_contain}: {var}")]
    async fn attribute_get_owners_contain(
        context: &mut Context,
        var: VarParse,
        containment: ContainmentParse,
        owner_var: VarParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let attribute = get_attribute(context, var.name);
        let actuals: Vec<Thing> = attribute.get_owners(tx, None)?.try_collect().await?;
        let expected = get_thing(context, owner_var.name);
        containment.assert(&actuals, expected);
        Ok(())
    }

    #[step(expr = "attribute {var} has value type: {value_type}")]
    async fn attribute_has_value_type(context: &mut Context, var: VarParse, value_type: ValueTypeParse) {
        assert_eq!(get_attribute(context, var.name).type_.value_type, value_type.into());
    }

    #[step(expr = r"{var} = attribute\(( ){label}( )\) as\(( ){value_type}( )\) put: {value}")]
    async fn attribute_put_value(
        context: &mut Context,
        var: VarParse,
        type_label: LabelParse,
        value_type: ValueTypeParse,
        value: ValueParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let attribute_type = get_attribute_type(tx, type_label.into()).await?;
        assert_eq!(attribute_type.value_type, value_type.into());
        let attribute = attribute_type.put(tx, value.into_value(value_type.into())).await?;
        context.things.insert(var.name, Some(Thing::Attribute(attribute)));
        Ok(())
    }

    #[step(expr = r"attribute\(( ){label}( )\) as\(( ){value_type}( )\) put: {value}; throws exception")]
    async fn attribute_put_value_throws(
        context: &mut Context,
        type_label: LabelParse,
        value_type: ValueTypeParse,
        value: ValueParse,
    ) {
        assert!(attribute_put_value(context, VarParse { name: "".to_owned() }, type_label, value_type, value)
            .await
            .is_err());
    }

    #[step(expr = r"{var} = attribute\(( ){label}( )\) as\(( ){value_type}( )\) get: {value}")]
    async fn attribute_get_value(
        context: &mut Context,
        var: VarParse,
        type_label: LabelParse,
        value_type: ValueTypeParse,
        value: ValueParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let attribute_type = get_attribute_type(tx, type_label.into()).await?;
        assert_eq!(attribute_type.value_type, value_type.into());
        let attribute = attribute_type.get(tx, value.into_value(value_type.into())).await?;
        context.things.insert(var.name, attribute.map(Thing::Attribute));
        Ok(())
    }

    #[step(expr = "attribute {var} has {value_type} value: {value}")]
    async fn attribute_has_value(context: &mut Context, var: VarParse, value_type: ValueTypeParse, value: ValueParse) {
        assert_eq!(get_attribute(context, var.name).value, value.into_value(value_type.into()));
    }
}
