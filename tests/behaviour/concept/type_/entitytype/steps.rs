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
use futures::TryStreamExt;
use typedb_client::{
    concept::{EntityType, ScopedLabel},
    Result as TypeDBResult,
};

use crate::{
    behaviour::{
        concept::common::{get_attribute_type, get_entity_type, get_relation_type},
        parameter::{
            AnnotationsParse, ContainmentParse, LabelParse, OverrideLabelParse, OverrideScopedLabelParse,
            ScopedLabelParse, TransitivityParse,
        },
        util::iter_table,
        Context,
    },
    generic_step_impl,
};

generic_step_impl! {
    #[step(expr = "put entity type: {label}")]
    async fn put_entity_type(context: &mut Context, type_label: LabelParse) -> TypeDBResult<EntityType> {
        context.transaction().concept().put_entity_type(type_label.into()).await
    }

    #[step(expr = "delete entity type: {label}")]
    async fn delete_entity_type(context: &mut Context, type_label: LabelParse) -> TypeDBResult {
        let tx = context.transaction();
        get_entity_type(tx, type_label.into()).await?.delete(tx).await
    }

    #[step(expr = "delete entity type: {label}; throws exception")]
    async fn delete_entity_type_throws(context: &mut Context, type_label: LabelParse) {
        assert!(delete_entity_type(context, type_label).await.is_err());
    }

    #[step(expr = r"entity\(( ){label}( )\) is null: {word}")]
    async fn entity_type_is_null(context: &mut Context, type_label: LabelParse, is_null: bool) -> TypeDBResult {
        let res = context.transaction().concept().get_entity_type(type_label.into()).await?;
        assert_eq!(res.is_none(), is_null, "{res:?}");
        Ok(())
    }

    #[step(expr = r"entity\(( ){label}( )\) set label: {label}")]
    async fn entity_type_set_label(
        context: &mut Context,
        type_label: LabelParse,
        new_label: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        get_entity_type(tx, type_label.into()).await?.set_label(tx, new_label.into()).await
    }

    #[step(expr = r"entity\(( ){label}( )\) get label: {label}")]
    async fn entity_type_get_label(
        context: &mut Context,
        type_label: LabelParse,
        get_label: LabelParse,
    ) -> TypeDBResult {
        assert_eq!(get_entity_type(context.transaction(), type_label.into()).await?.label, get_label.name);
        Ok(())
    }

    #[step(expr = r"entity\(( ){label}( )\) set abstract: {word}")]
    async fn entity_type_set_abstract(
        context: &mut Context,
        type_label: LabelParse,
        is_abstract: bool,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut entity_type = get_entity_type(tx, type_label.into()).await?;
        if is_abstract {
            entity_type.set_abstract(tx).await
        } else {
            entity_type.unset_abstract(tx).await
        }
    }

    #[step(expr = r"entity\(( ){label}( )\) set abstract: {word}; throws exception")]
    async fn entity_type_set_abstract_throws(context: &mut Context, type_label: LabelParse, is_abstract: bool) {
        assert!(entity_type_set_abstract(context, type_label, is_abstract).await.is_err());
    }

    #[step(expr = r"entity\(( ){label}( )\) is abstract: {word}")]
    async fn entity_type_is_abstract(context: &mut Context, type_label: LabelParse, is_abstract: bool) -> TypeDBResult {
        assert_eq!(get_entity_type(context.transaction(), type_label.into()).await?.is_abstract, is_abstract);
        Ok(())
    }

    #[step(expr = r"entity\(( ){label}( )\) set supertype: {label}")]
    async fn entity_type_set_supertype(
        context: &mut Context,
        type_label: LabelParse,
        supertype_label: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let supertype = get_entity_type(tx, supertype_label.into()).await?;
        get_entity_type(tx, type_label.into()).await?.set_supertype(tx, supertype).await
    }

    #[step(expr = r"entity\(( ){label}( )\) set supertype: {label}; throws exception")]
    async fn entity_type_set_supertype_throws(
        context: &mut Context,
        type_label: LabelParse,
        supertype_label: LabelParse,
    ) {
        assert!(entity_type_set_supertype(context, type_label, supertype_label).await.is_err())
    }

    #[step(expr = r"entity\(( ){label}( )\) get supertype: {label}")]
    async fn entity_type_get_supertype(
        context: &mut Context,
        type_label: LabelParse,
        supertype: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        assert_eq!(get_entity_type(tx, type_label.into()).await?.get_supertype(tx).await?.label, supertype.name);
        Ok(())
    }

    #[step(expr = r"entity\(( ){label}( )\) get supertypes {maybe_contain}:")]
    async fn entity_type_get_supertypes(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let entity_type = get_entity_type(tx, type_label.into()).await?;
        let actuals = entity_type.get_supertypes(tx)?.map_ok(|et| et.label).try_collect::<Vec<_>>().await?;
        for supertype in iter_table(step) {
            containment.assert(&actuals, supertype);
        }
        Ok(())
    }

    #[step(expr = r"entity\(( ){label}( )\) get subtypes {maybe_contain}:")]
    async fn entity_type_get_subtypes(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let entity_type = get_entity_type(tx, type_label.into()).await?;
        let actuals = entity_type.get_subtypes(tx)?.map_ok(|et| et.label).try_collect::<Vec<_>>().await?;
        for subtype in iter_table(step) {
            containment.assert(&actuals, subtype);
        }
        Ok(())
    }

    #[step(expr = r"entity\(( ){label}( )\) set owns attribute type: {label}{override_label}{annotations}")]
    async fn entity_type_set_owns_attribute_type(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
        overridden_attribute_type_label: OverrideLabelParse,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut entity_type = get_entity_type(tx, type_label.into()).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label.into()).await?;
        let overridden_attribute_type = if let Some(label) = overridden_attribute_type_label.name {
            Some(get_attribute_type(tx, label).await?)
        } else {
            None
        };
        entity_type.set_owns(tx, attribute_type, overridden_attribute_type, annotations.into()).await
    }

    #[step(
        expr = r"entity\(( ){label}( )\) set owns attribute type: {label}{override_label}{annotations}; throws exception"
    )]
    async fn entity_type_set_owns_attribute_type_throws(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
        overridden_attribute_type_label: OverrideLabelParse,
        annotations: AnnotationsParse,
    ) {
        assert!(entity_type_set_owns_attribute_type(
            context,
            type_label,
            attribute_type_label,
            overridden_attribute_type_label,
            annotations
        )
        .await
        .is_err());
    }

    #[step(expr = r"entity\(( ){label}( )\) unset owns attribute type: {label}")]
    async fn entity_type_unset_owns_attribute_type(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut entity_type = get_entity_type(tx, type_label.into()).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label.into()).await?;
        assert!(entity_type.unset_owns(tx, attribute_type).await.is_ok());
        Ok(())
    }

    #[step(expr = r"entity\(( ){label}( )\) unset owns attribute type: {label}; throws exception")]
    async fn entity_type_unset_owns_attribute_type_throws(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
    ) {
        assert!(entity_type_unset_owns_attribute_type(context, type_label, attribute_type_label).await.is_err());
    }

    #[step(expr = r"entity\(( ){label}( )\) get owns{maybe_explicit} attribute types{annotations}(;) {maybe_contain}:")]
    async fn entity_type_get_owns_attribute_types_contain(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        transitivity: TransitivityParse,
        annotations: AnnotationsParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let entity_type = get_entity_type(tx, type_label.into()).await?;
        let actuals: Vec<String> = entity_type
            .get_owns(tx, None, transitivity.into(), annotations.into())?
            .map_ok(|at| at.label)
            .try_collect()
            .await?;
        for attribute in iter_table(step) {
            containment.assert(&actuals, attribute);
        }
        Ok(())
    }

    #[step(expr = r"entity\(( ){label}( )\) get owns overridden attribute\(( ){label}( )\) is null: {word}")]
    async fn entity_type_get_owns_attribute_type(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
        is_null: bool,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let entity_type = get_entity_type(tx, type_label.into()).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label.into()).await?;
        let res = entity_type.get_owns_overridden(tx, attribute_type).await?;
        assert_eq!(res.is_none(), is_null);
        Ok(())
    }

    #[step(expr = r"entity\(( ){label}( )\) get owns overridden attribute\(( ){label}( )\) get label: {label}")]
    async fn entity_type_get_owns_attribute_type_label(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
        overridden_label: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let entity_type = get_entity_type(tx, type_label.into()).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label.into()).await?;
        let res = entity_type.get_owns_overridden(tx, attribute_type).await?;
        assert_eq!(res.map(|at| at.label), Some(overridden_label.into()));
        Ok(())
    }

    #[step(expr = r"entity\(( ){label}( )\) set plays role: {scoped_label}{override_scoped_label}")]
    async fn entity_type_set_plays_role(
        context: &mut Context,
        type_label: LabelParse,
        role_label: ScopedLabelParse,
        overridden_role_label: OverrideScopedLabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut entity_type = get_entity_type(tx, type_label.into()).await?;
        let role = get_relation_type(tx, role_label.scope)
            .await?
            .get_relates_for_role_label(tx, role_label.name)
            .await?
            .unwrap();
        let overridden_role =
            if let OverrideScopedLabelParse { scope: Some(scope), name: Some(name) } = overridden_role_label {
                Some(get_relation_type(tx, scope).await?.get_relates_for_role_label(tx, name).await?.unwrap())
            } else {
                None
            };
        entity_type.set_plays(tx, role, overridden_role).await
    }

    #[step(expr = r"entity\(( ){label}( )\) set plays role: {scoped_label}{override_scoped_label}; throws exception")]
    async fn entity_type_set_plays_role_throws(
        context: &mut Context,
        type_label: LabelParse,
        role_label: ScopedLabelParse,
        overridden_role_label: OverrideScopedLabelParse,
    ) {
        assert!(entity_type_set_plays_role(context, type_label, role_label, overridden_role_label).await.is_err());
    }

    #[step(expr = r"entity\(( ){label}( )\) unset plays role: {scoped_label}")]
    async fn entity_type_unset_plays_role(
        context: &mut Context,
        type_label: LabelParse,
        role_label: ScopedLabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut entity_type = get_entity_type(tx, type_label.into()).await?;
        let role = get_relation_type(tx, role_label.scope)
            .await?
            .get_relates_for_role_label(tx, role_label.name)
            .await?
            .unwrap();
        entity_type.unset_plays(tx, role).await
    }

    #[step(expr = r"entity\(( ){label}( )\) unset plays role: {scoped_label}; throws exception")]
    async fn entity_type_unset_plays_role_throws(
        context: &mut Context,
        type_label: LabelParse,
        role_label: ScopedLabelParse,
    ) {
        assert!(entity_type_unset_plays_role(context, type_label, role_label).await.is_err());
    }

    #[step(expr = r"entity\(( ){label}( )\) get playing roles{maybe_explicit} {maybe_contain}:")]
    async fn entity_type_get_playing_roles_contain(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        transitivity: TransitivityParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let entity_type = get_entity_type(tx, type_label.into()).await?;
        let actuals: Vec<ScopedLabel> =
            entity_type.get_plays(tx, transitivity.into())?.map_ok(|rt| rt.label).try_collect().await?;
        for role_label in iter_table(step) {
            let role_label: ScopedLabel = role_label.parse::<ScopedLabelParse>().unwrap().into();
            containment.assert(&actuals, &role_label);
        }
        Ok(())
    }
}
