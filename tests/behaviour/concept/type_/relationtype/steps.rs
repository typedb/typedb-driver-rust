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
    concept::{RelationType, ScopedLabel},
    Result as TypeDBResult, Transitivity,
};

use crate::{
    assert_err,
    behaviour::{
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
    #[step(expr = "put relation type: {label}")]
    async fn put_relation_type(context: &mut Context, type_label: LabelParse) -> TypeDBResult<RelationType> {
        context.transaction().concept().put_relation_type(type_label.into()).await
    }

    #[step(expr = "delete relation type: {label}")]
    async fn delete_relation_type(context: &mut Context, type_label: LabelParse) -> TypeDBResult {
        let tx = context.transaction();
        context.get_relation_type(type_label.into()).await?.delete(tx).await
    }

    #[step(expr = "delete relation type: {label}; throws exception")]
    async fn delete_relation_type_throws(context: &mut Context, type_label: LabelParse) {
        assert_err!(delete_relation_type(context, type_label).await);
    }

    #[step(expr = r"relation\(( ){label}( )\) is null: {word}")]
    async fn relation_type_is_null(context: &mut Context, type_label: LabelParse, is_null: bool) -> TypeDBResult {
        let res = context.transaction().concept().get_relation_type(type_label.into()).await?;
        assert_eq!(res.is_none(), is_null, "{res:?}");
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )\) set label: {label}")]
    async fn relation_type_set_label(
        context: &mut Context,
        type_label: LabelParse,
        new_label: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        context.get_relation_type(type_label.into()).await?.set_label(tx, new_label.into()).await
    }

    #[step(expr = r"relation\(( ){label}( )\) get label: {label}")]
    async fn relation_type_get_label(
        context: &mut Context,
        type_label: LabelParse,
        get_label: LabelParse,
    ) -> TypeDBResult {
        assert_eq!(context.get_relation_type(type_label.into()).await?.label, get_label.name);
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )\) set abstract: {word}")]
    async fn relation_type_set_abstract(
        context: &mut Context,
        type_label: LabelParse,
        is_abstract: bool,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = context.get_relation_type(type_label.into()).await?;
        if is_abstract {
            relation_type.set_abstract(tx).await
        } else {
            relation_type.unset_abstract(tx).await
        }
    }

    #[step(expr = r"relation\(( ){label}( )\) set abstract: {word}; throws exception")]
    async fn relation_type_set_abstract_throws(context: &mut Context, type_label: LabelParse, is_abstract: bool) {
        assert_err!(relation_type_set_abstract(context, type_label, is_abstract).await);
    }

    #[step(expr = r"relation\(( ){label}( )\) is abstract: {word}")]
    async fn relation_type_is_abstract(
        context: &mut Context,
        type_label: LabelParse,
        is_abstract: bool,
    ) -> TypeDBResult {
        assert_eq!(context.get_relation_type(type_label.into()).await?.is_abstract, is_abstract);
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )\) set supertype: {label}")]
    async fn relation_type_set_supertype(
        context: &mut Context,
        type_label: LabelParse,
        supertype_label: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let supertype = context.get_relation_type(supertype_label.into()).await?;
        context.get_relation_type(type_label.into()).await?.set_supertype(tx, supertype).await
    }

    #[step(expr = r"relation\(( ){label}( )\) set supertype: {label}; throws exception")]
    async fn relation_type_set_supertype_throws(
        context: &mut Context,
        type_label: LabelParse,
        supertype_label: LabelParse,
    ) {
        assert_err!(relation_type_set_supertype(context, type_label, supertype_label).await)
    }

    #[step(expr = r"relation\(( ){label}( )\) get supertype: {label}")]
    async fn relation_type_get_supertype(
        context: &mut Context,
        type_label: LabelParse,
        supertype: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        assert_eq!(context.get_relation_type(type_label.into()).await?.get_supertype(tx).await?.label, supertype.name);
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )\) get supertypes {maybe_contain}:")]
    async fn relation_type_get_supertypes(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let actuals = relation_type.get_supertypes(tx)?.map_ok(|rt| rt.label).try_collect::<Vec<_>>().await?;
        for supertype in iter_table(step) {
            containment.assert(&actuals, supertype);
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )\) get subtypes {maybe_contain}:")]
    async fn relation_type_get_subtypes(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let actuals: Vec<String> = relation_type.get_subtypes(tx)?.map_ok(|rt| rt.label).try_collect().await?;
        for subtype in iter_table(step) {
            containment.assert(&actuals, subtype);
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )\) set owns attribute type: {label}{override_label}{annotations}")]
    async fn relation_type_set_owns_attribute_type(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
        overridden_attribute_type_label: OverrideLabelParse,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = context.get_relation_type(type_label.into()).await?;
        let attribute_type = context.get_attribute_type(attribute_type_label.into()).await?;
        let overridden_attribute_type = if let Some(label) = overridden_attribute_type_label.name {
            Some(context.get_attribute_type(label).await?)
        } else {
            None
        };
        relation_type.set_owns(tx, attribute_type, overridden_attribute_type, annotations.into()).await
    }

    #[step(
        expr = r"relation\(( ){label}( )\) set owns attribute type: {label}{override_label}{annotations}; throws exception"
    )]
    async fn relation_type_set_owns_attribute_type_throws(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
        overridden_attribute_type_label: OverrideLabelParse,
        annotations: AnnotationsParse,
    ) {
        assert_err!(
            relation_type_set_owns_attribute_type(
                context,
                type_label,
                attribute_type_label,
                overridden_attribute_type_label,
                annotations
            )
            .await
        );
    }

    #[step(expr = r"relation\(( ){label}( )\) unset owns attribute type: {label}")]
    async fn relation_type_unset_owns_attribute_type(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = context.get_relation_type(type_label.into()).await?;
        let attribute_type = context.get_attribute_type(attribute_type_label.into()).await?;
        relation_type.unset_owns(tx, attribute_type).await
    }

    #[step(expr = r"relation\(( ){label}( )\) unset owns attribute type: {label}; throws exception")]
    async fn relation_type_unset_owns_attribute_type_throws(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
    ) {
        assert_err!(relation_type_unset_owns_attribute_type(context, type_label, attribute_type_label).await);
    }

    #[step(
        expr = r"relation\(( ){label}( )\) get owns{maybe_explicit} attribute types{annotations}(;) {maybe_contain}:"
    )]
    async fn relation_type_get_owns_attribute_types_contain(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        transitivity: TransitivityParse,
        annotations: AnnotationsParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let actuals: Vec<String> = relation_type
            .get_owns(tx, None, transitivity.into(), annotations.into())?
            .map_ok(|at| at.label)
            .try_collect()
            .await?;
        for attribute in iter_table(step) {
            containment.assert(&actuals, attribute);
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )\) get owns overridden attribute\(( ){label}( )\) is null: {word}")]
    async fn relation_type_get_owns_overridden_attribute_type(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
        is_null: bool,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let attribute_type = context.get_attribute_type(attribute_type_label.into()).await?;
        let res = relation_type.get_owns_overridden(tx, attribute_type).await?;
        assert_eq!(res.is_none(), is_null);
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )\) get owns overridden attribute\(( ){label}( )\) get label: {label}")]
    async fn relation_type_get_owns_overridden_attribute_type_label(
        context: &mut Context,
        type_label: LabelParse,
        attribute_type_label: LabelParse,
        overridden_label: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let attribute_type = context.get_attribute_type(attribute_type_label.into()).await?;
        let res = relation_type.get_owns_overridden(tx, attribute_type).await?;
        assert_eq!(res.map(|at| at.label), Some(overridden_label.into()));
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )\) set plays role: {scoped_label}{override_scoped_label}")]
    async fn relation_type_set_plays_role(
        context: &mut Context,
        type_label: LabelParse,
        role_label: ScopedLabelParse,
        overridden_role_label: OverrideScopedLabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(role_label.scope).await?;
        let role = relation_type.get_relates_for_role_label(tx, role_label.name).await?.unwrap();
        // FIXME
        let overridden_role =
            if let OverrideScopedLabelParse { scope: Some(scope), name: Some(name) } = overridden_role_label {
                Some(context.get_relation_type(scope).await?.get_relates_for_role_label(tx, name).await?.unwrap())
            } else {
                None
            };
        let mut relation_type = context.get_relation_type(type_label.into()).await?;
        relation_type.set_plays(tx, role, overridden_role).await
    }

    #[step(expr = r"relation\(( ){label}( )\) set plays role: {scoped_label}{override_scoped_label}; throws exception")]
    async fn relation_type_set_plays_role_throws(
        context: &mut Context,
        type_label: LabelParse,
        role_label: ScopedLabelParse,
        overridden_role_label: OverrideScopedLabelParse,
    ) {
        assert_err!(relation_type_set_plays_role(context, type_label, role_label, overridden_role_label).await);
    }

    #[step(expr = r"relation\(( ){label}( )\) unset plays role: {scoped_label}")]
    async fn relation_type_unset_plays_role(
        context: &mut Context,
        type_label: LabelParse,
        role_label: ScopedLabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(role_label.scope).await?;
        let role = relation_type.get_relates_for_role_label(tx, role_label.name).await?.unwrap();
        let mut relation_type = context.get_relation_type(type_label.into()).await?;
        relation_type.unset_plays(tx, role).await
    }

    #[step(expr = r"relation\(( ){label}( )\) unset plays role: {scoped_label}; throws exception")]
    async fn relation_type_unset_plays_role_throws(
        context: &mut Context,
        type_label: LabelParse,
        role_label: ScopedLabelParse,
    ) {
        assert_err!(relation_type_unset_plays_role(context, type_label, role_label).await);
    }

    #[step(expr = r"relation\(( ){label}( )\) get playing roles{maybe_explicit} {maybe_contain}:")]
    async fn relation_type_get_playing_roles_contain(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        transitivity: TransitivityParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let actuals: Vec<ScopedLabel> =
            relation_type.get_plays(tx, transitivity.into())?.map_ok(|rt| rt.label).try_collect().await?;
        for role_label in iter_table(step) {
            let role_label: ScopedLabel = role_label.parse::<ScopedLabelParse>().unwrap().into();
            containment.assert(&actuals, &role_label);
        }
        Ok(())
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[step(expr = r"relation\(( ){label}( )) set relates role: {label}{override_label}")]
    async fn relation_type_set_relates(
        context: &mut Context,
        type_label: LabelParse,
        role_name: LabelParse,
        overridden_role_name: OverrideLabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = context.get_relation_type(type_label.into()).await?;
        relation_type.set_relates(tx, role_name.into(), overridden_role_name.into()).await
    }

    #[step(expr = r"relation\(( ){label}( )) set relates role: {label}{override_label}; throws exception")]
    async fn relation_type_set_relates_throws(
        context: &mut Context,
        type_label: LabelParse,
        role_name: LabelParse,
        overridden_role_name: OverrideLabelParse,
    ) {
        assert_err!(relation_type_set_relates(context, type_label, role_name, overridden_role_name).await);
    }

    #[step(expr = r"relation\(( ){label}( )) unset related role: {label}")]
    async fn relation_type_unset_related(
        context: &mut Context,
        type_label: LabelParse,
        role_name: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = context.get_relation_type(type_label.into()).await?;
        relation_type.unset_relates(tx, role_name.into()).await
    }

    #[step(expr = r"relation\(( ){label}( )) unset related role: {label}; throws exception")]
    async fn relation_type_unset_related_throws(context: &mut Context, type_label: LabelParse, role_name: LabelParse) {
        assert_err!(relation_type_unset_related(context, type_label, role_name).await);
    }

    #[step(expr = r"relation\(( ){label}( )) get role\(( ){label}( )) is null: {word}")]
    async fn relation_type_get_role_is_null(
        context: &mut Context,
        type_label: LabelParse,
        role_name: LabelParse,
        is_null: bool,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        assert_eq!(relation_type.get_relates_for_role_label(tx, role_name.into()).await?.is_none(), is_null);
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )) get overridden role\(( ){label}( )) is null: {word}")]
    async fn relation_type_get_overridden_role_is_null(
        context: &mut Context,
        type_label: LabelParse,
        role_name: LabelParse,
        is_null: bool,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        assert_eq!(relation_type.get_relates_overridden(tx, role_name.into()).await?.is_none(), is_null);
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )) get role\(( ){label}( )) set label: {label}")]
    async fn relation_type_set_role_label(
        context: &mut Context,
        type_label: LabelParse,
        role_name: LabelParse,
        new_name: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let role_type = relation_type.get_relates_for_role_label(tx, role_name.into()).await?.unwrap();
        role_type.set_label(tx, new_name.name).await
    }

    #[step(expr = r"relation\(( ){label}( )) get role\(( ){label}( )) get label: {label}")]
    async fn relation_type_get_role_label(
        context: &mut Context,
        type_label: LabelParse,
        role_name: LabelParse,
        expected_name: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let role_type = relation_type.get_relates_for_role_label(tx, role_name.into()).await?.unwrap();
        assert_eq!(role_type.label.name, expected_name.name);
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )) get overridden role\(( ){label}( )) get label: {label}")]
    async fn relation_type_get_overridden_role_label(
        context: &mut Context,
        type_label: LabelParse,
        role_name: LabelParse,
        expected_name: LabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let role_type = relation_type.get_relates_overridden(tx, role_name.into()).await?.unwrap();
        assert_eq!(role_type.label.name, expected_name.name);
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )) get role\(( ){label}( )) is abstract: {word}")]
    async fn relation_type_role_is_abstract(
        context: &mut Context,
        type_label: LabelParse,
        role_name: LabelParse,
        is_abstract: bool,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        assert_eq!(
            relation_type.get_relates_for_role_label(tx, role_name.name).await?.unwrap().is_abstract,
            is_abstract
        );
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )) get related{maybe_explicit} roles {maybe_contain}:")]
    async fn relation_type_get_related_roles(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        transitivity: TransitivityParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let actuals: Vec<ScopedLabel> =
            relation_type.get_relates(tx, transitivity.into())?.map_ok(|rt| rt.label).try_collect().await?;
        for role_label in iter_table(step) {
            let role_label: ScopedLabel = if role_label.contains(':') {
                role_label.parse::<ScopedLabelParse>().unwrap().into()
            } else {
                ScopedLabel { scope: relation_type.label.clone(), name: role_label.to_owned() }
            };
            containment.assert(&actuals, &role_label);
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )) get role\(( ){label}( )) get supertype: {scoped_label}")]
    async fn relation_type_get_role_get_supertype(
        context: &mut Context,
        type_label: LabelParse,
        role_name: LabelParse,
        supertype: ScopedLabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let role_type = relation_type.get_relates_for_role_label(tx, role_name.into()).await?.unwrap();
        assert_eq!(role_type.get_supertype(tx).await?.label, ScopedLabel::from(supertype));
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )) get role\(( ){label}( )) get supertypes {maybe_contain}:")]
    async fn relation_type_get_role_get_supertypes(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        role_name: LabelParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let role_type = relation_type.get_relates_for_role_label(tx, role_name.into()).await?.unwrap();
        let actuals: Vec<ScopedLabel> = role_type.get_supertypes(tx)?.map_ok(|rt| rt.label).try_collect().await?;
        for supertype in iter_table(step) {
            let supertype: ScopedLabel = supertype.parse::<ScopedLabelParse>().unwrap().into();
            containment.assert(&actuals, &supertype);
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )) get role\(( ){label}( )) get players {maybe_contain}:")]
    async fn relation_type_get_role_players(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        role_name: LabelParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let role_type = relation_type.get_relates_for_role_label(tx, role_name.into()).await?.unwrap();
        let actuals: Vec<String> = role_type
            .get_player_types(tx, Transitivity::Transitive)?
            .map_ok(|t| t.label().to_owned())
            .try_collect()
            .await?;
        for player_type in iter_table(step) {
            containment.assert(&actuals, player_type);
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){label}( )) get role\(( ){label}( )) get subtypes {maybe_contain}:")]
    async fn relation_type_get_role_subtypes(
        context: &mut Context,
        step: &Step,
        type_label: LabelParse,
        role_name: LabelParse,
        containment: ContainmentParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = context.get_relation_type(type_label.into()).await?;
        let role_type = relation_type.get_relates_for_role_label(tx, role_name.into()).await?.unwrap();
        let actuals: Vec<ScopedLabel> =
            role_type.get_subtypes(tx, Transitivity::Transitive)?.map_ok(|rt| rt.label).try_collect().await?;
        for subtype in iter_table(step) {
            let subtype: ScopedLabel = subtype.parse::<ScopedLabelParse>().unwrap().into();
            containment.assert(&actuals, &subtype);
        }
        Ok(())
    }
}
