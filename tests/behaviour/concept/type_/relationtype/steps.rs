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
    Annotation, Result as TypeDBResult, Transitivity,
};

use crate::{
    behaviour::{
        concept::common::{get_attribute_type, get_relation_type},
        parameter::{AnnotationsParse, ScopedLabelParse},
        util::iter_table,
        Context,
    },
    generic_step_impl,
};

async fn relation_type_get_owns_attribute_types(
    context: &mut Context,
    type_label: String,
    transitivity: Transitivity,
    annotations: &[Annotation],
) -> TypeDBResult<Vec<String>> {
    let tx = context.transaction();
    let relation_type = get_relation_type(tx, type_label).await?;
    relation_type.get_owns(tx, None, transitivity, annotations)?.map_ok(|at| at.label).try_collect().await
}

async fn relation_type_get_playing_roles(
    context: &mut Context,
    type_label: String,
    transitivity: Transitivity,
) -> TypeDBResult<Vec<ScopedLabel>> {
    let tx = context.transaction();
    let relation_type = get_relation_type(tx, type_label).await?;
    relation_type.get_plays(tx, transitivity)?.map_ok(|at| at.label).try_collect().await
}

generic_step_impl! {
    #[step(expr = "put relation type: {word}")]
    async fn put_relation_type(context: &mut Context, type_label: String) -> TypeDBResult<RelationType> {
        context.transaction().concept().put_relation_type(type_label).await
    }

    #[step(expr = "delete relation type: {word}")]
    async fn delete_relation_type(context: &mut Context, type_label: String) -> TypeDBResult {
        let tx = context.transaction();
        get_relation_type(tx, type_label).await?.delete(tx).await
    }

    #[step(expr = "delete relation type: {word}; throws exception")]
    async fn delete_relation_type_throws(context: &mut Context, type_label: String) {
        assert!(delete_relation_type(context, type_label).await.is_err());
    }

    #[step(expr = r"relation\(( ){word}( )\) is null: {word}")]
    async fn relation_type_is_null(context: &mut Context, type_label: String, is_null: bool) -> TypeDBResult {
        let res = context.transaction().concept().get_relation_type(type_label).await?;
        assert_eq!(res.is_none(), is_null, "{res:?}");
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) set label: {word}")]
    async fn relation_type_set_label(context: &mut Context, type_label: String, new_label: String) -> TypeDBResult {
        let tx = context.transaction();
        get_relation_type(tx, type_label).await?.set_label(tx, new_label).await
    }

    #[step(expr = r"relation\(( ){word}( )\) get label: {word}")]
    async fn relation_type_get_label(context: &mut Context, type_label: String, get_label: String) -> TypeDBResult {
        assert_eq!(get_relation_type(context.transaction(), type_label).await?.label, get_label);
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) set abstract: {word}")]
    async fn relation_type_set_abstract(context: &mut Context, type_label: String, is_abstract: bool) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await?;
        if is_abstract {
            relation_type.set_abstract(tx).await
        } else {
            relation_type.unset_abstract(tx).await
        }
    }

    #[step(expr = r"relation\(( ){word}( )\) set abstract: {word}; throws exception")]
    async fn relation_type_set_abstract_throws(context: &mut Context, type_label: String, is_abstract: bool) {
        assert!(relation_type_set_abstract(context, type_label, is_abstract).await.is_err());
    }

    #[step(expr = r"relation\(( ){word}( )\) is abstract: {word}")]
    async fn relation_type_is_abstract(context: &mut Context, type_label: String, is_abstract: bool) -> TypeDBResult {
        assert_eq!(get_relation_type(context.transaction(), type_label).await?.is_abstract, is_abstract);
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) set supertype: {word}")]
    async fn relation_set_supertype(context: &mut Context, type_label: String, supertype_label: String) -> TypeDBResult {
        let tx = context.transaction();
        let supertype = get_relation_type(tx, supertype_label).await?;
        get_relation_type(tx, type_label).await?.set_supertype(tx, supertype).await
    }

    #[step(expr = r"relation\(( ){word}( )\) set supertype: {word}; throws exception")]
    async fn relation_set_supertype_throws(context: &mut Context, type_label: String, supertype_label: String) {
        assert!(relation_set_supertype(context, type_label, supertype_label).await.is_err())
    }

    #[step(expr = r"relation\(( ){word}( )\) get supertype: {word}")]
    async fn relation_get_supertype(context: &mut Context, type_label: String, supertype: String) -> TypeDBResult {
        let tx = context.transaction();
        assert_eq!(get_relation_type(tx, type_label).await?.get_supertype(tx).await?.label, supertype);
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get supertypes contain:")]
    async fn relation_get_supertypes_contain(context: &mut Context, step: &Step, type_label: String) -> TypeDBResult {
        let tx = context.transaction();
        let actuals = get_relation_type(tx, type_label)
            .await?
            .get_supertypes(tx)?
            .map_ok(|et| et.label)
            .try_collect::<Vec<_>>()
            .await?;
        for supertype in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == supertype));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get supertypes do not contain:")]
    async fn relation_get_supertypes_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let actuals = get_relation_type(tx, type_label)
            .await?
            .get_supertypes(tx)?
            .map_ok(|et| et.label)
            .try_collect::<Vec<_>>()
            .await?;
        for supertype in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != supertype));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get subtypes contain:")]
    async fn relation_get_subtypes_contain(context: &mut Context, step: &Step, type_label: String) -> TypeDBResult {
        let tx = context.transaction();
        let actuals = get_relation_type(tx, type_label)
            .await?
            .get_subtypes(tx)?
            .map_ok(|et| et.label)
            .try_collect::<Vec<_>>()
            .await?;
        for subtype in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == subtype));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get subtypes do not contain:")]
    async fn relation_get_subtypes_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let actuals = get_relation_type(tx, type_label)
            .await?
            .get_subtypes(tx)?
            .map_ok(|et| et.label)
            .try_collect::<Vec<_>>()
            .await?;
        for subtype in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != subtype));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) set owns attribute type: {word}")]
    async fn relation_type_set_owns_attribute_type(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        // FIXME barf ~~~~~~~~~~~~~~~~~~~~~~~~~~~v~~~~~v
        relation_type.set_owns(tx, attribute_type, None, &[]).await
    }

    #[step(expr = r"relation\(( ){word}( )\) set owns attribute type: {word}; throws exception")]
    async fn relation_type_set_owns_attribute_type_throws(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
    ) {
        assert!(relation_type_set_owns_attribute_type(context, type_label, attribute_type_label).await.is_err());
    }

    #[step(expr = r"relation\(( ){word}( )\) set owns attribute type: {word}, with annotations: {annotations}")]
    async fn relation_type_set_owns_attribute_type_with_annotations(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        relation_type.set_owns(tx, attribute_type, None, &annotations).await
    }

    #[step(
        expr = r"relation\(( ){word}( )\) set owns attribute type: {word}, with annotations: {annotations}; throws exception"
    )]
    async fn relation_type_set_owns_attribute_type_with_annotations_throws(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        annotations: AnnotationsParse,
    ) {
        assert!(relation_type_set_owns_attribute_type_with_annotations(
            context,
            type_label,
            attribute_type_label,
            annotations
        )
        .await
        .is_err());
    }

    #[step(expr = r"relation\(( ){word}( )\) set owns attribute type: {word} as {word}")]
    async fn relation_type_set_owns_attribute_type_overridden(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        overridden_attribute_type_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        let overridden_attribute_type = get_attribute_type(tx, overridden_attribute_type_label).await?;
        // FIXME barf ~~~~~~~~~~~~~~~~~~~~~~~~~~~v~~~~~v
        relation_type.set_owns(tx, attribute_type, Some(overridden_attribute_type), &[]).await
    }

    #[step(expr = r"relation\(( ){word}( )\) set owns attribute type: {word} as {word}; throws exception")]
    async fn relation_type_set_owns_attribute_type_overridden_throws(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        overridden_attribute_type_label: String,
    ) {
        assert!(relation_type_set_owns_attribute_type_overridden(
            context,
            type_label,
            attribute_type_label,
            overridden_attribute_type_label
        )
        .await
        .is_err());
    }

    #[step(expr = r"relation\(( ){word}( )\) set owns attribute type: {word} as {word}, with annotations: {annotations}")]
    async fn relation_type_set_owns_attribute_type_overridden_with_annotations(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        overridden_attribute_type_label: String,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        let overridden_attribute_type = get_attribute_type(tx, overridden_attribute_type_label).await?;
        relation_type.set_owns(tx, attribute_type, Some(overridden_attribute_type), &annotations).await
    }

    #[step(
        expr = r"relation\(( ){word}( )\) set owns attribute type: {word} as {word}, with annotations: {annotations}; throws exception"
    )]
    async fn relation_type_set_owns_attribute_type_overridden_with_annotations_throws(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        overridden_attribute_type_label: String,
        annotations: AnnotationsParse,
    ) {
        assert!(relation_type_set_owns_attribute_type_overridden_with_annotations(
            context,
            type_label,
            attribute_type_label,
            overridden_attribute_type_label,
            annotations
        )
        .await
        .is_err());
    }

    #[step(expr = r"relation\(( ){word}( )\) unset owns attribute type: {word}")]
    async fn relation_type_unset_owns_attribute_type(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        assert!(relation_type.unset_owns(tx, attribute_type).await.is_ok());
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) unset owns attribute type: {word}; throws exception")]
    async fn relation_type_unset_owns_attribute_type_throws(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
    ) {
        assert!(relation_type_unset_owns_attribute_type(context, type_label, attribute_type_label).await.is_err());
    }

    #[step(expr = r"relation\(( ){word}( )\) get owns attribute types contain:")]
    async fn relation_type_get_owns_attribute_types_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals = relation_type_get_owns_attribute_types(context, type_label, Transitivity::Transitive, &[]).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get owns attribute types do not contain:")]
    async fn relation_type_get_owns_attribute_types_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals = relation_type_get_owns_attribute_types(context, type_label, Transitivity::Transitive, &[]).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get owns types with annotations: {annotations}; contain:")]
    async fn relation_type_get_owns_attribute_types_with_annotations_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let actuals =
            relation_type_get_owns_attribute_types(context, type_label, Transitivity::Transitive, &annotations).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute), "{attribute} not in {actuals:?}");
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get owns types with annotations: {annotations}; do not contain:")]
    async fn relation_type_get_owns_attribute_types_with_annotations_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let actuals =
            relation_type_get_owns_attribute_types(context, type_label, Transitivity::Transitive, &annotations).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get owns explicit attribute types contain:")]
    async fn relation_type_get_owns_explicit_attribute_types_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals = relation_type_get_owns_attribute_types(context, type_label, Transitivity::Explicit, &[]).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute), "{attribute} not in {actuals:?}");
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get owns explicit attribute types do not contain:")]
    async fn relation_type_get_owns_explicit_attribute_types_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals = relation_type_get_owns_attribute_types(context, type_label, Transitivity::Explicit, &[]).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get owns explicit types with annotations: {annotations}; contain:")]
    async fn relation_type_get_owns_explicit_attribute_types_with_annotations_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let actuals =
            relation_type_get_owns_attribute_types(context, type_label, Transitivity::Explicit, &annotations).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute), "{attribute} not in {actuals:?}");
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get owns explicit types with annotations: {annotations}; do not contain:")]
    async fn relation_type_get_owns_explicit_attribute_types_with_annotations_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let actuals =
            relation_type_get_owns_attribute_types(context, type_label, Transitivity::Explicit, &annotations).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get owns overridden attribute\(( ){word}( )\) is null: {word}")]
    async fn relation_type_get_owns_overridden_attribute_type(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        is_null: bool,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        let res = relation_type.get_owns_overridden(tx, attribute_type).await?;
        assert_eq!(res.is_none(), is_null);
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get owns overridden attribute\(( ){word}( )\) get label: {word}")]
    async fn relation_type_get_owns_overridden_attribute_type_label(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        overridden_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await?;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        let res = relation_type.get_owns_overridden(tx, attribute_type).await?;
        assert_eq!(res.map(|at| at.label), Some(overridden_label));
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) set plays role: {scoped_label}")]
    async fn relation_type_set_plays_role(
        context: &mut Context,
        type_label: String,
        role_label: ScopedLabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await?;
        let role = get_relation_type(tx, role_label.scope)
            .await?
            .get_relates_for_role_label(tx, role_label.name)
            .await?
            .unwrap();
        relation_type.set_plays(tx, role, None).await
    }

    #[step(expr = r"relation\(( ){word}( )\) set plays role: {scoped_label}; throws exception")]
    async fn relation_type_set_plays_role_throws(
        context: &mut Context,
        type_label: String,
        role_label: ScopedLabelParse,
    ) {
        assert!(relation_type_set_plays_role(context, type_label, role_label).await.is_err());
    }

    #[step(expr = r"relation\(( ){word}( )\) set plays role: {scoped_label} as {scoped_label}")]
    async fn relation_type_set_plays_role_overridden(
        context: &mut Context,
        type_label: String,
        role_label: ScopedLabelParse,
        overridden_role_label: ScopedLabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await?;
        let role = get_relation_type(tx, role_label.scope)
            .await?
            .get_relates_for_role_label(tx, role_label.name)
            .await?
            .unwrap();
        let overridden_role = get_relation_type(tx, overridden_role_label.scope)
            .await?
            .get_relates_for_role_label(tx, overridden_role_label.name)
            .await?
            .unwrap();
        relation_type.set_plays(tx, role, Some(overridden_role)).await
    }

    #[step(expr = r"relation\(( ){word}( )\) set plays role: {scoped_label} as {scoped_label}; throws exception")]
    async fn relation_type_set_plays_role_overridden_throws(
        context: &mut Context,
        type_label: String,
        role_label: ScopedLabelParse,
        overridden_role_label: ScopedLabelParse,
    ) {
        assert!(relation_type_set_plays_role_overridden(context, type_label, role_label, overridden_role_label)
            .await
            .is_err());
    }

    #[step(expr = r"relation\(( ){word}( )\) unset plays role: {scoped_label}")]
    async fn relation_type_unset_plays_role(
        context: &mut Context,
        type_label: String,
        role_label: ScopedLabelParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await?;
        let role = get_relation_type(tx, role_label.scope)
            .await?
            .get_relates_for_role_label(tx, role_label.name)
            .await?
            .unwrap();
        relation_type.unset_plays(tx, role).await
    }

    #[step(expr = r"relation\(( ){word}( )\) unset plays role: {scoped_label}; throws exception")]
    async fn relation_type_unset_plays_role_throws(
        context: &mut Context,
        type_label: String,
        role_label: ScopedLabelParse,
    ) {
        assert!(relation_type_unset_plays_role(context, type_label, role_label).await.is_err());
    }

    #[step(expr = r"relation\(( ){word}( )\) get playing roles contain:")]
    async fn relation_type_get_playing_roles_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals = relation_type_get_playing_roles(context, type_label, Transitivity::Transitive).await?;
        for role_label in iter_table(step) {
            let role_label: ScopedLabel = role_label.parse::<ScopedLabelParse>().unwrap().into();
            assert!(actuals.iter().any(|actual| *actual == role_label));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get playing roles do not contain:")]
    async fn relation_type_get_playing_roles_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals = relation_type_get_playing_roles(context, type_label, Transitivity::Transitive).await?;
        for role_label in iter_table(step) {
            let role_label: ScopedLabel = role_label.parse::<ScopedLabelParse>().unwrap().into();
            assert!(actuals.iter().all(|actual| *actual != role_label));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get playing roles explicit contain:")]
    async fn relation_type_get_playing_roles_explicit_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals = relation_type_get_playing_roles(context, type_label, Transitivity::Explicit).await?;
        for role_label in iter_table(step) {
            let role_label: ScopedLabel = role_label.parse::<ScopedLabelParse>().unwrap().into();
            assert!(actuals.iter().any(|actual| *actual == role_label));
        }
        Ok(())
    }

    #[step(expr = r"relation\(( ){word}( )\) get playing roles explicit do not contain:")]
    async fn relation_type_get_playing_roles_explicit_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals = relation_type_get_playing_roles(context, type_label, Transitivity::Explicit).await?;
        for role_label in iter_table(step) {
            let role_label: ScopedLabel = role_label.parse::<ScopedLabelParse>().unwrap().into();
            assert!(actuals.iter().all(|actual| *actual != role_label));
        }
        Ok(())
    }
}
