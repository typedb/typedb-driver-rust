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
    concept::{AttributeType, RelationType},
    Annotation, Result as TypeDBResult, Transaction, Transitivity,
};

use crate::{
    behaviour::{
        util::{iter_table, AnnotationsParse},
        Context,
    },
    generic_step_impl,
};

async fn get_relation_type(tx: &Transaction<'_>, type_label: String) -> TypeDBResult<RelationType> {
    tx.concept().get_relation_type(type_label).await.map(|relation_type| {
        assert!(relation_type.is_some());
        relation_type.unwrap()
    })
}

async fn get_attribute_type(tx: &Transaction<'_>, type_label: String) -> TypeDBResult<AttributeType> {
    tx.concept().get_attribute_type(type_label).await.map(|attribute_type| {
        assert!(attribute_type.is_some());
        attribute_type.unwrap()
    })
}

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

    #[step(regex = r"^relation\( ?(\S+) ?\) is null: (\S+)$")]
    async fn relation_type_is_null(context: &mut Context, type_label: String, is_null: bool) -> TypeDBResult {
        let res = context.transaction().concept().get_relation_type(type_label).await?;
        assert_eq!(res.is_none(), is_null, "{res:?}");
        Ok(())
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set label: (\S+)$")]
    async fn relation_type_set_label(context: &mut Context, type_label: String, new_label: String) -> TypeDBResult {
        let tx = context.transaction();
        get_relation_type(tx, type_label).await?.set_label(tx, new_label).await
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get label: (\S+)$")]
    async fn relation_type_get_label(context: &mut Context, type_label: String, get_label: String) -> TypeDBResult {
        assert_eq!(get_relation_type(context.transaction(), type_label).await?.label, get_label);
        Ok(())
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set abstract: (\S+)$")]
    async fn relation_type_set_abstract(context: &mut Context, type_label: String, is_abstract: bool) -> TypeDBResult {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await?;
        if is_abstract {
            relation_type.set_abstract(tx).await
        } else {
            relation_type.unset_abstract(tx).await
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set abstract: (\S+); throws exception$")]
    async fn relation_type_set_abstract_throws(context: &mut Context, type_label: String, is_abstract: bool) {
        assert!(relation_type_set_abstract(context, type_label, is_abstract).await.is_err());
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) is abstract: (\S+)$")]
    async fn relation_type_is_abstract(context: &mut Context, type_label: String, is_abstract: bool) -> TypeDBResult {
        assert_eq!(get_relation_type(context.transaction(), type_label).await?.is_abstract, is_abstract);
        Ok(())
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set supertype: (\S+)$")]
    async fn relation_set_supertype(
        context: &mut Context,
        type_label: String,
        supertype_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let supertype = get_relation_type(tx, supertype_label).await?;
        get_relation_type(tx, type_label).await?.set_supertype(tx, supertype).await
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set supertype: (\S+); throws exception$")]
    async fn relation_set_supertype_throws(context: &mut Context, type_label: String, supertype_label: String) {
        assert!(relation_set_supertype(context, type_label, supertype_label).await.is_err())
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get supertype: (\S+)$")]
    async fn relation_get_supertype(context: &mut Context, type_label: String, supertype: String) -> TypeDBResult {
        let tx = context.transaction();
        assert_eq!(get_relation_type(tx, type_label).await?.get_supertype(tx).await?.label, supertype);
        Ok(())
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get supertypes contain:")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) get supertypes do not contain:")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) get subtypes contain:")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) get subtypes do not contain:")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) set owns attribute type: (\S+)$")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) set owns attribute type: (\S+), with annotations: ([^;]*)$")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) set owns attribute type: (\S+); throws exception$")]
    async fn relation_type_set_owns_attribute_type_throws(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
    ) {
        assert!(relation_type_set_owns_attribute_type(context, type_label, attribute_type_label).await.is_err());
    }

    #[step(
        regex = r"^relation\( ?(\S+) ?\) set owns attribute type: (\S+), with annotations: ([^;]*); throws exception$"
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

    #[step(regex = r"^relation\( ?(\S+) ?\) unset owns attribute type: (\S+)$")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns attribute types contain:$")]
    async fn relation_type_get_owns_attribute_types_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals =
            relation_type_get_owns_attribute_types(context, type_label, Transitivity::Transitive, &[]).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute));
        }
        Ok(())
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns types with annotations: (\S+); contain:$")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns attribute types do not contain:$")]
    async fn relation_type_get_owns_attribute_types_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals =
            relation_type_get_owns_attribute_types(context, type_label, Transitivity::Transitive, &[]).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
        Ok(())
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns types with annotations: (\S+); do not contain:$")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns explicit attribute types contain:$")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns explicit attribute types do not contain:$")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns explicit types with annotations: (\S+); contain:$")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns explicit types with annotations: (\S+); do not contain:$")]
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

    #[step(expr = r"relation\(( )?{word}( )\) get owns overridden attribute\(( ){word}( )\) is null: {word}")]
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

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns overridden attribute\( ?(\S+) ?\) get label: (\S+)$")]
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
}
