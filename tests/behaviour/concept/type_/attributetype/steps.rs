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
use typedb_client::{Annotation, Result as TypeDBResult, Transitivity};

use crate::{
    behaviour::{
        concept::common::get_attribute_type,
        util::{iter_table, AnnotationsParse, ValueTypeParse},
        Context,
    },
    generic_step_impl,
};

async fn attribute_type_get_owns_attribute_types(
    context: &mut Context,
    type_label: String,
    transitivity: Transitivity,
    annotations: &[Annotation],
) -> TypeDBResult<Vec<String>> {
    let tx = context.transaction();
    let attribute_type = get_attribute_type(tx, type_label).await?;
    attribute_type.get_owns(tx, None, transitivity, annotations)?.map_ok(|at| at.label).try_collect().await
}

generic_step_impl! {
    #[step(expr = "put attribute type: {word}, with value type: {word}")]
    async fn put_attribute_type(context: &mut Context, type_label: String, value_type: ValueTypeParse) {
        context.transaction().concept().put_attribute_type(type_label, value_type.into()).await.unwrap();
    }

    #[step(expr = "delete attribute type: {word}")]
    async fn delete_attribute_type(context: &mut Context, type_label: String) -> TypeDBResult {
        let tx = context.transaction();
        get_attribute_type(tx, type_label).await?.delete(tx).await
    }

    #[step(expr = "delete attribute type: {word}; throws exception")]
    async fn delete_attribute_type_throws(context: &mut Context, type_label: String) {
        assert!(delete_attribute_type(context, type_label).await.is_err());
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) is null: (\S+)$")]
    async fn attribute_type_is_null(context: &mut Context, type_label: String, is_null: bool) -> TypeDBResult {
        let res = context.transaction().concept().get_attribute_type(type_label).await?;
        assert_eq!(res.is_none(), is_null, "{res:?}");
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) set label: (\S+)$")]
    async fn attribute_type_set_label(context: &mut Context, type_label: String, new_label: String) -> TypeDBResult {
        let tx = context.transaction();
        get_attribute_type(tx, type_label).await?.set_label(tx, new_label).await
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get label: (\S+)$")]
    async fn attribute_type_get_label(context: &mut Context, type_label: String, get_label: String) -> TypeDBResult {
        assert_eq!(get_attribute_type(context.transaction(), type_label).await?.label, get_label);
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) set abstract: (\S+)$")]
    async fn attribute_type_set_abstract(context: &mut Context, type_label: String, is_abstract: bool) -> TypeDBResult {
        let tx = context.transaction();
        let mut attribute_type = get_attribute_type(tx, type_label).await?;
        if is_abstract {
            attribute_type.set_abstract(tx).await
        } else {
            attribute_type.unset_abstract(tx).await
        }
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) set abstract: (\S+); throws exception$")]
    async fn attribute_type_set_abstract_throws(context: &mut Context, type_label: String, is_abstract: bool) {
        assert!(attribute_type_set_abstract(context, type_label, is_abstract).await.is_err());
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) is abstract: (\S+)$")]
    async fn attribute_type_is_abstract(context: &mut Context, type_label: String, is_abstract: bool) -> TypeDBResult {
        assert_eq!(get_attribute_type(context.transaction(), type_label).await?.is_abstract, is_abstract);
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) set supertype: (\S+)$")]
    async fn attribute_set_supertype(
        context: &mut Context,
        type_label: String,
        supertype_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let supertype = get_attribute_type(tx, supertype_label).await?;
        get_attribute_type(tx, type_label).await?.set_supertype(tx, supertype).await
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) set supertype: (\S+); throws exception$")]
    async fn attribute_set_supertype_throws(context: &mut Context, type_label: String, supertype_label: String) {
        assert!(attribute_set_supertype(context, type_label, supertype_label).await.is_err())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get supertype: (\S+)$")]
    async fn attribute_get_supertype(context: &mut Context, type_label: String, supertype: String) -> TypeDBResult {
        let tx = context.transaction();
        assert_eq!(get_attribute_type(tx, type_label).await?.get_supertype(tx).await?.label, supertype);
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get supertypes contain:")]
    async fn attribute_get_supertypes_contain(context: &mut Context, step: &Step, type_label: String) -> TypeDBResult {
        let tx = context.transaction();
        let actuals = get_attribute_type(tx, type_label)
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

    #[step(regex = r"^attribute\( ?(\S+) ?\) get supertypes do not contain:")]
    async fn attribute_get_supertypes_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let actuals = get_attribute_type(tx, type_label)
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

    #[step(regex = r"^attribute\( ?(\S+) ?\) get subtypes contain:")]
    async fn attribute_get_subtypes_contain(context: &mut Context, step: &Step, type_label: String) -> TypeDBResult {
        let tx = context.transaction();
        let actuals = get_attribute_type(tx, type_label)
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

    #[step(regex = r"^attribute\( ?(\S+) ?\) get subtypes do not contain:")]
    async fn attribute_get_subtypes_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let actuals = get_attribute_type(tx, type_label)
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

    #[step(regex = r"^attribute\( ?(\S+) ?\) set owns attribute type: (\S+)$")]
    async fn attribute_type_set_owns_attribute_type(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut attribute_type = get_attribute_type(tx, type_label).await?;
        let owned_attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        // FIXME barf ~~~~~~~~~~~~~~~~~~~~~~~~~~~v~~~~~v
        attribute_type.set_owns(tx, owned_attribute_type, None, &[]).await
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) set owns attribute type: (\S+), with annotations: ([^;]*)$")]
    async fn attribute_type_set_owns_attribute_type_with_annotations(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut attribute_type = get_attribute_type(tx, type_label).await?;
        let owned_attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        attribute_type.set_owns(tx, owned_attribute_type, None, &annotations).await
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) set owns attribute type: (\S+); throws exception$")]
    async fn attribute_type_set_owns_attribute_type_throws(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
    ) {
        assert!(attribute_type_set_owns_attribute_type(context, type_label, attribute_type_label).await.is_err());
    }

    #[step(
        regex = r"^attribute\( ?(\S+) ?\) set owns attribute type: (\S+), with annotations: ([^;]*); throws exception$"
    )]
    async fn attribute_type_set_owns_attribute_type_with_annotations_throws(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        annotations: AnnotationsParse,
    ) {
        assert!(attribute_type_set_owns_attribute_type_with_annotations(
            context,
            type_label,
            attribute_type_label,
            annotations
        )
        .await
        .is_err());
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) unset owns attribute type: (\S+)$")]
    async fn attribute_type_unset_owns_attribute_type(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let mut attribute_type = get_attribute_type(tx, type_label).await?;
        let owned_attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        assert!(attribute_type.unset_owns(tx, owned_attribute_type).await.is_ok());
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get owns attribute types contain:$")]
    async fn attribute_type_get_owns_attribute_types_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals =
            attribute_type_get_owns_attribute_types(context, type_label, Transitivity::Transitive, &[]).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute));
        }
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get owns types with annotations: (\S+); contain:$")]
    async fn attribute_type_get_owns_attribute_types_with_annotations_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let actuals =
            attribute_type_get_owns_attribute_types(context, type_label, Transitivity::Transitive, &annotations)
                .await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute), "{attribute} not in {actuals:?}");
        }
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get owns attribute types do not contain:$")]
    async fn attribute_type_get_owns_attribute_types_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals =
            attribute_type_get_owns_attribute_types(context, type_label, Transitivity::Transitive, &[]).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get owns types with annotations: (\S+); do not contain:$")]
    async fn attribute_type_get_owns_attribute_types_with_annotations_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let actuals =
            attribute_type_get_owns_attribute_types(context, type_label, Transitivity::Transitive, &annotations)
                .await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get owns explicit attribute types contain:$")]
    async fn attribute_type_get_owns_explicit_attribute_types_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals = attribute_type_get_owns_attribute_types(context, type_label, Transitivity::Explicit, &[]).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute), "{attribute} not in {actuals:?}");
        }
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get owns explicit attribute types do not contain:$")]
    async fn attribute_type_get_owns_explicit_attribute_types_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) -> TypeDBResult {
        let actuals = attribute_type_get_owns_attribute_types(context, type_label, Transitivity::Explicit, &[]).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get owns explicit types with annotations: (\S+); contain:$")]
    async fn attribute_type_get_owns_explicit_attribute_types_with_annotations_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let actuals =
            attribute_type_get_owns_attribute_types(context, type_label, Transitivity::Explicit, &annotations).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute), "{attribute} not in {actuals:?}");
        }
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get owns explicit types with annotations: (\S+); do not contain:$")]
    async fn attribute_type_get_owns_explicit_attribute_types_with_annotations_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) -> TypeDBResult {
        let actuals =
            attribute_type_get_owns_attribute_types(context, type_label, Transitivity::Explicit, &annotations).await?;
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
        Ok(())
    }

    #[step(expr = r"attribute\(( )?{word}( )\) get owns overridden attribute\(( ){word}( )\) is null: {word}")]
    async fn attribute_type_get_owns_overridden_attribute_type(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        is_null: bool,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let attribute_type = get_attribute_type(tx, type_label).await?;
        let owned_attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        let res = attribute_type.get_owns_overridden(tx, owned_attribute_type).await?;
        assert_eq!(res.is_none(), is_null);
        Ok(())
    }

    #[step(regex = r"^attribute\( ?(\S+) ?\) get owns overridden attribute\( ?(\S+) ?\) get label: (\S+)$")]
    async fn attribute_type_get_owns_overridden_attribute_type_label(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        overridden_label: String,
    ) -> TypeDBResult {
        let tx = context.transaction();
        let attribute_type = get_attribute_type(tx, type_label).await?;
        let owned_attribute_type = get_attribute_type(tx, attribute_type_label).await?;
        let res = attribute_type.get_owns_overridden(tx, owned_attribute_type).await?;
        assert_eq!(res.map(|at| at.label), Some(overridden_label));
        Ok(())
    }
}
