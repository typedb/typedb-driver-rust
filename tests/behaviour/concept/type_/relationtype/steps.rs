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
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use typedb_client::{
    concept::{AttributeType, RelationType},
    Result as TypeDBResult, Transaction, Transitivity,
};

use crate::{
    behaviour::{
        util::{iter_table, AnnotationsParse},
        Context,
    },
    generic_step_impl,
};

async fn get_relation_type(tx: &Transaction<'_>, type_label: String) -> RelationType {
    let relation_type = tx.concept().get_relation_type(type_label).await;
    assert!(relation_type.is_ok(), "{relation_type:?}");
    let relation_type = relation_type.unwrap();
    assert!(relation_type.is_some());
    relation_type.unwrap()
}

async fn try_get_relation_type(tx: &Transaction<'_>, type_label: String) -> TypeDBResult<RelationType> {
    tx.concept().get_relation_type(type_label).await.map(|relation_type| {
        assert!(relation_type.is_some());
        relation_type.unwrap()
    })
}

async fn get_attribute_type(tx: &Transaction<'_>, type_label: String) -> AttributeType {
    let attribute_type = tx.concept().get_attribute_type(type_label).await;
    assert!(attribute_type.is_ok(), "{attribute_type:?}");
    let attribute_type = attribute_type.unwrap();
    assert!(attribute_type.is_some());
    attribute_type.unwrap()
}

async fn try_get_attribute_type(tx: &Transaction<'_>, type_label: String) -> TypeDBResult<AttributeType> {
    tx.concept().get_attribute_type(type_label).await.map(|attribute_type| {
        assert!(attribute_type.is_some());
        attribute_type.unwrap()
    })
}

generic_step_impl! {
    #[step(expr = "put relation type: {word}")]
    async fn put_relation_type(context: &mut Context, type_label: String) {
        context.transaction().concept().put_relation_type(type_label).await.unwrap();
    }

    #[step(expr = "delete relation type: {word}")]
    async fn delete_relation_type(context: &mut Context, type_label: String) {
        let tx = context.transaction();
        assert!(get_relation_type(tx, type_label).await.delete(tx).await.is_ok());
    }

    #[step(expr = "delete relation type: {word}; throws exception")]
    async fn delete_relation_type_throws_exception(context: &mut Context, type_label: String) {
        let tx = context.transaction();
        assert!(try_get_relation_type(tx, type_label)
            .and_then(|mut relation_type| async move { relation_type.delete(tx).await })
            .await
            .is_err());
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) is null: (\S+)$")]
    async fn relation_type_is_null(context: &mut Context, type_label: String, is_null: bool) {
        let res = context.transaction().concept().get_relation_type(type_label).await;
        assert!(res.is_ok());
        if is_null {
            assert!(res.unwrap().is_none());
        } else {
            assert!(res.unwrap().is_some());
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set label: (\S+)$")]
    async fn relation_type_set_label(context: &mut Context, type_label: String, new_label: String) {
        let tx = context.transaction();
        assert!(get_relation_type(tx, type_label).await.set_label(tx, new_label).await.is_ok());
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get label: (\S+)$")]
    async fn relation_type_get_label(context: &mut Context, type_label: String, get_label: String) {
        assert_eq!(get_relation_type(context.transaction(), type_label).await.label, get_label);
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set abstract: (\S+)$")]
    async fn relation_type_set_abstract(context: &mut Context, type_label: String, is_abstract: bool) {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await;
        if is_abstract {
            assert!(relation_type.set_abstract(tx).await.is_ok());
        } else {
            assert!(relation_type.unset_abstract(tx).await.is_ok());
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set abstract: (\S+); throws exception$")]
    async fn relation_type_set_abstract_throws(context: &mut Context, type_label: String, is_abstract: bool) {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await;
        if is_abstract {
            assert!(relation_type.set_abstract(tx).await.is_err());
        } else {
            assert!(relation_type.unset_abstract(tx).await.is_err());
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) is abstract: (\S+)$")]
    async fn relation_type_is_abstract(context: &mut Context, type_label: String, is_abstract: bool) {
        assert_eq!(get_relation_type(context.transaction(), type_label).await.is_abstract, is_abstract);
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set supertype: (\S+)$")]
    async fn relation_set_supertype(context: &mut Context, type_label: String, supertype_label: String) {
        let tx = context.transaction();
        let supertype = get_relation_type(tx, supertype_label).await;
        assert!(get_relation_type(tx, type_label).await.set_supertype(tx, supertype).await.is_ok());
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set supertype: (\S+); throws exception$")]
    async fn relation_set_supertype_throws(context: &mut Context, type_label: String, supertype_label: String) {
        let tx = context.transaction();
        let supertype = try_get_relation_type(tx, supertype_label).await;
        assert!(supertype.is_ok());
        assert!(try_get_relation_type(tx, type_label)
            .and_then(|mut relation_type| async move { relation_type.set_supertype(tx, supertype.unwrap()).await })
            .await
            .is_err());
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get supertype: (\S+)$")]
    async fn relation_get_supertype(context: &mut Context, type_label: String, supertype: String) {
        let tx = context.transaction();
        assert_eq!(get_relation_type(tx, type_label).await.get_supertype(tx).await.unwrap().label, supertype);
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get supertypes contain:")]
    async fn relation_get_supertypes_contain(context: &mut Context, step: &Step, type_label: String) {
        let tx = context.transaction();
        let stream = get_relation_type(tx, type_label).await.get_supertypes(tx);
        assert!(stream.is_ok());
        let actuals = stream.unwrap().map_ok(|et| et.label).try_collect::<Vec<_>>().await;
        assert!(actuals.is_ok());
        let actuals = actuals.unwrap();
        for supertype in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == supertype));
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get supertypes do not contain:")]
    async fn relation_get_supertypes_do_not_contain(context: &mut Context, step: &Step, type_label: String) {
        let tx = context.transaction();
        let stream = get_relation_type(tx, type_label).await.get_supertypes(tx);
        assert!(stream.is_ok());
        let actuals = stream.unwrap().map_ok(|et| et.label).try_collect::<Vec<_>>().await;
        assert!(actuals.is_ok());
        let actuals = actuals.unwrap();
        for supertype in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != supertype));
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get subtypes contain:")]
    async fn relation_get_subtypes_contain(context: &mut Context, step: &Step, type_label: String) {
        let tx = context.transaction();
        let stream = get_relation_type(tx, type_label).await.get_subtypes(tx);
        assert!(stream.is_ok());
        let actuals = stream.unwrap().map_ok(|et| et.label).try_collect::<Vec<_>>().await;
        assert!(actuals.is_ok());
        let actuals = actuals.unwrap();
        for subtype in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == subtype));
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get subtypes do not contain:")]
    async fn relation_get_subtypes_do_not_contain(context: &mut Context, step: &Step, type_label: String) {
        let tx = context.transaction();
        let stream = get_relation_type(tx, type_label).await.get_subtypes(tx);
        assert!(stream.is_ok());
        let actuals = stream.unwrap().map_ok(|et| et.label).try_collect::<Vec<_>>().await;
        assert!(actuals.is_ok());
        let actuals = actuals.unwrap();
        for subtype in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != subtype));
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set owns attribute type: (\S+)$")]
    async fn relation_type_set_owns_attribute_type(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
    ) {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await;
        // FIXME barf ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~v~~~~~v
        assert!(relation_type.set_owns(tx, attribute_type, None, &[]).await.is_ok());
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set owns attribute type: (\S+), with annotations: ([^;]*)$")]
    async fn relation_type_set_owns_attribute_type_with_annotations(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        annotations: AnnotationsParse,
    ) {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await;
        assert!(relation_type.set_owns(tx, attribute_type, None, &annotations).await.is_ok());
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) set owns attribute type: (\S+); throws exception$")]
    async fn relation_type_set_owns_attribute_type_throws(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
    ) {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await;
        assert!(relation_type.set_owns(tx, attribute_type, None, &[]).await.is_err());
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
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await;
        assert!(relation_type.set_owns(tx, attribute_type, None, &annotations).await.is_err());
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) unset owns attribute type: (\S+)$")]
    async fn relation_type_unset_owns_attribute_type(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
    ) {
        let tx = context.transaction();
        let mut relation_type = get_relation_type(tx, type_label).await;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await;
        assert!(relation_type.unset_owns(tx, attribute_type).await.is_ok());
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns attribute types contain:$")]
    async fn relation_type_get_owns_attribute_types(context: &mut Context, step: &Step, type_label: String) {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await;
        let actuals: Vec<String> = relation_type
            .get_owns(tx, None, Transitivity::Transitive, &[])
            .unwrap()
            .map(|at| at.map(|t| t.label))
            .try_collect()
            .await
            .unwrap();
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute));
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns types with annotations: (\S+); contain:$")]
    async fn relation_type_get_owns_attribute_types_with_annotations(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await;
        let actuals: Vec<String> = relation_type
            .get_owns(tx, None, Transitivity::Transitive, &annotations)
            .unwrap()
            .map(|at| at.map(|t| t.label))
            .try_collect()
            .await
            .unwrap();
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute), "{attribute} not in {actuals:?}");
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns attribute types do not contain:$")]
    async fn relation_type_get_owns_attribute_types_do_not_contain(context: &mut Context, step: &Step, type_label: String) {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await;
        let actuals: Vec<String> = relation_type
            .get_owns(tx, None, Transitivity::Transitive, &[])
            .unwrap()
            .map(|at| at.map(|t| t.label))
            .try_collect()
            .await
            .unwrap();
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns types with annotations: (\S+); do not contain:$")]
    async fn relation_type_get_owns_attribute_types_with_annotations_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await;
        let actuals: Vec<String> = relation_type
            .get_owns(tx, None, Transitivity::Transitive, &annotations)
            .unwrap()
            .map(|at| at.map(|t| t.label))
            .try_collect()
            .await
            .unwrap();
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns explicit attribute types contain:$")]
    async fn relation_type_get_owns_explicit_attribute_types(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await;
        let actuals: Vec<String> = relation_type
            .get_owns(tx, None, Transitivity::Explicit, &[])
            .unwrap()
            .map(|at| at.map(|t| t.label))
            .try_collect()
            .await
            .unwrap();
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute), "{attribute} not in {actuals:?}");
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns explicit attribute types do not contain:$")]
    async fn relation_type_get_owns_explicit_attribute_types_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
    ) {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await;
        let actuals: Vec<String> = relation_type
            .get_owns(tx, None, Transitivity::Explicit, &[])
            .unwrap()
            .map(|at| at.map(|t| t.label))
            .try_collect()
            .await
            .unwrap();
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns explicit types with annotations: (\S+); contain:$")]
    async fn relation_type_get_owns_explicit_attribute_types_with_annotations(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await;
        let actuals: Vec<String> = relation_type
            .get_owns(tx, None, Transitivity::Explicit, &annotations)
            .unwrap()
            .map(|at| at.map(|t| t.label))
            .try_collect()
            .await
            .unwrap();
        for attribute in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == attribute), "{attribute} not in {actuals:?}");
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns explicit types with annotations: (\S+); do not contain:$")]
    async fn relation_type_get_owns_explicit_attribute_types_with_annotations_do_not_contain(
        context: &mut Context,
        step: &Step,
        type_label: String,
        annotations: AnnotationsParse,
    ) {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await;
        let actuals: Vec<String> = relation_type
            .get_owns(tx, None, Transitivity::Explicit, &annotations)
            .unwrap()
            .map(|at| at.map(|t| t.label))
            .try_collect()
            .await
            .unwrap();
        for attribute in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != attribute));
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns overridden attribute\( ?(\S+) ?\) is null: (\S*)$")]
    async fn relation_type_get_owns_overridden_attribute_type(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        is_null: bool,
    ) {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await;
        let res = relation_type.get_owns_overridden(tx, attribute_type).await;
        assert!(res.is_ok());
        if is_null {
            assert!(res.unwrap().is_none());
        } else {
            assert!(res.unwrap().is_some());
        }
    }

    #[step(regex = r"^relation\( ?(\S+) ?\) get owns overridden attribute\( ?(\S+) ?\) get label: (\S+)$")]
    async fn relation_type_get_owns_overridden_attribute_type_label(
        context: &mut Context,
        type_label: String,
        attribute_type_label: String,
        overridden_label: String,
    ) {
        let tx = context.transaction();
        let relation_type = get_relation_type(tx, type_label).await;
        let attribute_type = get_attribute_type(tx, attribute_type_label).await;
        let res = relation_type.get_owns_overridden(tx, attribute_type).await;
        assert!(res.is_ok());
        let Ok(res) = res else { unreachable!() };
        assert!(res.is_some());
        let Some(res) = res else { unreachable!() };
        assert_eq!(res.label, overridden_label);
    }
}
