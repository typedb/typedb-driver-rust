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
use futures::{TryFutureExt, TryStreamExt};
use typedb_client::{concept::EntityType, Annotation::Key, Result as TypeDBResult, Transaction};

use crate::{
    behaviour::{util::iter_table, Context},
    generic_step_impl,
};

async fn get_entity_type(tx: &Transaction<'_>, type_label: String) -> EntityType {
    let entity_type = tx.concept().get_entity_type(type_label).await;
    assert!(entity_type.is_ok(), "{entity_type:?}");
    let entity_type = entity_type.unwrap();
    assert!(entity_type.is_some());
    entity_type.unwrap()
}

async fn try_get_entity_type(tx: &Transaction<'_>, type_label: String) -> TypeDBResult<EntityType> {
    tx.concept().get_entity_type(type_label).await.map(|entity_type| {
        assert!(entity_type.is_some());
        entity_type.unwrap()
    })
}

generic_step_impl! {
    #[step(expr = "put entity type: {word}")]
    async fn put_entity_type(context: &mut Context, type_label: String) {
        context.transaction().concept().put_entity_type(type_label).await.unwrap();
    }

    #[step(expr = "delete entity type: {word}")]
    async fn delete_entity_type(context: &mut Context, type_label: String) {
        let tx = context.transaction();
        assert!(get_entity_type(tx, type_label).await.delete(tx).await.is_ok());
    }

    #[step(expr = "delete entity type: {word}; throws exception")]
    async fn delete_entity_type_throws_exception(context: &mut Context, type_label: String) {
        let tx = context.transaction();
        assert!(try_get_entity_type(tx, type_label)
            .and_then(|mut entity_type| async move { entity_type.delete(tx).await })
            .await
            .is_err());
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) is null: (\S+)$")]
    async fn entity_type_is_null(context: &mut Context, type_label: String, is_null: bool) {
        let res = context.transaction().concept().get_entity_type(type_label).await;
        assert!(res.is_ok());
        if is_null {
            assert!(res.unwrap().is_none());
        } else {
            assert!(res.unwrap().is_some());
        }
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) set label: (\S+)$")]
    async fn entity_type_set_label(context: &mut Context, type_label: String, new_label: String) {
        let tx = context.transaction();
        assert!(get_entity_type(tx, type_label).await.set_label(tx, new_label).await.is_ok());
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) get label: (\S+)$")]
    async fn entity_type_get_label(context: &mut Context, type_label: String, get_label: String) {
        assert_eq!(get_entity_type(context.transaction(), type_label).await.label, get_label);
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) set abstract: (\S+)$")]
    async fn entity_type_set_abstract(context: &mut Context, type_label: String, is_abstract: bool) {
        let tx = context.transaction();
        let mut entity_type = get_entity_type(tx, type_label).await;
        if is_abstract {
            assert!(entity_type.set_abstract(tx).await.is_ok());
        } else {
            assert!(entity_type.unset_abstract(tx).await.is_ok());
        }
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) set abstract: (\S+); throws exception$")]
    async fn entity_type_set_abstract_throws(context: &mut Context, type_label: String, is_abstract: bool) {
        let tx = context.transaction();
        let mut entity_type = get_entity_type(tx, type_label).await;
        if is_abstract {
            assert!(entity_type.set_abstract(tx).await.is_err());
        } else {
            assert!(entity_type.unset_abstract(tx).await.is_err());
        }
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) is abstract: (\S+)$")]
    async fn entity_type_is_abstract(context: &mut Context, type_label: String, is_abstract: bool) {
        assert_eq!(get_entity_type(context.transaction(), type_label).await.is_abstract, is_abstract);
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) set supertype: (\S+)$")]
    async fn entity_set_supertype(context: &mut Context, type_label: String, supertype_label: String) {
        let tx = context.transaction();
        let supertype = get_entity_type(tx, supertype_label).await;
        assert!(get_entity_type(tx, type_label).await.set_supertype(tx, supertype).await.is_ok());
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) set supertype: (\S+); throws exception$")]
    async fn entity_set_supertype_throws(context: &mut Context, type_label: String, supertype_label: String) {
        let tx = context.transaction();
        let supertype = try_get_entity_type(tx, supertype_label).await;
        assert!(supertype.is_ok());
        assert!(try_get_entity_type(tx, type_label)
            .and_then(|mut entity_type| async move { entity_type.set_supertype(tx, supertype.unwrap()).await })
            .await
            .is_err());
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) get supertype: (\S+)$")]
    async fn entity_get_supertype(context: &mut Context, type_label: String, supertype: String) {
        let tx = context.transaction();
        assert_eq!(get_entity_type(tx, type_label).await.get_supertype(tx).await.unwrap().label, supertype);
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) get supertypes contain:")]
    async fn entity_get_supertypes_contain(context: &mut Context, step: &Step, type_label: String) {
        let tx = context.transaction();
        let stream = get_entity_type(tx, type_label).await.get_supertypes(tx);
        assert!(stream.is_ok());
        let actuals = stream.unwrap().map_ok(|et| et.label).try_collect::<Vec<_>>().await;
        assert!(actuals.is_ok());
        let actuals = actuals.unwrap();
        for supertype in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == supertype));
        }
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) get supertypes do not contain:")]
    async fn entity_get_supertypes_do_not_contain(context: &mut Context, step: &Step, type_label: String) {
        let tx = context.transaction();
        let stream = get_entity_type(tx, type_label).await.get_supertypes(tx);
        assert!(stream.is_ok());
        let actuals = stream.unwrap().map_ok(|et| et.label).try_collect::<Vec<_>>().await;
        assert!(actuals.is_ok());
        let actuals = actuals.unwrap();
        for supertype in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != supertype));
        }
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) get subtypes contain:")]
    async fn entity_get_subtypes_contain(context: &mut Context, step: &Step, type_label: String) {
        let tx = context.transaction();
        let stream = get_entity_type(tx, type_label).await.get_subtypes(tx);
        assert!(stream.is_ok());
        let actuals = stream.unwrap().map_ok(|et| et.label).try_collect::<Vec<_>>().await;
        assert!(actuals.is_ok());
        let actuals = actuals.unwrap();
        for subtype in iter_table(step) {
            assert!(actuals.iter().any(|actual| actual == subtype));
        }
    }

    #[step(regex = r"^entity\( ?(\S+) ?\) get subtypes do not contain:")]
    async fn entity_get_subtypes_do_not_contain(context: &mut Context, step: &Step, type_label: String) {
        let tx = context.transaction();
        let stream = get_entity_type(tx, type_label).await.get_subtypes(tx);
        assert!(stream.is_ok());
        let actuals = stream.unwrap().map_ok(|et| et.label).try_collect::<Vec<_>>().await;
        assert!(actuals.is_ok());
        let actuals = actuals.unwrap();
        for subtype in iter_table(step) {
            assert!(actuals.iter().all(|actual| actual != subtype));
        }
    }
}
