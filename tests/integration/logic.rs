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

use std::{collections::HashMap, default::Default};

use futures::{TryFutureExt, TryStreamExt};
use serial_test::serial;
use typedb_client::{
    answer::{ConceptMap, Explainable, Explainables},
    concept::{Attribute, Concept, Value},
    logic::Explanation,
    transaction::concept::api::ThingAPI,
    Connection, DatabaseManager, Options, Session,
    SessionType::{Data, Schema},
    Transaction,
    TransactionType::{Read, Write},
};

use super::common;
use crate::test_for_each_arg;

test_for_each_arg! {
    {
        core => common::new_core_connection().unwrap(),
        cluster => common::new_cluster_connection().unwrap(),
    }

    async fn test_disjunction_explainable(connection: Connection) -> typedb_client::Result {
        let schema = r#"define
            person sub entity,
                owns name,
                plays friendship:friend,
                plays marriage:husband,
                plays marriage:wife;
            name sub attribute, value string;
            friendship sub relation,
                relates friend;
            marriage sub relation,
                relates husband, relates wife;"#;
        common::create_test_database_with_schema(connection.clone(), schema).await?;

        let databases = DatabaseManager::new(connection);
        {
            let session = Session::new(databases.get(common::TEST_DATABASE).await?, Schema).await?;
            let transaction = session.transaction(Write).await?;
            transaction.logic().put_rule(
                "marriage-is-friendship".to_string(),
                typeql_lang::parse_pattern("{ $x isa person; $y isa person; (husband: $x, wife: $y) isa marriage; }")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?
                    .into_conjunction(),
                typeql_lang::parse_variable("(friend: $x, friend: $y) isa friendship")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?,
            ).await?;
            transaction.commit().await?;
        }

        let session = Session::new(databases.get(common::TEST_DATABASE).await?, Data).await?;
        let transaction = session.transaction(Write).await?;
        let data = r#"insert $x isa person, has name 'Zack';
            $y isa person, has name 'Yasmin';
            (husband: $x, wife: $y) isa marriage;"#;
        let _ = transaction.query().insert(data)?;
        transaction.commit().await?;

        let with_inference_and_explanation = Options::new().infer(true).explain(true);
        let transaction = session.transaction_with_options(Read, with_inference_and_explanation).await?;
        let answer_stream = transaction.query().match_(
            r#"match $p1 isa person;
            { (friend: $p1, friend: $p2) isa friendship;}
            or { $p1 has name 'Zack'; };"#,
        )?;
        let answers = answer_stream.try_collect::<Vec<ConceptMap>>().await?;

        assert_eq!(3, answers.len());

        let mut without_explainable = answers.get(0).unwrap();
        let mut with_explainable = answers.get(1).unwrap();
        if without_explainable.map.contains_key("p2") {
            (without_explainable, with_explainable) = (with_explainable, without_explainable);
        };

        assert_eq!(3, with_explainable.map.len());
        assert_eq!(2, without_explainable.map.len());

        assert!(!with_explainable.explainables.is_empty());
        assert!(without_explainable.explainables.is_empty());

        assert_single_explainable_explanations(with_explainable, 1, 1, &transaction).await;

        Ok(())
    }

    async fn test_relation_explainable(connection: Connection) -> typedb_client::Result {
        let schema = r#"define
            person sub entity,
                owns name,
                plays friendship:friend,
                plays marriage:husband,
                plays marriage:wife;
            name sub attribute, value string;
            friendship sub relation,
                relates friend;
            marriage sub relation,
                relates husband, relates wife;"#;
        common::create_test_database_with_schema(connection.clone(), schema).await?;

        let databases = DatabaseManager::new(connection);
        {
            let session = Session::new(databases.get(common::TEST_DATABASE).await?, Schema).await?;
            let transaction = session.transaction(Write).await?;
            transaction.logic().put_rule(
                "marriage-is-friendship".to_string(),
                typeql_lang::parse_pattern("{ $x isa person; $y isa person; (husband: $x, wife: $y) isa marriage; }")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?
                    .into_conjunction(),
                typeql_lang::parse_variable("(friend: $x, friend: $y) isa friendship")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?,
            ).await?;
            transaction.commit().await?;
        }

        let session = Session::new(databases.get(common::TEST_DATABASE).await?, Data).await?;
        let transaction = session.transaction(Write).await?;
        let data = r#"insert $x isa person, has name 'Zack';
            $y isa person, has name 'Yasmin';
            (husband: $x, wife: $y) isa marriage;"#;
        let _ = transaction.query().insert(data)?;
        transaction.commit().await?;

        let with_inference_and_explanation = Options::new().infer(true).explain(true);
        let transaction = session.transaction_with_options(Read, with_inference_and_explanation).await?;
        let answer_stream = transaction.query().match_(
            r#"match (friend: $p1, friend: $p2) isa friendship; $p1 has name $na;"#,
        )?;
        let answers = answer_stream.try_collect::<Vec<ConceptMap>>().await?;

        assert_eq!(2, answers.len());

        assert!(!answers.get(0).unwrap().explainables.is_empty());
        assert!(!answers.get(1).unwrap().explainables.is_empty());

        assert_single_explainable_explanations(answers.get(0).unwrap(), 1, 1, &transaction).await;
        assert_single_explainable_explanations(answers.get(1).unwrap(), 1, 1, &transaction).await;

        Ok(())
    }

    async fn test_relation_explainable_multiple_ways(connection: Connection) -> typedb_client::Result {
        let schema = r#"define
            person sub entity,
                owns name,
                plays friendship:friend,
                plays marriage:husband,
                plays marriage:wife;
            name sub attribute, value string;
            friendship sub relation,
                relates friend;
            marriage sub relation,
                relates husband, relates wife;"#;
        common::create_test_database_with_schema(connection.clone(), schema).await?;

        let databases = DatabaseManager::new(connection);
        {
            let session = Session::new(databases.get(common::TEST_DATABASE).await?, Schema).await?;
            let transaction = session.transaction(Write).await?;
            transaction.logic().put_rule(
                "marriage-is-friendship".to_string(),
                typeql_lang::parse_pattern("{ $x isa person; $y isa person; (husband: $x, wife: $y) isa marriage; }")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?
                    .into_conjunction(),
                typeql_lang::parse_variable("(friend: $x, friend: $y) isa friendship")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?,
            ).await?;
            transaction.logic().put_rule(
                "everyone-is-friends".to_string(),
                typeql_lang::parse_pattern("{ $x isa person; $y isa person; not { $x is $y; }; }")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?
                    .into_conjunction(),
                typeql_lang::parse_variable("(friend: $x, friend: $y) isa friendship")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?,
            ).await?;
            transaction.commit().await?;
        }

        let session = Session::new(databases.get(common::TEST_DATABASE).await?, Data).await?;
        let transaction = session.transaction(Write).await?;
        let data = r#"insert $x isa person, has name 'Zack';
            $y isa person, has name 'Yasmin';
            (husband: $x, wife: $y) isa marriage;"#;
        let _ = transaction.query().insert(data)?;
        transaction.commit().await?;

        let with_inference_and_explanation = Options::new().infer(true).explain(true);
        let transaction = session.transaction_with_options(Read, with_inference_and_explanation).await?;
        let answer_stream = transaction.query().match_(
            r#"match (friend: $p1, friend: $p2) isa friendship; $p1 has name $na;"#,
        )?;
        let answers = answer_stream.try_collect::<Vec<ConceptMap>>().await?;

        assert_eq!(2, answers.len());

        assert!(!answers.get(0).unwrap().explainables.is_empty());
        assert!(!answers.get(1).unwrap().explainables.is_empty());

        assert_single_explainable_explanations(answers.get(0).unwrap(), 1, 3, &transaction).await;
        assert_single_explainable_explanations(answers.get(1).unwrap(), 1, 3, &transaction).await;

        Ok(())
    }

    async fn test_has_explicit_explainable_two_ways(connection: Connection) -> typedb_client::Result {
        let schema = r#"define
            milk sub entity,
                owns age-in-days,
                owns is-still-good;
            age-in-days sub attribute, value long;
            is-still-good sub attribute, value boolean;"#;
        common::create_test_database_with_schema(connection.clone(), schema).await?;

        let databases = DatabaseManager::new(connection);
        {
            let session = Session::new(databases.get(common::TEST_DATABASE).await?, Schema).await?;
            let transaction = session.transaction(Write).await?;
            transaction.logic().put_rule(
                "old-milk-is-not-good".to_string(),
                typeql_lang::parse_pattern("{ $x isa milk, has age-in-days <= 10; }")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?
                    .into_conjunction(),
                typeql_lang::parse_variable("$x has is-still-good true")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?,
            ).await?;
            transaction.logic().put_rule(
                "all-milk-is-good".to_string(),
                typeql_lang::parse_pattern("{ $x isa milk; }")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?
                    .into_conjunction(),
                typeql_lang::parse_variable("$x has is-still-good true")
                    .map_err(|err| typedb_client::Error::Other(format!("{err:?}")))?,
            ).await?;
            transaction.commit().await?;
        }

        let session = Session::new(databases.get(common::TEST_DATABASE).await?, Data).await?;
        let transaction = session.transaction(Write).await?;
        let data = r#"insert $x isa milk, has age-in-days 5;"#;
        let _ = transaction.query().insert(data)?;
        let data = r#"insert $x isa milk, has age-in-days 10;"#;
        let _ = transaction.query().insert(data)?;
        let data = r#"insert $x isa milk, has age-in-days 15;"#;
        let _ = transaction.query().insert(data)?;
        transaction.commit().await?;

        let with_inference_and_explanation = Options::new().infer(true).explain(true);
        let transaction = session.transaction_with_options(Read, with_inference_and_explanation).await?;
        let answer_stream = transaction.query().match_(
            r#"match $x has is-still-good $a;"#,
        )?;
        let answers = answer_stream.try_collect::<Vec<ConceptMap>>().await?;

        assert_eq!(3, answers.len());

        assert!(!answers.get(0).unwrap().explainables.is_empty());
        assert!(!answers.get(1).unwrap().explainables.is_empty());
        assert!(!answers.get(2).unwrap().explainables.is_empty());

        let age_in_days = transaction.concept().get_attribute_type(String::from("age-in-days")).await?.unwrap();
        for ans in answers {
            match ans.map.get("x").unwrap() {
                Concept::Entity(entity) => {
                    let attributes: Vec<Attribute> = entity.get_has(&transaction, vec![age_in_days.clone()], vec![])?.try_collect().await?;
                    if attributes.first().unwrap().value == Value::Long(15) {
                        assert_single_explainable_explanations(&ans, 1, 1, &transaction).await;
                    } else {
                        assert_single_explainable_explanations(&ans, 1, 2, &transaction).await;
                    }
                },
                _ => panic!("Incorrect Concept type: {:?}", ans.map.get("x").unwrap()),
            }
        }

        Ok(())
    }
}

async fn assert_single_explainable_explanations(
    ans: &ConceptMap,
    explainables_count: usize,
    explanations_count: usize,
    transaction: &Transaction<'_>,
) {
    check_explainable_vars(ans);
    let explainables = ans.clone().explainables;
    let mut all_explainables = explainables.attributes.values().collect::<Vec<_>>();
    all_explainables.extend(explainables.relations.values().collect::<Vec<_>>());
    all_explainables.extend(explainables.ownerships.values().collect::<Vec<_>>());
    assert_eq!(explainables_count, all_explainables.len());
    let explainable = all_explainables.get(0).unwrap();
    assert!(explainable.id >= 0);
    let stream = transaction.query().explain(explainable.id);
    assert!(stream.is_ok());
    let result = stream.unwrap().try_collect::<Vec<_>>().await;
    assert!(result.is_ok());
    let explanations = result.unwrap();
    assert_eq!(explanations_count, explanations.len());
    for explanation in explanations {
        let mapping = explanation.variable_mapping;
        let projected = apply_mapping(&mapping, ans);
        for var in projected.map.keys() {
            assert!(explanation.conclusion.map.contains_key(var));
            assert_eq!(explanation.conclusion.map.get(var), projected.map.get(var));
        }
    }
}

fn check_explainable_vars(ans: &ConceptMap) {
    ans.clone().explainables.relations.into_keys().for_each(|k| assert!(ans.map.contains_key(k.as_str())));
    ans.clone().explainables.attributes.into_keys().for_each(|k| assert!(ans.map.contains_key(k.as_str())));
    ans.clone()
        .explainables
        .ownerships
        .into_keys()
        .for_each(|(k1, k2)| assert!(ans.map.contains_key(k1.as_str()) && ans.map.contains_key(k2.as_str())));
}

fn apply_mapping(mapping: &HashMap<String, Vec<String>>, complete_map: &ConceptMap) -> ConceptMap {
    let mut concepts: HashMap<String, Concept> = HashMap::new();
    for key in mapping.keys() {
        assert!(complete_map.map.contains_key(key));
        let concept = complete_map.get(key).unwrap();
        for mapped in mapping.get(key).unwrap() {
            assert!(!concepts.contains_key(mapped) || concepts.get(mapped).unwrap() == concept);
            concepts.insert(mapped.to_string(), concept.clone());
        }
    }
    ConceptMap { map: concepts, explainables: Default::default() }
}
