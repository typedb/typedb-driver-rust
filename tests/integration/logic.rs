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

use std::{sync::Arc, time::Instant};
use std::collections::HashMap;

use chrono::{NaiveDate, NaiveDateTime};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use regex::internal::Input;
use serial_test::serial;
use tokio::sync::mpsc;
use typedb_client::{concept::{Attribute, Concept, Value}, error::ConnectionError, Connection, DatabaseManager, Error, Options, Session, SessionType::{Data, Schema}, TransactionType::{Read, Write}, answer::ConceptMap, Transaction};
use typedb_client::logic::Explanation;

use super::common;

macro_rules! test_for_each_arg {
    {
        $perm_args:tt
        $( $( #[ $extra_anno:meta ] )* $async:ident fn $test:ident $args:tt -> $ret:ty $test_impl:block )+
    } => {
        test_for_each_arg!{ @impl $( $async fn $test $args $ret $test_impl )+ }
        test_for_each_arg!{ @impl_per $perm_args  { $( $( #[ $extra_anno ] )* $async fn $test )+ } }
    };

    { @impl $( $async:ident fn $test:ident $args:tt $ret:ty $test_impl:block )+ } => {
        mod _impl {
            use super::*;
            $( pub $async fn $test $args -> $ret $test_impl )+
        }
    };

    { @impl_per { $($mod:ident => $arg:expr),+ $(,)? } $fns:tt } => {
        $(test_for_each_arg!{ @impl_mod { $mod => $arg } $fns })+
    };

    { @impl_mod { $mod:ident => $arg:expr } { $( $( #[ $extra_anno:meta ] )* async fn $test:ident )+ } } => {
        mod $mod {
            use super::*;
        $(
            #[tokio::test]
            #[serial($mod)]
            $( #[ $extra_anno ] )*
            pub async fn $test() {
                _impl::$test($arg).await.unwrap();
            }
        )+
        }
    };
}

test_for_each_arg! {
    {
        core => common::new_core_connection().unwrap(),
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

        // let databases = DatabaseManager::new(connection);
        // if databases.contains(common::TEST_DATABASE).await? {
        //     databases.get(common::TEST_DATABASE).and_then(Database::delete).await?;
        // }
        // databases.create(common::TEST_DATABASE).await?;
        // let database = databases.get(common::TEST_DATABASE).await?;
        // let session = Session::new(database, Schema).await?;
        // let transaction = session.transaction(Write).await?;
        //
        // let person = transaction.concept().put_entity_type(str!{"person"}).await?;
        // let name = transaction.concept().put_attribute_type("name", AttributeType.ValueType.STRING);
        // person.set_owns(name).await?;
        // let friendship = transaction.concept().put_relation_type("friendship").await?;
        // friendship.set_relates("friend").await?;
        // let marriage = transaction.concept().put_relation_type("marriage").await?;
        // marriage.set_relates("husband").await?;
        // marriage.set_relates("wife").await?;
        // person.set_plays(friendship.get_relates("friend")).await?;
        // person.set_plays(marriage.get_relates("husband")).await?;
        // person.set_plays(marriage.get_relates("wife")).await?;

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

        assert!(with_explainable.explainables.is_some());
        // assert!(without_explainable.explainables.is_none());

        assert_single_explainable_explanations(with_explainable, 1, 1, &transaction).await;

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
    let explainables = ans.clone().explainables.unwrap();
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

    }
    // explanations.forEach(explanation -> {
    // Map<Retrievable, Set<Variable>> mapping = explanation.variableMapping();
    // Map<Retrievable, Set<Retrievable>> retrievableMapping = new HashMap<>();
    // mapping.forEach((k, v) -> retrievableMapping.put(
    // k, iterate(v).filter(Identifier::isRetrievable).map(Variable::asRetrievable).toSet()
    // ));
    // ConceptMap projected = applyMapping(retrievableMapping, ans);
    // projected.concepts().forEach((var, concept) -> {
    // assertTrue(explanation.conclusionAnswer().concepts().containsKey(var));
    // assertEquals(explanation.conclusionAnswer().concepts().get(var), concept);
    // });
    // });
    // return explanations;
}

fn check_explainable_vars(ans: &ConceptMap) {
    ans.clone().explainables.unwrap().relations.into_keys().for_each(|k| assert!(ans.map.contains_key(k.as_str())));
    ans.clone().explainables.unwrap().attributes.into_keys().for_each(|k| assert!(ans.map.contains_key(k.as_str())));
    ans.clone().explainables.unwrap().ownerships.into_keys()
        .for_each(|(k1, k2)| assert!(ans.map.contains_key(k1.as_str()) && ans.map.contains_key(k2.as_str())));
}

fn apply_mapping(mapping: HashMap<String, Vec<String>>, complete_map: ConceptMap) -> ConceptMap {
    let concepts: HashMap<String, Vec<String>> = HashMap::new();
    for key in mapping.keys() {
        assert!(complete_map.map.contains_key(key));
        let concept = complete_map.get(key).unwrap();
        for mapped in mapping.get(key).unwrap() {
            assert!(!concepts.contains_key(mapped) || concepts.get(mapped).unwrap().equals(concept));
            concepts.insert(mapped, concept);
        }
    }
    return ConceptMap::from(concepts);
}