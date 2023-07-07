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

use std::collections::HashMap;

mod steps;

use chrono::{NaiveDateTime, NaiveTime};
use cucumber::gherkin::Step;
use futures::{
    stream::{self, StreamExt},
    TryFutureExt, TryStreamExt,
};
use regex::{Captures, Regex};
use tokio::time::sleep;
use typedb_client::{
    answer::ConceptMap,
    concept::{Annotation, Attribute, Concept, Entity, Relation, Value},
    logic::Rule,
    transaction::concept::api::ThingAPI,
    DatabaseManager, Result as TypeDBResult,
};
use typeql_lang::{parse_patterns, parse_query, pattern::Variable};

use crate::{assert_with_timeout, behaviour::Context};

pub fn iter_table(step: &Step) -> impl Iterator<Item = &str> {
    step.table().unwrap().rows.iter().flatten().map(String::as_str)
}

pub fn iter_table_map(step: &Step) -> impl Iterator<Item = HashMap<&str, &str>> {
    let (keys, rows) = step.table().unwrap().rows.split_first().unwrap();
    rows.iter().map(|row| keys.iter().zip(row).map(|(k, v)| (k.as_str(), v.as_str())).collect())
}

pub async fn match_answer_concept_map(
    context: &Context,
    answer_identifiers: &HashMap<&str, &str>,
    answer: &ConceptMap,
) -> bool {
    stream::iter(answer_identifiers.keys())
        .all(|key| async {
            answer.map.contains_key(key.clone())
                && match_answer_concept(context, answer_identifiers.get(key).unwrap(), answer.get(key).unwrap()).await
        })
        .await
}

pub async fn match_answer_concept(context: &Context, answer_identifier: &str, answer: &Concept) -> bool {
    let identifiers: Vec<&str> = answer_identifier.splitn(2, ":").collect();
    match identifiers[0] {
        "key" => key_values_equal(context, identifiers[1], answer).await,
        "label" => labels_equal(identifiers[1], answer),
        "value" => values_equal(identifiers[1], answer)
        "attr" => values_equal(identifiers[1], answer),
        _ => unreachable!(),
    }
}

async fn key_values_equal(context: &Context, expected_label_and_value: &str, answer: &Concept) -> bool {
    let identifiers: Vec<&str> = expected_label_and_value.splitn(2, ":").collect();
    assert_eq!(identifiers.len(), 2, "Unexpected table cell format: {expected_label_and_value}.");

    let res = match answer {
        Concept::Entity(entity) => {
            async { entity.get_has(context.transaction(), vec![], vec![Annotation::Key]) }
                .and_then(|stream| async { stream.try_collect::<Vec<_>>().await })
                .await
        }
        Concept::Relation(rel) => {
            async { rel.get_has(context.transaction(), vec![], vec![Annotation::Key]) }
                .and_then(|stream| async { stream.try_collect::<Vec<_>>().await })
                .await
        }
        Concept::Attribute(attr) => {
            async { attr.get_has(context.transaction(), vec![], vec![Annotation::Key]) }
                .and_then(|stream| async { stream.try_collect::<Vec<_>>().await })
                .await
        }
        _ => unreachable!("Unexpected Concept type: {answer:?}"),
    };
    match res {
        Ok(_) => {
            let equals = res
                .unwrap()
                .into_iter()
                .filter(|attr| attr.type_.label == identifiers[0])
                .collect::<Vec<_>>()
                .first()
                .and_then(|attr| Some(value_equals_str(&attr.value, identifiers[1])));
            match equals {
                Some(val) => val,
                None => false,
            }
        }
        Err(_) => false,
    }
}

fn labels_equal(expected_label: &str, answer: &Concept) -> bool {
    answer.type_label_cloned() == expected_label
}

fn values_equal(expected_label_and_value: &str, answer: &Concept) -> bool {
    let identifiers: Vec<&str> = expected_label_and_value.splitn(2, ":").collect();
    assert_eq!(identifiers.len(), 2, "Unexpected table cell format: {expected_label_and_value}.");
    let Concept::Attribute(Attribute { value, .. }) = answer else { unreachable!() };
    value_equals_str(value, identifiers[1])
}

fn value_equals_str(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(val) => val == expected,
        Value::Long(val) => expected.parse::<i64>().and_then(|expected| Ok(expected.eq(val))).unwrap_or_else(|_| false),
        Value::Double(val) => expected
            .parse::<f64>()
            .and_then(|expected| Ok(equals_approximate(expected, *val)))
            .unwrap_or_else(|_| false),
        Value::Boolean(val) => {
            expected.parse::<bool>().and_then(|expected| Ok(expected.eq(val))).unwrap_or_else(|_| false)
        }
        Value::DateTime(val) => {
            if expected.contains(":") {
                val == &NaiveDateTime::parse_from_str(expected, "%Y-%m-%dT%H:%M:%S").unwrap()
            } else {
                val == &NaiveDateTime::parse_from_str(expected, "%Y-%m-%d").unwrap()
            }
        }
    }
}

pub fn equals_approximate(first: f64, second: f64) -> bool {
    const EPS: f64 = 1e-4;
    return (first - second).abs() < EPS;
}

pub async fn match_templated_answer(
    context: &mut Context,
    step: &Step,
    answer: &ConceptMap,
) -> TypeDBResult<Vec<ConceptMap>> {
    let query = apply_query_template(step.docstring().unwrap(), answer);
    let parsed = parse_query(&query)?;
    Ok(context.transaction().query().match_(&parsed.to_string())?.try_collect::<Vec<_>>().await?)
}

fn apply_query_template(query_template: &String, answer: &ConceptMap) -> String {
    let re = Regex::new(r"<answer\.(.+?)\.iid>").unwrap();
    re.replace_all(query_template, |caps: &Captures| format!("{}", get_iid(&answer.map.get(&caps[1]).unwrap())))
        .to_string()
}

fn get_iid(concept: &Concept) -> String {
    let iid = match concept {
        Concept::Entity(Entity { iid, .. }) => iid,
        Concept::Relation(Relation { iid, .. }) => iid,
        Concept::Attribute(Attribute { iid, .. }) => iid,
        _ => unreachable!("Unexpected Concept type: {concept:?}"),
    };
    iid.to_string()
}

pub async fn match_answer_rule(answer_identifiers: &HashMap<&str, &str>, answer: &Rule) -> bool {
    let when_clause = answer_identifiers.get("when").unwrap().to_string();
    let when = parse_patterns(when_clause.as_str()).unwrap()[0].clone().into_conjunction();
    let then_clause = answer_identifiers.get("then").unwrap().to_string();
    let then = parse_patterns(then_clause.as_str()).unwrap()[0].clone().into_variable();
    answer_identifiers.get("label").unwrap().to_string() == answer.label
        && when == answer.when
        && then == Variable::Thing(answer.then.clone())
}

pub async fn create_database_with_timeout(databases: &DatabaseManager, name: String) {
    assert_with_timeout!(databases.create(name.clone()).await.is_ok(), "Database {name} couldn't be created.");
}

#[macro_export]
macro_rules! assert_with_timeout {
    ($expr:expr, $message:expr $(, $arg:expr)* $(,)?) => {{
        't: {
            for _ in 0..Context::STEP_REATTEMPT_LIMIT {
                if $expr {
                    break 't;
                }
                sleep(Context::STEP_REATTEMPT_SLEEP).await;
            }
            panic!($message $(, $arg)*);
        }
    }};
}
