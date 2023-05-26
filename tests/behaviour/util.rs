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

use chrono::{NaiveDateTime, NaiveTime};
use cucumber::gherkin::Step;
use futures::{TryFutureExt, TryStreamExt};
use regex::{Captures, Regex};
use std::collections::HashMap;
use typedb_client::{
    answer::ConceptMap,
    concept::{
        Attribute, AttributeType, Concept, Entity, EntityType, HasFilter, Relation, RelationType, RoleType,
        ScopedLabel, Thing, ThingType, Value, ValueType,
    },
    Annotation,
};
use typeql_lang::{
    parse_pattern, parse_queries, parse_query,
    pattern::{ThingVariableBuilder, TypeVariableBuilder},
    query::{AggregateQueryBuilder, TypeQLDefine, TypeQLInsert, TypeQLMatch, TypeQLUndefine},
    typeql_match, var,
};

use crate::behaviour::Context;

pub fn iter_table(step: &Step) -> impl Iterator<Item = &str> {
    step.table().unwrap().rows.iter().flatten().map(String::as_str)
}

pub fn iter_map_table(step: &Step) -> impl Iterator<Item = HashMap<&String, &String>> {
    let (keys, rows) = step.table().unwrap().rows.split_first().unwrap();
    rows.iter().map(|row| keys.iter().zip(row).collect())
}

pub async fn match_answer_concept_map(
    context: &Context,
    answer_identifiers: &HashMap<&String, &String>,
    answer: &ConceptMap,
) -> bool {
    for key in answer_identifiers.keys() {
        if !(answer.map.contains_key(key.clone())
            && match_answer_concept(context, answer_identifiers.get(key).unwrap(), answer.get(key).unwrap()).await)
        {
            return false;
        }
    }
    true
}

pub async fn match_answer_concept(context: &Context, answer_identifier: &String, answer: &Concept) -> bool {
    let identifiers: Vec<&str> = answer_identifier.splitn(2, ":").collect();
    match identifiers[0] {
        "key" => key_values_equal(context, identifiers[1], answer).await,
        "label" => labels_equal(identifiers[1], answer),
        "value" => values_equal(identifiers[1], answer),
        _ => unreachable!(),
    }
}

async fn key_values_equal(context: &Context, identifiers: &str, answer: &Concept) -> bool {
    let attribute: Vec<&str> = identifiers.splitn(2, ":").collect();
    assert_eq!(attribute.len(), 2, "Unexpected table cell format: {identifiers}.");

    let filter = HasFilter::Annotations(Vec::from([Annotation::Key]));
    let res = match answer {
        Concept::Entity(entity) => async { entity.get_has(context.transaction(), filter) }
            .and_then(|stream| async { stream.try_collect::<Vec<_>>().await }).await,
        Concept::Attribute(attr) => async { attr.get_has(context.transaction(), filter) }
            .and_then(|stream| async { stream.try_collect::<Vec<_>>().await }).await,
        Concept::Relation(rel) => async { rel.get_has(context.transaction(), filter) }
            .and_then(|stream| async { stream.try_collect::<Vec<_>>().await }).await,
        _ => unreachable!()
    };
    match res {
        Ok(_) => {
            let equals = res.unwrap().into_iter().filter(|attr| attr.type_.label == attribute[0]).collect::<Vec<_>>()
                .first().and_then(|attr| Some(value_equals_str(&attr.value, attribute[1])));
            match equals {
                Some(val) => val,
                None => false
            }
        },
        Err(_) => false
    }
}

fn labels_equal(identifier: &str, answer: &Concept) -> bool {
    let mut binding = String::new();
    let label = match answer {
        Concept::EntityType(type_) => {
            binding = type_.clone().label;
            &binding
        }
        Concept::RoleType(RoleType { label: ScopedLabel { scope, name }, .. }) => {
            binding = format!("{scope}:{name}");
            &binding
        }
        Concept::Entity(Entity { type_: EntityType { label, .. }, .. }) => label,
        Concept::Relation(Relation { type_: RelationType { label, .. }, .. }) => label,
        Concept::RootThingType(_) => {
            binding = String::from("thing");
            &binding
        }
        Concept::RelationType(RelationType { label, .. }) => label,
        Concept::AttributeType(AttributeType { label, .. }) => label,
        Concept::Attribute(Attribute { type_: AttributeType { label, .. }, .. }) => label,
    };
    label == identifier
}

fn values_equal(identifiers: &str, answer: &Concept) -> bool {
    let attribute: Vec<&str> = identifiers.splitn(2, ":").collect();
    assert_eq!(attribute.len(), 2, "Unexpected table cell format: {identifiers}.");
    match answer {
        Concept::Attribute(Attribute { value, .. })
        => value_equals_str(value, attribute[1]),
        _ => false,
    }
}

fn value_equals_str(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(val) => val == expected,
        Value::Long(val) => {
            expected.parse::<i64>().and_then(|expected| Ok(expected.eq(val))).unwrap_or_else(|_| false)
        }
        Value::Double(val) => {
            expected
                .parse::<f64>()
                .and_then(|expected| Ok(equals_approximate(expected, *val)))
                .unwrap_or_else(|_| false)
        }
        Value::Boolean(val) => {
            expected.parse::<bool>().and_then(|expected| Ok(expected.eq(val))).unwrap_or_else(|_| false)
        }
        Value::DateTime(val) => format_datetime(val) == expected,
    }
}

fn get_iid(concept: &Concept) -> String {
    let iid = match concept {
        Concept::Entity(Entity { iid, .. }) => iid,
        Concept::Attribute(Attribute { iid, .. }) => iid,
        Concept::Relation(Relation { iid, .. }) => iid,
        _ => unreachable!(),
    };
    format!("0x{iid}")
}

pub fn equals_approximate(first: f64, second: f64) -> bool {
    const EPS: f64 = 1e-4;
    return (first - second).abs() < EPS;
}

pub fn apply_query_template(query_template: &String, answer: &ConceptMap) -> String {
    let re = Regex::new(r"<answer\.(.+?)\.iid>").unwrap();
    re.replace_all(query_template, |caps: &Captures| format!("{}", get_iid(&answer.map.get(&caps[1]).unwrap())))
        .to_string()
}

fn format_datetime(datetime: &NaiveDateTime) -> String {
    if datetime.time() == NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
        format!("{0}", datetime.date())
    } else {
        format!("{0}", datetime)
    }
}

// fn format_double(double: &f64) -> String {
//     let formatted = format!("{:.12}", double).trim_end_matches('0').to_string();
//     if formatted.ends_with('.') {
//         formatted + "0"
//     } else {
//         formatted
//     }
// }

