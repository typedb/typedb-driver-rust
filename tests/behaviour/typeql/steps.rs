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
use typedb_client::answer::Numeric;
use typeql_lang::parse_query;
use util::{apply_query_template, equals_approximate, iter_map_table, match_answer_concept_map};

use crate::behaviour::util;
use crate::{behaviour::Context, generic_step_impl};

generic_step_impl! {
    #[step(expr = "typeql define")]
    async fn typeql_define(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        assert!(parsed.is_ok());
        let res = context.transaction().query().define(&parsed.unwrap().to_string()).await;
        assert!(res.is_ok());
    }

    #[step(expr = "typeql define; throws exception")]
    async fn typeql_define_throws(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        if parsed.is_ok() {
            let res = context.transaction().query().define(&parsed.unwrap().to_string()).await;
            assert!(res.is_err());
        }
    }

    #[step(expr = "typeql define; throws exception containing {string}")]
    async fn typeql_define_throws_exception(context: &mut Context, step: &Step, exception: String) {
        let result = async { parse_query(step.docstring().unwrap()).map_err(|error| error.to_string()) }
            .and_then(|parsed| async move {
                context.transaction().query().define(&parsed.to_string()).await.map_err(|error| error.to_string())
            })
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains(&exception));
    }

    #[step(expr = "typeql undefine")]
    async fn typeql_undefine(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        assert!(parsed.is_ok());
        let res = context.transaction().query().undefine(&parsed.unwrap().to_string()).await;
        assert!(res.is_ok());
    }

    #[step(expr = "typeql undefine; throws exception")]
    async fn typeql_undefine_throws(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        if parsed.is_ok() {
            let res = context.transaction().query().undefine(&parsed.unwrap().to_string()).await;
            assert!(res.is_err());
        }
    }

    #[step(expr = "typeql insert")]
    async fn typeql_insert(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        assert!(parsed.is_ok());
        let inserted = context.transaction().query().insert(&parsed.unwrap().to_string());
        assert!(inserted.is_ok());
        let res = inserted.unwrap().try_collect::<Vec<_>>().await;
        assert!(res.is_ok());
    }

    #[step(expr = "typeql insert; throws exception")]
    async fn typeql_insert_throws(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        if parsed.is_ok() {
            let inserted = context.transaction().query().insert(&parsed.unwrap().to_string());
            if inserted.is_ok() {
                let res = inserted.unwrap().try_collect::<Vec<_>>().await;
                assert!(res.is_err());
            }
        }
    }

    #[step(expr = "typeql insert; throws exception containing {string}")]
    async fn typeql_insert_throws_exception(context: &mut Context, step: &Step, exception: String) {
        let result = async {
            parse_query(step.docstring().unwrap()).map_err(|error| error.to_string()).and_then(|parsed| {
                context.transaction().query().insert(&parsed.to_string()).map_err(|error| error.to_string())
            })
        }
        .and_then(|inserted| async { inserted.try_collect::<Vec<_>>().await.map_err(|error| error.to_string()) })
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains(&exception));
    }

    #[step(expr = "typeql delete")]
    async fn typeql_delete(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        assert!(parsed.is_ok());
        let res = context.transaction().query().delete(&parsed.unwrap().to_string()).await;
        assert!(res.is_ok());
    }

    #[step(expr = "typeql delete; throws exception")]
    async fn typeql_delete_throws(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        if parsed.is_ok() {
            let res = context.transaction().query().delete(&parsed.unwrap().to_string()).await;
            assert!(res.is_err());
        }
    }

    #[step(expr = "typeql update")]
    async fn typeql_update(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        assert!(parsed.is_ok());
        let updated = context.transaction().query().update(&parsed.unwrap().to_string());
        assert!(updated.is_ok());
        let res = updated.unwrap().try_collect::<Vec<_>>().await;
        assert!(res.is_ok());
    }

    #[step(expr = "typeql update; throws exception")]
    async fn typeql_update_throws(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        if parsed.is_ok() {
            let updated = context.transaction().query().update(&parsed.unwrap().to_string());
            if updated.is_ok() {
                let res = updated.unwrap().try_collect::<Vec<_>>().await;
                assert!(res.is_err());
            }
        }
    }

    #[step(expr = "get answers of typeql match")]
    async fn get_answers_typeql_match(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        assert!(parsed.is_ok());
        let stream = context.transaction().query().match_(&parsed.unwrap().to_string());
        assert!(stream.is_ok());
        let res = stream.unwrap().try_collect::<Vec<_>>().await;
        assert!(res.is_ok());
        context.answer = res.unwrap();
    }

    #[step(expr = "get answers of typeql insert")]
    async fn get_answers_typeql_insert(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        assert!(parsed.is_ok());
        let inserted = context.transaction().query().insert(&parsed.unwrap().to_string());
        assert!(inserted.is_ok());
        let res = inserted.unwrap().try_collect::<Vec<_>>().await;
        assert!(res.is_ok());
        context.answer = res.unwrap();
    }

    #[step(expr = "answer size is: {int}")]
    async fn answer_size(context: &mut Context, expected_answers: usize) {
        let actual_answers = context.answer.len();
        assert_eq!(
            actual_answers, expected_answers,
            "The number of identifier entries (rows) should match the number of answers, \
            but found {expected_answers} identifier entries and {actual_answers} answers."
        );
    }

    #[step(expr = "uniquely identify answer concepts")]
    async fn uniquely_identify_answer_concepts(context: &mut Context, step: &Step) {
        let step_table = iter_map_table(step).collect::<Vec<_>>();
        let expected_answers = step_table.len();
        let actual_answers = context.answer.len();
        assert_eq!(
            actual_answers, expected_answers,
            "The number of identifier entries (rows) should match the number of answers, \
            but found {expected_answers} identifier entries and {actual_answers} answers."
        );
        let mut matched_rows = 0;
        for ans_row in &context.answer {
            for table_row in &step_table {
                if match_answer_concept_map(context, table_row, ans_row).await {
                    matched_rows += 1;
                    break;
                }
            }
        }
        assert_eq!(
            matched_rows, actual_answers,
            "An identifier entry (row) should match 1-to-1 to an answer, but there are only {matched_rows} \
            matched entries of given {actual_answers}."
        );
    }

    #[step(expr = "typeql match; throws exception")]
    async fn typeql_match_throws(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        match parsed {
            Ok(_) => {
                let matched = context.transaction().query().match_(&parsed.unwrap().to_string());
                if matched.is_ok() {
                    let res = matched.unwrap().try_collect::<Vec<_>>().await;
                    assert!(res.is_err());
                }
            }
            // NOTE: We manually close transaction here, because we want to align with all non-rust and non-java clients,
            // where parsing happens at server-side which closes transaction if they fail
            Err(_) => {
                for session_tracker in &mut context.session_trackers {
                    session_tracker.transactions_mut().clear();
                }
            }
        }
    }

    #[step(expr = "each answer satisfies")]
    async fn each_answer_satisfies(context: &mut Context, step: &Step) {
        for answer in &context.answer {
            let query = apply_query_template(step.docstring().unwrap(), answer);
            let parsed = parse_query(&query);
            assert!(parsed.is_ok());
            let stream = context.transaction().query().match_(&parsed.unwrap().to_string());
            assert!(stream.is_ok());
            let res = stream.unwrap().try_collect::<Vec<_>>().await;
            assert!(res.is_ok());
            assert_eq!(res.unwrap().len(), 1);
        }
    }

    #[step(expr = "templated typeql match; throws exception")]
    async fn templated_typeql_match_throws(context: &mut Context, step: &Step) {
        for answer in &context.answer {
            let query = apply_query_template(step.docstring().unwrap(), answer);
            let parsed = parse_query(&query);
            if parsed.is_ok() {
                let stream = context.transaction().query().match_(&parsed.unwrap().to_string());
                if stream.is_ok() {
                    let res = stream.unwrap().try_collect::<Vec<_>>().await;
                    assert!(res.is_err());
                }
            }
        }
    }

    #[step(expr = "order of answer concepts is")]
    async fn order_of_answer_concept(context: &mut Context, step: &Step) {
        let step_table = iter_map_table(step).collect::<Vec<_>>();
        let expected_answers = step_table.len();
        let actual_answers = context.answer.len();
        assert_eq!(
            actual_answers, expected_answers,
            "The number of identifier entries (rows) should match the number of answers, \
            but found {expected_answers} identifier entries and {actual_answers} answers."
        );
        for i in 0..expected_answers {
            let ans_row = &context.answer.get(i).unwrap();
            let table_row = &step_table.get(i).unwrap();
            assert!(
                match_answer_concept_map(context, table_row, ans_row).await,
                "The answer at index {i} does not match the identifier entry (row) at index {i}."
            );
        }
    }

    #[step(expr = "get answer of typeql match aggregate")]
    async fn get_answers_typeql_match_aggregate(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        assert!(parsed.is_ok());
        let res = context.transaction().query().match_aggregate(&parsed.unwrap().to_string()).await;
        assert!(res.is_ok());
        context.numeric_answer = res.unwrap();
    }

    #[step(expr = "aggregate value is: {float}")]
    async fn aggregate_value(context: &mut Context, expected_answer: f64) {
        let answer: f64 = match context.numeric_answer {
            Numeric::Long(value) => value as f64,
            Numeric::Double(value) => value,
            Numeric::NaN => unreachable!("Last answer in NaN while expected answer is not."),
        };
        assert!(
            equals_approximate(answer, expected_answer),
            "Last answer is {answer} while expected answer is {expected_answer}"
        );
    }

    #[step(expr = "aggregate answer is not a number")]
    async fn aggregate_answer_is_nan(context: &mut Context) {
        assert!(matches!(context.numeric_answer, Numeric::NaN));
    }

    #[step(expr = "typeql match aggregate; throws exception")]
    async fn typeql_match_aggregate_throws(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        if parsed.is_ok() {
            let res = context.transaction().query().match_aggregate(&parsed.unwrap().to_string()).await;
            assert!(res.is_err());
        }
    }

    // #[step(expr = "get answers of typeql match group")]
    // async fn get_answers_typeql_match_group(context: &mut Context, step: &Step) {
    //     let parsed = parse_query(step.docstring().unwrap());
    //     assert!(parsed.is_ok());
    //     let stream = context.transaction().query().match_(&parsed.unwrap().to_string());
    //     assert!(stream.is_ok());
    //     let res = stream.unwrap().try_collect::<Vec<_>>().await;
    //     assert!(res.is_ok());
    //     context.answer = res.unwrap();
    // }
}
