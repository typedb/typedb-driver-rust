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
use typeql_lang::parse_query;

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
        match parse_query(step.docstring().unwrap()) {
            Ok(parsed) => {
                let res = context.transaction().query().define(&parsed.to_string()).await;
                assert!(res.is_err());
                assert!(res.unwrap_err().to_string().contains(&exception));
            },
            Err(error) => assert!(error.to_string().contains(&exception)),
        }
    }

    #[step(expr = "typeql insert")]
    async fn typeql_insert(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        assert!(parsed.is_ok());
        let stream = context.transaction().query().insert(&parsed.unwrap().to_string());
        assert!(stream.is_ok());
        let res = stream.unwrap().try_collect::<Vec<_>>().await;
        assert!(res.is_ok());
    }

    #[step(expr = "typeql insert; throws exception")]
    async fn typeql_insert_throws(context: &mut Context, step: &Step) {
        let parsed = parse_query(step.docstring().unwrap());
        if parsed.is_ok() {
            let stream = context.transaction().query().insert(&parsed.unwrap().to_string());
            if stream.is_ok() {
                let res = stream.unwrap().try_collect::<Vec<_>>().await;
                assert!(res.is_err());
            }
        }
    }

    #[step(expr = "typeql insert; throws exception containing {string}")]
    async fn typeql_insert_throws_exception(context: &mut Context, step: &Step, exception: String) {
        match parse_query(step.docstring().unwrap()) {
            Ok(parsed) => {
                match context.transaction().query().insert(&parsed.to_string()) {
                    Ok(stream) => {
                        let res = stream.try_collect::<Vec<_>>().await;
                        assert!(res.is_err());
                        assert!(res.map(|_| ()).unwrap_err().to_string().contains(&exception));
                    },
                    Err(error) => assert!(error.to_string().contains(&exception)),
                }
            },
            Err(error) => assert!(error.to_string().contains(&exception)),
        }
    }
}
