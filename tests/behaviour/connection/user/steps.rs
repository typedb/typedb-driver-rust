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

use std::path::PathBuf;

use cucumber::{gherkin::Step, given, then, when};
use typedb_client::{Connection, Credential, Options, TransactionType};

use crate::{behaviour::Context, generic_step_impl};

generic_step_impl! {

    #[step(expr = "users get all")]
    async fn users_get_all(context: &mut Context) {
        let res = context.users.all().await;
        assert!(res.is_ok());
        panic!("{res:?}");
    }

    #[step(expr = "users contains: {word}")]
    async fn users_contains(context: &mut Context, username: String) {
        let res = context.users.contains(username).await;
        assert!(res.is_ok(), "{:?}", res.err());
        assert!(res.unwrap());
    }

    #[step(expr = "users not contains: {word}")]
    async fn users_not_contains(context: &mut Context, username: String) {
        let res = context.users.contains(username).await;
        assert!(res.is_ok(), "{:?}", res.err());
        assert!(!res.unwrap());
    }

    #[step(expr = "users create: {word}, {word}")]
    async fn users_create(context: &mut Context, username: String, password: String) {
        let res = context.users.create(username, password).await;
        assert!(res.is_ok(), "{:?}", res.err());
    }

    #[step(expr = "users password set: {word}, {word}")]
    async fn users_password_set(context: &mut Context, username: String, password: String) {
        let res = context.users.set_password(username, password).await;
        assert!(res.is_ok(), "{:?}", res.err());
    }

    #[step(expr = "users delete: {word}")]
    async fn user_delete(context: &mut Context, username: String) {
        let res = context.users.delete(username).await;
        assert!(res.is_ok(), "{:?}", res.err());
    }

}
