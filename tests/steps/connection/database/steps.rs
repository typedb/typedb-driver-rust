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

use std::collections::HashSet;

use cucumber::{gherkin::Step, given, then, when};
use futures::{future::try_join_all, TryFutureExt};
use typedb_client::Database;

use crate::{generic_step_impl, TypeDBWorld};

generic_step_impl! {
    #[step(expr = "connection create database: {word}")]
    async fn connection_create_database(world: &mut TypeDBWorld, name: String) {
        world.databases.create(name).await.unwrap();
    }

    #[step(expr = "connection create database(s):")]
    async fn connection_create_databases(world: &mut TypeDBWorld, step: &Step) {
        for name in step.table.as_ref().unwrap().rows.iter().flatten() {
            world.databases.create(name).await.unwrap();
        }
    }

    #[step("connection create databases in parallel:")]
    async fn connection_create_databases_in_parallel(world: &mut TypeDBWorld, step: &Step) {
        try_join_all(step.table.as_ref().unwrap().rows.iter().flatten().map(|name| world.databases.create(name)))
            .await
            .unwrap();
    }

    #[step(expr = "connection delete database: {word}")]
    async fn connection_delete_database(world: &mut TypeDBWorld, name: String) {
        world.databases.get(name).and_then(Database::delete).await.unwrap();
    }

    #[step(expr = "connection delete database(s):")]
    async fn connection_delete_databases(world: &mut TypeDBWorld, step: &Step) {
        for name in step.table.as_ref().unwrap().rows.iter().flatten() {
            world.databases.get(name).and_then(Database::delete).await.unwrap();
        }
    }

    #[step(expr = "connection delete databases in parallel:")]
    async fn connection_delete_databases_in_parallel(world: &mut TypeDBWorld, step: &Step) {
        try_join_all(
            step.table
                .as_ref()
                .unwrap()
                .rows
                .iter()
                .flatten()
                .map(|name| world.databases.get(name).and_then(Database::delete)),
        )
        .await
        .unwrap();
    }

    #[step(expr = "connection delete database; throws exception: {word}")]
    async fn connection_delete_database_throws_exception(world: &mut TypeDBWorld, name: String) {
        assert!(world.databases.get(name).and_then(Database::delete).await.is_err());
    }

    #[step(expr = "connection delete database(s); throws exception")]
    async fn connection_delete_databases_throws_exception(world: &mut TypeDBWorld, step: &Step) {
        for name in step.table.as_ref().unwrap().rows.iter().flatten() {
            assert!(world.databases.get(name).and_then(Database::delete).await.is_err());
        }
    }

    #[step(expr = "connection has database: {word}")]
    async fn connection_has_database(world: &mut TypeDBWorld, name: String) {
        assert!(world.databases.contains(name).await.unwrap());
    }

    #[step(expr = "connection has database(s):")]
    async fn connection_has_databases(world: &mut TypeDBWorld, step: &Step) {
        let names: HashSet<String> =
            step.table.as_ref().unwrap().rows.iter().flatten().map(|name| name.to_owned()).collect();
        let all_databases = world.databases.all().await.unwrap().into_iter().map(|db| db.name().to_owned()).collect();
        assert_eq!(names, all_databases);
    }

    #[step(expr = "connection does not have database: {word}")]
    async fn connection_does_not_have_database(world: &mut TypeDBWorld, name: String) {
        assert!(!world.databases.contains(name).await.unwrap());
    }

    #[step(expr = "connection does not have database(s):")]
    async fn connection_does_not_have_databases(world: &mut TypeDBWorld, step: &Step) {
        let all_databases: HashSet<String> =
            world.databases.all().await.unwrap().into_iter().map(|db| db.name().to_owned()).collect();
        for name in step.table.as_ref().unwrap().rows.iter().flatten() {
            assert!(!all_databases.contains(name));
        }
    }
}
