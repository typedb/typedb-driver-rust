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
use futures::{future::try_join_all, TryFutureExt};
use typedb_client::{Session, SessionType};

use crate::{generic_step_impl, steps::util, TypeDBWorld};

generic_step_impl! {
    #[step(expr = "connection open schema session for database: {word}")]
    async fn connection_open_schema_session_for_database(world: &mut TypeDBWorld, name: String) {
        world.sessions.push(Session::new(world.databases.get(name).await.unwrap(), SessionType::Schema).await.unwrap());
    }

    #[step(expr = "connection open (data )session for database: {word}")]
    async fn connection_open_data_session_for_database(world: &mut TypeDBWorld, name: String) {
        world.sessions.push(Session::new(world.databases.get(name).await.unwrap(), SessionType::Data).await.unwrap());
    }

    #[step(expr = "connection open schema session(s) for database(s):")]
    async fn connection_open_schema_sessions_for_databases(world: &mut TypeDBWorld, step: &Step) {
        for name in util::iter_table(step) {
            world
                .sessions
                .push(Session::new(world.databases.get(name).await.unwrap(), SessionType::Schema).await.unwrap());
        }
    }

    #[step(expr = "connection open (data )session(s) for database(s):")]
    async fn connection_open_data_sessions_for_databases(world: &mut TypeDBWorld, step: &Step) {
        for name in util::iter_table(step) {
            world
                .sessions
                .push(Session::new(world.databases.get(name).await.unwrap(), SessionType::Data).await.unwrap());
        }
    }

    #[step(expr = "connection open (data )sessions in parallel for databases:")]
    async fn connection_open_data_sessions_in_parallel_for_databases(world: &mut TypeDBWorld, step: &Step) {
        let new_sessions = try_join_all(
            util::iter_table(step)
                .map(|name| world.databases.get(name).and_then(|db| Session::new(db, SessionType::Data))),
        )
        .await.unwrap();
        world.sessions.extend(new_sessions.into_iter());
    }

    #[step(expr = "session(s) is/are null: {word}")]
    #[step(expr = "sessions in parallel are null: {word}")]
    async fn sessions_are_null(_world: &mut TypeDBWorld, is_null: bool) {
        assert!(!is_null); // Rust sessions are not nullable
    }

    #[step(expr = "session(s) is/are open: {word}")]
    #[step(expr = "sessions in parallel are open: {word}")]
    async fn sessions_are_open(world: &mut TypeDBWorld, is_open: bool) {
        for session in &world.sessions {
            assert!(session.is_open() == is_open);
        }
    }

    #[step(expr = "session(s) has/have database: {word}")]
    async fn sessions_have_database(world: &mut TypeDBWorld, name: String) {
        assert_eq!(world.sessions.get(0).unwrap().database_name(), name)
    }

    #[step(expr = "session(s) has/have database(s):")]
    #[step(expr = "sessions in parallel have databases:")]
    async fn sessions_have_databases(world: &mut TypeDBWorld, step: &Step) {
        assert_eq!(step.table.as_ref().unwrap().rows.len(), world.sessions.len());
        for (name, session) in util::iter_table(step).zip(&world.sessions) {
            assert_eq!(name, session.database_name());
        }
    }

    #[step(expr = "set session option {word} to: {word}")]
    async fn set_session_option_to(_world: &mut TypeDBWorld, _option: String, _value: String) {
        todo!()
        // if (!optionSetters.containsKey(option)) {
        //     throw new RuntimeException("Unrecognised option: " + option);
        // }
        // optionSetters.get(option).accept(sessionOptions, value);
    }
}
