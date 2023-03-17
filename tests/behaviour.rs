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

use cucumber::{StatsWriter, World};
use typedb_client::{Connection, DatabaseManager, Session};

mod steps;

#[derive(Debug, World)]
pub struct TypeDBWorld {
    pub connection: Connection,
    pub databases: DatabaseManager,
    pub sessions: Vec<Session>,
}

impl TypeDBWorld {
    async fn test(glob: &'static str) -> bool {
        !Self::cucumber()
            .fail_on_skipped()
            .max_concurrent_scenarios(Some(1))
            .with_default_cli()
            .filter_run(glob, |_, _, sc| !sc.tags.iter().any(|t| t == "ignore" || t == "ignore-typedb"))
            .await
            .execution_has_failed()
    }
}

impl Default for TypeDBWorld {
    fn default() -> Self {
        let connection = Connection::new_plaintext("0.0.0.0:1729").unwrap();
        let databases = DatabaseManager::new(connection.clone());
        Self { connection, databases, sessions: Vec::new() }
    }
}

#[macro_export]
macro_rules! generic_step_impl {
    {$($(#[step($pattern:expr)])+ $async:ident fn $fn_name:ident $args:tt $body:tt)+} => {
        $($(
        #[given($pattern)]
        #[when($pattern)]
        #[then($pattern)]
        )*
        $async fn $fn_name $args $body
        )*
    };
}
