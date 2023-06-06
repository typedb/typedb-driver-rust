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
use typedb_client::{Options, TransactionType};

use crate::{
    behaviour::{Context},
    generic_step_impl,
};

generic_step_impl! {

    #[step(expr = "connection opens with authentication: {word}, {word}")]
    async fn connection_opens_with_authentication(context: &mut Context, login: String, password: String) {
        Connection::new_encrypted(
            &["localhost:11729", "localhost:21729", "localhost:31729"],
            Credential::with_tls(
                login,
                password,
                Some(&PathBuf::from(
                    std::env::var("ROOT_CA")
                        .expect("ROOT_CA environment variable needs to be set for cluster tests to run"),
                )),
            )?,
        )
    }

    #[step(expr = "users delete: {word}")]
    async fn user_delete(context: &mut Context, user: String) {
        // for session_tracker in &mut context.session_trackers {
        //     session_tracker.open_transaction(type_.transaction_type).await.unwrap();
        // }
    }

}
