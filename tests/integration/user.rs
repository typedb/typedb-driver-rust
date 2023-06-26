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

use futures::future::try_join_all;
use serial_test::serial;
use typedb_client::{Connection, Credential, Result as TypeDBResult, UserManager};

use super::common;
use crate::test_for_each_arg;

test_for_each_arg! {
    {
        cluster => common::new_cluster_connection().unwrap(),
    }

    async fn create_and_delete_user(connection: Connection) -> TypeDBResult {
        let users = UserManager::new(connection);
        users.create(String::from("user"), String::from("password")).await?;
        assert_eq!(2, users.all().await?.len());
        users.delete(String::from("user")).await?;
        assert_eq!(1, users.all().await?.len());
        Ok(())
    }

    async fn create_users_and_purge(connection: Connection) -> TypeDBResult {
        let users = UserManager::new(connection);
        users.create(String::from("user"), String::from("password")).await?;
        assert_eq!(2, users.all().await?.len());
        users.create(String::from("user2"), String::from("password2")).await?;
        assert_eq!(3, users.all().await?.len());

        try_join_all(
            users
                .all()
                .await
                .unwrap()
                .into_iter()
                .filter(|user| dbg!(&user.username) != "admin")
                .map(|user| users.delete(dbg!(user.username))),
        )
        .await?;
        assert_eq!(1, users.all().await?.len());
        Ok(())
    }

    async fn create_users_reconnect_and_purge(connection: Connection) -> TypeDBResult {
        assert_eq!(3, connection.server_count());

        let users = UserManager::new(connection);
        users.create(String::from("user"), String::from("password")).await?;
        assert_eq!(2, users.all().await?.len());
        users.create(String::from("user2"), String::from("password2")).await?;
        assert_eq!(3, users.all().await?.len());

        let connection = common::new_cluster_connection().unwrap();
        let users = UserManager::new(connection);
        assert_eq!(3, users.all().await?.len());
        try_join_all(
            users
                .all()
                .await
                .unwrap()
                .into_iter()
                .filter(|user| dbg!(&user.username) != "admin")
                .map(|user| users.delete(dbg!(user.username))),
        )
        .await?;
        assert_eq!(1, users.all().await?.len());
        Ok(())
    }
}
