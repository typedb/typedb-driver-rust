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

/*
use std::path::PathBuf;

use futures::{StreamExt, TryFutureExt};
use serial_test::serial;
use typedb_client::{
    cluster,
    common::{error::ClientError, Credential, SessionType::Data, TransactionType::Write},
    Error,
    SessionType::Schema,
};

const TEST_DATABASE: &str = "test";

#[tokio::test]
#[serial]
async fn basic() {
    let mut client = new_cluster_client().await.unwrap();
    create_test_database_with_schema(&mut client, "define person sub entity;").await.unwrap();
    assert!(client.databases().contains(TEST_DATABASE.to_owned()).await.unwrap());

    let mut session = client.session(TEST_DATABASE.to_owned(), Data).await.unwrap();
    let transaction = session.transaction(Write).await.unwrap();
    let mut answer_stream = transaction.query.match_("match $x sub thing;").unwrap();
    while let Some(result) = answer_stream.next().await {
        assert!(dbg!(result).is_ok())
    }
    transaction.commit().await.unwrap();
}

#[tokio::test]
#[serial]
async fn force_close_client() {
    let mut client = new_cluster_client().await.unwrap();
    create_test_database_with_schema(&mut client, "define person sub entity;").await.unwrap();
    assert!(client.databases().contains(TEST_DATABASE.to_owned()).await.unwrap());

    let mut database = client.databases().get(TEST_DATABASE.to_owned()).await.unwrap();
    assert!(database.schema().await.is_ok());

    let mut session = client.session(TEST_DATABASE.to_owned(), Data).await.unwrap();
    let client2 = client.clone();
    client2.force_close();

    let schema = database.schema().await;
    assert!(schema.is_err());
    assert_eq!(schema.unwrap_err(), Error::Client(ClientError::ClientIsClosed()));

    let database = client.databases().get(TEST_DATABASE.to_owned()).await;
    assert!(database.is_err());
    assert_eq!(database.unwrap_err(), Error::Client(ClientError::ClientIsClosed()));

    let transaction = session.transaction(Write).await;
    assert!(transaction.is_err());
    assert_eq!(transaction.unwrap_err(), Error::Client(ClientError::ClientIsClosed()));

    let session = client.session(TEST_DATABASE.to_owned(), Data).await;
    assert!(session.is_err());
    assert_eq!(session.unwrap_err(), Error::Client(ClientError::ClientIsClosed()));
}

#[tokio::test]
#[serial]
async fn force_close_session() {
    let mut client = new_cluster_client().await.unwrap();
    create_test_database_with_schema(&mut client, "define person sub entity;").await.unwrap();
    assert!(client.databases().contains(TEST_DATABASE.to_owned()).await.unwrap());

    // let mut session = Arc::new(client.session(TEST_DATABASE.to_owned(), Data).await.unwrap());
    // let mut transaction = session.transaction(Write).await.unwrap();

    // let session2 = session.clone();
    // session2.force_close();

    // let mut answer_stream = transaction.query.match_("match $x sub thing;").unwrap();
    // assert!(matches!(answer_stream.next().await, Some(Err(_))));
    // assert!(answer_stream.next().await.is_none());
    // assert!(transaction.query.match_("match $x sub thing;").is_err());

    // let transaction = session.transaction(Write).await;
    // assert!(transaction.is_err());
    // assert_eq!(transaction.unwrap_err(), Error::Client(ClientError::SessionIsClosed()));

    // assert!(client.session(TEST_DATABASE.to_owned(), Data).await.is_ok());
}

async fn new_cluster_client() -> typedb_client::Result<cluster::Client> {
    cluster::Client::new(
        &["localhost:11729", "localhost:21729", "localhost:31729"],
        Credential::with_tls(
            "admin",
            "password",
            Some(&PathBuf::from(std::env::var("ROOT_CA").unwrap())),
        ),
    )
    .await
}

async fn create_test_database_with_schema(
    client: &mut cluster::Client,
    schema: &str,
) -> typedb_client::Result {
    if client.databases().contains(TEST_DATABASE.to_owned()).await? {
        client
            .databases()
            .get(TEST_DATABASE.to_owned())
            .and_then(cluster::Database::delete)
            .await?;
    }
    client.databases().create(TEST_DATABASE.to_owned()).await?;

    let mut session = client.session(TEST_DATABASE.to_owned(), Schema).await?;
    let transaction = session.transaction(Write).await?;
    transaction.query.define(schema).await?;
    transaction.commit().await?;
    Ok(())
}
*/
