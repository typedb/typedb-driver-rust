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

use std::{path::PathBuf, sync::Arc, time::Instant};

use chrono::{NaiveDate, NaiveDateTime};
use futures::{StreamExt, TryFutureExt};
use tokio::sync::mpsc;
use typedb_client::{
    common::{
        error::ClientError,
        SessionType::{Data, Schema},
        TransactionType::{Read, Write},
    },
    concept::{Attribute, Concept, DateTimeAttribute, StringAttribute, Thing},
    Connection, Credential, Database, DatabaseManager, Error, Options, Session,
};

const TEST_DATABASE: &str = "test";

macro_rules! permutation_tests {
    { $perm_args:tt
        $( $( # $extra_anno:tt )* async fn $test:ident $args:tt $test_impl:tt )+
    } => {
        permutation_tests!{ @impl $( async fn $test $args $test_impl )+ }
        permutation_tests!{ @impl_per $perm_args  { $( $( # $extra_anno )* async fn $test )+ } }
    };

    { @impl $( async fn $test:ident $args:tt $test_impl:tt )+ } => {
        $( pub async fn $test $args $test_impl )+
    };

    { @impl_per { $($mod:ident => $arg:expr),+ $(,)? } $fns:tt } => {
        $(permutation_tests!{ @impl_mod { $mod => $arg } $fns })+
    };

    { @impl_mod { $mod:ident => $arg:expr } {
        $( $( # $extra_anno:tt )* async fn $test:ident )+
    } } => {
        mod $mod {
        use serial_test::serial;
        $(
            #[tokio::test]
            #[serial($mod)]
            $( # $extra_anno )*
            pub async fn $test() {
                super::$test($arg).await
            }
        )+
        }
    };
}

permutation_tests! {
{
    core => super::new_core_connection().unwrap(),
    cluster => super::new_cluster_connection().unwrap(),
}

async fn basic(connection: Connection) {
    create_test_database_with_schema(connection.clone(), "define person sub entity;")
        .await
        .unwrap();
    let mut databases = DatabaseManager::new(connection);
    assert!(databases.contains(TEST_DATABASE.into()).await.unwrap());

    let session =
        Session::new(databases.get(TEST_DATABASE.into()).await.unwrap(), Data).await.unwrap();
    let transaction = session.transaction(Write).await.unwrap();
    let answer_stream = transaction.query.match_("match $x sub thing;").unwrap();
    let results: Vec<_> = answer_stream.collect().await;
    transaction.commit().await.unwrap();
    assert_eq!(results.len(), 5);
    assert!(results.into_iter().all(|res| res.is_ok()));
}

async fn query_error(connection: Connection) {
    create_test_database_with_schema(connection.clone(), "define person sub entity;")
        .await
        .unwrap();
    let mut databases = DatabaseManager::new(connection);

    let session =
        Session::new(databases.get(TEST_DATABASE.into()).await.unwrap(), Data).await.unwrap();
    let transaction = session.transaction(Write).await.unwrap();
    let answer_stream = transaction.query.match_("match $x sub nonexistent-type;").unwrap();
    let results: Vec<_> = answer_stream.collect().await;
    assert_eq!(results.len(), 1);
    assert!(results.into_iter().all(|res| res.unwrap_err().to_string().contains("[TYR03]")));
}

async fn concurrent_transactions(connection: Connection) {
    create_test_database_with_schema(connection.clone(), "define person sub entity;")
        .await
        .unwrap();
    let mut databases = DatabaseManager::new(connection);

    let session = Arc::new(
        Session::new(databases.get(TEST_DATABASE.into()).await.unwrap(), Data).await.unwrap(),
    );

    let (sender, mut receiver) = mpsc::channel(5 * 5 * 8);

    for _ in 0..8 {
        let sender = sender.clone();
        let session = session.clone();
        tokio::spawn(async move {
            for _ in 0..5 {
                let transaction = session.transaction(Read).await.unwrap();
                let mut answer_stream = transaction.query.match_("match $x sub thing;").unwrap();
                while let Some(result) = answer_stream.next().await {
                    sender.send(result).await.unwrap();
                }
            }
        });
    }
    drop(sender); // receiver expects data while any sender is live

    let mut results = Vec::with_capacity(5 * 5 * 8);
    while let Some(result) = receiver.recv().await {
        results.push(result);
    }
    assert_eq!(results.len(), 5 * 5 * 8);
    assert!(results.into_iter().all(|res| res.is_ok()));
}

async fn query_options(connection: Connection) {
    let schema = r#"define
        person sub entity,
            owns name,
            owns age;
        name sub attribute, value string;
        age sub attribute, value long;
        rule age-rule: when { $x isa person; } then { $x has age 25; };"#;
    create_test_database_with_schema(connection.clone(), schema).await.unwrap();
    let mut databases = DatabaseManager::new(connection);

    let session =
        Session::new(databases.get(TEST_DATABASE.into()).await.unwrap(), Data).await.unwrap();
    let transaction = session.transaction(Write).await.unwrap();
    let data = "insert $x isa person, has name 'Alice'; $y isa person, has name 'Bob';";
    let _ = transaction.query.insert(data);
    transaction.commit().await.unwrap();

    let transaction = session.transaction(Read).await.unwrap();
    let age_count = transaction.query.match_aggregate("match $x isa age; count;").await.unwrap();
    assert_eq!(age_count.into_i64(), 0);

    let with_inference = Options::new_core().infer(true);
    let transaction = session.transaction_with_options(Read, with_inference).await.unwrap();
    let age_count = transaction.query.match_aggregate("match $x isa age; count;").await.unwrap();
    assert_eq!(age_count.into_i64(), 1);
}

async fn many_concept_types(connection: Connection) {
    let schema = r#"define
        person sub entity,
            owns name,
            owns date-of-birth,
            plays friendship:friend;
        name sub attribute, value string;
        date-of-birth sub attribute, value datetime;
        friendship sub relation,
            relates friend;"#;
    create_test_database_with_schema(connection.clone(), schema).await.unwrap();
    let mut databases = DatabaseManager::new(connection);

    let session =
        Session::new(databases.get(TEST_DATABASE.into()).await.unwrap(), Data).await.unwrap();
    let transaction = session.transaction(Write).await.unwrap();
    let data = r#"insert
        $x isa person, has name "Alice", has date-of-birth 1994-10-03;
        $y isa person, has name "Bob", has date-of-birth 1993-04-17;
        (friend: $x, friend: $y) isa friendship;"#;
    let _ = transaction.query.insert(data);
    transaction.commit().await.unwrap();

    let transaction = session.transaction(Read).await.unwrap();
    let mut answer_stream = transaction
        .query
        .match_(
            r#"match
        $p isa person, has name $name, has date-of-birth $date-of-birth;
        $f($role: $p) isa friendship;"#,
        )
        .unwrap();

    while let Some(result) = answer_stream.next().await {
        assert!(result.is_ok());
        let mut result = result.unwrap().map;
        let name = unwrap_string(result.remove("name").unwrap());
        let date_of_birth = unwrap_date_time(result.remove("date-of-birth").unwrap()).date();
        match name.as_str() {
            "Alice" => assert_eq!(date_of_birth, NaiveDate::from_ymd_opt(1994, 10, 3).unwrap()),
            "Bob" => assert_eq!(date_of_birth, NaiveDate::from_ymd_opt(1993, 4, 17).unwrap()),
            _ => unreachable!(),
        }
    }
}

async fn force_close_connection(connection: Connection) {
    create_test_database_with_schema(connection.clone(), "define person sub entity;")
        .await
        .unwrap();
    let mut databases = DatabaseManager::new(connection.clone());

    let database = databases.get(TEST_DATABASE.into()).await.unwrap();
    assert!(database.schema().await.is_ok());

    let session =
        Session::new(databases.get(TEST_DATABASE.into()).await.unwrap(), Data).await.unwrap();
    connection.clone().force_close();

    let schema = database.schema().await;
    assert!(schema.is_err());
    assert_eq!(schema.unwrap_err(), Error::Client(ClientError::ClientIsClosed()));

    let database2 = databases.get(TEST_DATABASE.into()).await;
    assert!(database2.is_err());
    assert_eq!(database2.unwrap_err(), Error::Client(ClientError::ClientIsClosed()));

    let transaction = session.transaction(Write).await;
    assert!(transaction.is_err());
    assert_eq!(transaction.unwrap_err(), Error::Client(ClientError::ClientIsClosed()));

    let session = Session::new(database, Data).await;
    assert!(session.is_err());
    assert_eq!(session.unwrap_err(), Error::Client(ClientError::ClientIsClosed()));
}

async fn force_close_session(connection: Connection) {
    create_test_database_with_schema(connection.clone(), "define person sub entity;")
        .await
        .unwrap();
    let mut databases = DatabaseManager::new(connection.clone());

    let session = Arc::new(
        Session::new(databases.get(TEST_DATABASE.into()).await.unwrap(), Data).await.unwrap(),
    );
    let _transaction = session.transaction(Write).await.unwrap();

    let session2 = session.clone();
    session2.force_close();

    // let answer_stream = transaction.query.match_("match $x sub thing;");
    // assert!(answer_stream.is_err());
    // assert!(transaction.query.match_("match $x sub thing;").is_err());

    let transaction = session.transaction(Write).await;
    assert!(transaction.is_err());
    assert_eq!(transaction.unwrap_err(), Error::Client(ClientError::SessionIsClosed()));

    assert!(Session::new(databases.get(TEST_DATABASE.into()).await.unwrap(), Data).await.is_ok());
}

#[ignore]
async fn streaming_perf(connection: Connection) {
    for i in 0..5 {
        let schema = r#"define
            person sub entity, owns name, owns age;
            name sub attribute, value string;
            age sub attribute, value long;"#;
        create_test_database_with_schema(connection.clone(), schema).await.unwrap();
        let mut databases = DatabaseManager::new(connection.clone());

        let start_time = Instant::now();
        let session =
            Session::new(databases.get(TEST_DATABASE.into()).await.unwrap(), Data).await.unwrap();
        let transaction = session.transaction(Write).await.unwrap();
        for j in 0..100_000 {
            drop(transaction.query.insert(format!("insert $x {j} isa age;").as_str()).unwrap());
        }
        transaction.commit().await.unwrap();
        println!(
            "iteration {i}: inserted and committed 100k attrs in {}ms",
            start_time.elapsed().as_millis()
        );

        let mut start_time = Instant::now();
        let session =
            Session::new(databases.get(TEST_DATABASE.into()).await.unwrap(), Data).await.unwrap();
        let transaction = session.transaction(Read).await.unwrap();
        let mut answer_stream = transaction.query.match_("match $x isa attribute;").unwrap();
        let mut sum: i64 = 0;
        let mut idx = 0;
        while let Some(result) = answer_stream.next().await {
            match result {
                Ok(concept_map) => {
                    for (_, concept) in concept_map {
                        if let Concept::Thing(Thing::Attribute(Attribute::Long(long_attr))) =
                            concept
                        {
                            sum += long_attr.value
                        }
                    }
                }
                Err(err) => {
                    panic!("An error occurred fetching answers of a Match query: {}", err)
                }
            }
            idx = idx + 1;
            if idx == 100_000 {
                println!(
                    "iteration {i}: retrieved and summed 100k attrs in {}ms",
                    start_time.elapsed().as_millis()
                );
                start_time = Instant::now();
            }
        }
        println!("sum is {}", sum);
    }
}
}

fn new_core_connection() -> typedb_client::Result<Connection> {
    Connection::new_plaintext("127.0.0.1:1729")
}

fn new_cluster_connection() -> typedb_client::Result<Connection> {
    Connection::from_init(
        &["localhost:11729", "localhost:21729", "localhost:31729"],
        Credential::with_tls(
            "admin",
            "password",
            Some(&PathBuf::from(
                std::env::var("ROOT_CA").expect(
                    "ROOT_CA environment variable needs to be set for cluster tests to run",
                ),
            )),
        ),
    )
}

async fn create_test_database_with_schema(
    connection: Connection,
    schema: &str,
) -> typedb_client::Result {
    let mut databases = DatabaseManager::new(connection);
    if databases.contains(TEST_DATABASE.into()).await? {
        databases.get(TEST_DATABASE.into()).and_then(Database::delete).await?;
    }
    databases.create(TEST_DATABASE.into()).await?;

    let database = databases.get(TEST_DATABASE.into()).await?;
    let session = Session::new(database, Schema).await?;
    let transaction = session.transaction(Write).await?;
    transaction.query.define(schema).await?;
    transaction.commit().await?;
    Ok(())
}

// Concept helpers
// FIXME should be removed after concept API is implemented
fn unwrap_date_time(concept: Concept) -> NaiveDateTime {
    match concept {
        Concept::Thing(Thing::Attribute(Attribute::DateTime(DateTimeAttribute {
            value, ..
        }))) => value,
        _ => unreachable!(),
    }
}

fn unwrap_string(concept: Concept) -> String {
    match concept {
        Concept::Thing(Thing::Attribute(Attribute::String(StringAttribute { value, .. }))) => value,
        _ => unreachable!(),
    }
}
