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

mod concept;
mod connection;
mod parameter;
mod session_tracker;
mod typeql;
mod util;

use std::collections::HashMap;

use cucumber::{StatsWriter, World};
use futures::future::try_join_all;
use typedb_client::{
    concept::{Attribute, AttributeType, Entity, EntityType, Relation, RelationType, Thing},
    Connection, Database, DatabaseManager, Result as TypeDBResult, Transaction,
};

use self::session_tracker::SessionTracker;

#[derive(Debug, World)]
pub struct Context {
    pub connection: Connection,
    pub databases: DatabaseManager,
    pub session_trackers: Vec<SessionTracker>,
    pub things: HashMap<String, Option<Thing>>,
}

impl Context {
    async fn test(glob: &'static str) -> bool {
        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_panic(info);
            std::process::exit(1);
        }));

        !Self::cucumber()
            .repeat_failed()
            .fail_on_skipped()
            .max_concurrent_scenarios(Some(1))
            .with_default_cli()
            .after(|_, _, _, _, context| {
                Box::pin(async {
                    context.unwrap().after_scenario().await.unwrap();
                })
            })
            .filter_run(glob, |_, _, sc| !sc.tags.iter().any(Self::is_ignore_tag))
            .await
            .execution_has_failed()
    }

    fn is_ignore_tag(t: &String) -> bool {
        t == "ignore" || t == "ignore-typedb" || t == "ignore-client-rust" || t == "ignore-typedb-client-rust"
    }

    async fn after_scenario(&self) -> TypeDBResult {
        try_join_all(self.databases.all().await.unwrap().into_iter().map(Database::delete)).await?;
        Ok(())
    }

    pub fn transaction(&self) -> &Transaction {
        self.session_trackers.get(0).unwrap().transaction()
    }

    pub fn take_transaction(&mut self) -> Transaction {
        self.session_trackers.get_mut(0).unwrap().take_transaction()
    }

    pub async fn get_entity_type(&self, type_label: String) -> TypeDBResult<EntityType> {
        self.transaction().concept().get_entity_type(type_label).await.map(|entity_type| {
            assert!(entity_type.is_some());
            entity_type.unwrap()
        })
    }

    pub async fn get_relation_type(&self, type_label: String) -> TypeDBResult<RelationType> {
        self.transaction().concept().get_relation_type(type_label).await.map(|relation_type| {
            assert!(relation_type.is_some());
            relation_type.unwrap()
        })
    }

    pub async fn get_attribute_type(&self, type_label: String) -> TypeDBResult<AttributeType> {
        self.transaction().concept().get_attribute_type(type_label).await.map(|attribute_type| {
            assert!(attribute_type.is_some());
            attribute_type.unwrap()
        })
    }

    pub fn get_thing(&self, var_name: String) -> &Thing {
        assert!(&self.things.contains_key(&var_name));
        self.things.get(&var_name).unwrap().as_ref().unwrap()
    }

    pub fn get_entity(&self, var_name: String) -> &Entity {
        let thing = self.get_thing(var_name);
        assert!(matches!(thing, Thing::Entity(_)));
        let Thing::Entity(entity) = thing else { unreachable!() };
        entity
    }

    pub fn get_relation(&self, var_name: String) -> &Relation {
        let thing = self.get_thing(var_name);
        assert!(matches!(thing, Thing::Relation(_)));
        let Thing::Relation(relation) = thing else { unreachable!() };
        relation
    }

    pub fn get_attribute(&self, var_name: String) -> &Attribute {
        let thing = self.get_thing(var_name);
        assert!(matches!(thing, Thing::Attribute(_)));
        let Thing::Attribute(attribute) = thing else { unreachable!() };
        attribute
    }

    pub fn insert_thing(&mut self, var_name: String, thing: Option<Thing>) {
        self.things.insert(var_name, thing);
    }

    pub fn insert_entity(&mut self, var_name: String, entity: Option<Entity>) {
        self.insert_thing(var_name, entity.map(Thing::Entity));
    }

    pub fn insert_relation(&mut self, var_name: String, relation: Option<Relation>) {
        self.insert_thing(var_name, relation.map(Thing::Relation));
    }

    pub fn insert_attribute(&mut self, var_name: String, attribute: Option<Attribute>) {
        self.insert_thing(var_name, attribute.map(Thing::Attribute));
    }
}

impl Default for Context {
    fn default() -> Self {
        let connection = Connection::new_plaintext("0.0.0.0:1729").unwrap();
        let databases = DatabaseManager::new(connection.clone());
        Self { connection, databases, session_trackers: Vec::new(), things: HashMap::new() }
    }
}

#[macro_export]
macro_rules! generic_step_impl {
    {$($(#[step($pattern:expr)])+ $async:ident fn $fn_name:ident $args:tt $(-> $res:ty)? $body:block)+} => {
        $($(
        #[given($pattern)]
        #[when($pattern)]
        #[then($pattern)]
        )*
        $async fn $fn_name $args $(-> $res)? $body
        )*
    };
}

#[macro_export]
macro_rules! assert_err {
    ($expr:expr) => {{
        let res = $expr;
        assert!(res.is_err(), "{res:?}")
    }};
}
