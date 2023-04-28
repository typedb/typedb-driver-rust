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

use async_trait::async_trait;
use chrono::NaiveDateTime;

use super::type_::{AttributeType, EntityType, RelationType};
use crate::{
    common::{Result, IID},
    Transaction,
};

#[async_trait]
pub trait ThingAPI {
    fn get_iid(&self) -> &IID;
}

macro_rules! default_impl {
    { impl $trait:ident $body:tt for $($t:ident),* $(,)? } => {
        $(impl $trait for $t $body)*
    }
}

default_impl! {
    impl ThingAPI {
        fn get_iid(&self) -> &IID {
            &self.iid
        }
    } for Entity, Relation, Attribute
}

// TODO: Storing the Type here is *extremely* inefficient; we could be effectively creating
//       1 million copies of the same data when matching concepts of homogeneous types
#[derive(Clone, Debug)]
pub struct Entity {
    pub iid: IID,
    pub type_: EntityType,
}

impl Entity {
    pub fn new(iid: IID, type_: EntityType) -> Self {
        Self { iid, type_ }
    }
}

#[derive(Clone, Debug)]
pub struct Relation {
    pub iid: IID,
    pub type_: RelationType,
}

impl Relation {
    pub fn new(iid: IID, type_: RelationType) -> Self {
        Self { iid, type_ }
    }
}

#[derive(Clone, Debug)]
pub struct Attribute {
    pub iid: IID,
    pub type_: AttributeType,
    pub value: Value,
}

impl Attribute {
    pub fn new(iid: IID, type_: AttributeType, value: Value) -> Self {
        Self { iid, type_, value }
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    Boolean(bool),
    Long(i64),
    Double(f64),
    String(String),
    DateTime(NaiveDateTime),
}
