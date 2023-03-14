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

use chrono::NaiveDateTime;
use enum_dispatch::enum_dispatch;

use crate::common::IID;
use super::type_::*;

#[derive(Clone, Debug)]
#[enum_dispatch(ThingAPI)]
pub enum Thing {
    Entity(Entity),
    Relation(Relation),
    Attribute(Attribute),
}

trait ThingAPI {
    fn get_iid(&self) -> &IID;
}

// TODO: Storing the Type here is *extremely* inefficient; we could be effectively creating
//       1 million copies of the same data when matching concepts of homogeneous types
#[derive(Clone, Debug)]
pub struct Entity {
    pub iid: IID,
    pub type_: EntityType,
}

#[derive(Clone, Debug)]
pub struct Relation {
    pub iid: IID,
    pub type_: RelationType,
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
} for Entity, Relation
}

#[derive(Clone, Debug)]
pub enum Attribute {
    Boolean { iid: IID, value: bool },
    Long { iid: IID, value: i64 },
    Double { iid: IID, value: f64 },
    String { iid: IID, value: String },
    DateTime { iid: IID, value: NaiveDateTime },
}

impl ThingAPI for Attribute {
    fn get_iid(&self) -> &IID {
        match self {
            Attribute::Boolean { iid, .. } => iid,
            Attribute::Long { iid, .. } => iid,
            Attribute::Double { iid, .. } => iid,
            Attribute::String { iid, .. } => iid,
            Attribute::DateTime { iid, .. } => iid,
        }
    }
}

pub mod attribute {
    #[derive(Copy, Clone, Debug)]
    pub enum ValueType {
        Object = 0,
        Boolean = 1,
        Long = 2,
        Double = 3,
        String = 4,
        DateTime = 5,
    }
}
