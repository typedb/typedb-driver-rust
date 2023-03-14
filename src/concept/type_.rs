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

use std::fmt;

#[derive(Clone, Debug)]
pub enum Type {
    Thing(ThingType),
    Role(RoleType),
}

#[derive(Clone, Debug)]
pub enum ThingType {
    Root(RootThingType),
    Entity(EntityType),
    Relation(RelationType),
    Attribute(AttributeType),
}

#[derive(Debug)]
pub enum EntityOrThingType {
    EntityType(EntityType),
    RootThingType(RootThingType),
}

#[derive(Clone, Debug)]
pub struct RootThingType {
    pub label: String,
}

impl RootThingType {
    const LABEL: &'static str = "thing";

    pub fn new() -> Self {
        Self { label: String::from(Self::LABEL) }
    }
}

impl Default for RootThingType {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct EntityType {
    pub label: String,
}

impl EntityType {
    pub fn new(label: String) -> Self {
        Self { label }
    }
}

#[derive(Clone, Debug)]
pub struct RelationType {
    pub label: String,
}

impl RelationType {
    pub fn new(label: String) -> Self {
        Self { label }
    }
}

#[derive(Clone, Debug)]
pub enum AttributeType {
    Root(RootAttributeType),
    Boolean(BooleanAttributeType),
    Long(LongAttributeType),
    Double(DoubleAttributeType),
    String(StringAttributeType),
    DateTime(DateTimeAttributeType),
}

#[derive(Clone, Debug)]
pub struct RootAttributeType {
    pub label: String,
}

impl RootAttributeType {
    const LABEL: &'static str = "attribute";

    pub fn new() -> Self {
        Self { label: String::from(Self::LABEL) }
    }
}

impl Default for RootAttributeType {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct BooleanAttributeType {
    pub label: String,
}

impl BooleanAttributeType {
    pub fn new(label: String) -> Self {
        Self { label }
    }
}

#[derive(Clone, Debug)]
pub struct LongAttributeType {
    pub label: String,
}

impl LongAttributeType {
    pub fn new(label: String) -> Self {
        Self { label }
    }
}

#[derive(Clone, Debug)]
pub struct DoubleAttributeType {
    pub label: String,
}

impl DoubleAttributeType {
    pub fn new(label: String) -> Self {
        Self { label }
    }
}

#[derive(Clone, Debug)]
pub struct StringAttributeType {
    pub label: String,
}

impl StringAttributeType {
    pub fn new(label: String) -> Self {
        Self { label }
    }
}

#[derive(Clone, Debug)]
pub struct DateTimeAttributeType {
    pub label: String,
}

impl DateTimeAttributeType {
    pub fn new(label: String) -> Self {
        Self { label }
    }
}

#[derive(Clone, Debug)]
pub struct RoleType {
    pub label: ScopedLabel,
}

impl RoleType {
    pub fn new(label: ScopedLabel) -> Self {
        Self { label }
    }
}

#[derive(Clone, Debug)]
pub struct ScopedLabel {
    pub scope: String,
    pub name: String,
}

impl ScopedLabel {
    pub fn new(scope: String, name: String) -> Self {
        Self { scope, name }
    }
}

impl fmt::Display for ScopedLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.scope, self.name)
    }
}
