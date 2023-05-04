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

use futures::Stream;

use crate::{common::Transitivity, concept::Entity, Result, Transaction};

#[derive(Clone, Debug)]
pub struct RootThingType;

impl RootThingType {
    const LABEL: &'static str = "thing";

    pub fn new() -> Self {
        Self
    }
}

#[derive(Clone, Debug)]
pub struct EntityType {
    pub label: String,
    pub is_root: bool,
    pub is_abstract: bool,
}

impl EntityType {
    pub fn new(label: String, is_root: bool, is_abstract: bool) -> Self {
        Self { label, is_root, is_abstract }
    }

    pub async fn delete(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().entity_type_delete(self.clone()).await
    }

    pub async fn set_label(&mut self, transaction: &Transaction<'_>, new_label: String) -> Result {
        transaction.concept().entity_type_set_label(self.clone(), new_label).await
    }

    pub async fn set_abstract(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().entity_type_set_abstract(self.clone()).await
    }

    pub async fn unset_abstract(&mut self, transaction: &Transaction<'_>) -> Result {
        transaction.concept().entity_type_unset_abstract(self.clone()).await
    }

    pub async fn create(&self, transaction: &Transaction<'_>) -> Result<Entity> {
        transaction.concept().entity_type_create(self.clone()).await
    }

    pub async fn is_deleted(&self, transaction: &Transaction<'_>) -> Result<bool> {
        transaction.concept().get_entity_type(self.label.clone()).await.map(|res| res.is_some())
    }

    pub async fn get_supertype(&self, transaction: &Transaction<'_>) -> Result<Self> {
        transaction.concept().entity_type_get_supertype(self.clone()).await
    }

    pub async fn set_supertype(&mut self, transaction: &Transaction<'_>, supertype_label: String) -> Result {
        transaction.concept().entity_type_set_supertype(self.clone(), supertype_label).await
    }

    pub fn get_supertypes(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<Self>>> {
        transaction.concept().entity_type_get_supertypes(self.clone())
    }

    pub fn get_subtypes(&self, transaction: &Transaction<'_>) -> Result<impl Stream<Item = Result<Self>>> {
        transaction.concept().entity_type_get_subtypes(self.clone(), Transitivity::Transitive)
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
pub struct AttributeType {
    pub label: String,
    pub value_type: ValueType,
}

impl AttributeType {
    pub fn new(label: String, value_type: ValueType) -> Self {
        Self { label, value_type }
    }
}

#[derive(Clone, Debug)]
pub enum ValueType {
    Object,
    Boolean,
    Long,
    Double,
    String,
    DateTime,
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
