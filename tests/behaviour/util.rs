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

use std::{collections::HashMap, ops::Deref, str::FromStr};

use cucumber::gherkin::Step;
use typedb_client::{concept::ValueType, Annotation, TransactionType};

pub fn iter_table(step: &Step) -> impl Iterator<Item = &str> {
    step.table().unwrap().rows.iter().flatten().map(String::as_str)
}

#[derive(Clone, Copy, Debug)]
pub struct ValueTypeParse(ValueType);

impl Into<ValueType> for ValueTypeParse {
    fn into(self) -> ValueType {
        self.0
    }
}

impl FromStr for ValueTypeParse {
    type Err = ();

    fn from_str(type_: &str) -> Result<Self, Self::Err> {
        Ok(match type_ {
            "boolean" => Self(ValueType::Boolean),
            "long" => Self(ValueType::Long),
            "double" => Self(ValueType::Double),
            "string" => Self(ValueType::String),
            "datetime" => Self(ValueType::DateTime),
            _ => unreachable!("`{type_}` is not a valid value type"),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TransactionTypeParse(TransactionType);

impl Into<TransactionType> for TransactionTypeParse {
    fn into(self) -> TransactionType {
        self.0
    }
}

impl FromStr for TransactionTypeParse {
    type Err = ();

    fn from_str(type_: &str) -> Result<Self, Self::Err> {
        Ok(match type_ {
            "write" => Self(TransactionType::Write),
            "read" => Self(TransactionType::Read),
            _ => unreachable!("`{type_}` is not a valid transaction type"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct AnnotationsParse(Vec<Annotation>);

impl Deref for AnnotationsParse {
    type Target = Vec<Annotation>;

    fn deref(&self) -> &Vec<Annotation> {
        &self.0
    }
}

impl Into<Vec<Annotation>> for AnnotationsParse {
    fn into(self) -> Vec<Annotation> {
        self.0
    }
}

impl FromStr for AnnotationsParse {
    type Err = ();

    fn from_str(annotations: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            annotations
                .split(';')
                .map(|annotation| match annotation.trim() {
                    "key" => Annotation::Key,
                    "unique" => Annotation::Unique,
                    _ => unreachable!(),
                })
                .collect(),
        ))
    }
}
