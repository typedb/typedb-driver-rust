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

use std::{borrow::Borrow, convert::Infallible, fmt, ops::Not, str::FromStr};

use chrono::NaiveDateTime;
use cucumber::Parameter;
use typedb_client::{
    concept::{Annotation, ScopedLabel, Transitivity, Value, ValueType},
    TransactionType,
};

#[derive(Debug, Parameter)]
#[param(name = "maybe_contain", regex = r"(?:do not )?contain")]
pub struct ContainmentParse(bool);

impl ContainmentParse {
    pub fn assert<T, U>(&self, actuals: &[T], item: U)
    where
        T: Comparable<U> + fmt::Debug,
        U: PartialEq + fmt::Debug,
    {
        if self.0 {
            assert!(actuals.iter().any(|actual| actual.equals(&item)), "{item:?} not found in {actuals:?}")
        } else {
            assert!(actuals.iter().all(|actual| !actual.equals(&item)), "{item:?} found in {actuals:?}")
        }
    }
}

impl FromStr for ContainmentParse {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s == "contain"))
    }
}

pub trait Comparable<U: ?Sized> {
    fn equals(&self, item: &U) -> bool;
}

impl<T: Borrow<U>, U: PartialEq + ?Sized> Comparable<&U> for T {
    fn equals(&self, item: &&U) -> bool {
        self.borrow() == *item
    }
}

impl<'a, T1, T2, U1, U2> Comparable<(&'a U1, &'a U2)> for (T1, T2)
where
    T1: Comparable<&'a U1>,
    T2: Comparable<&'a U2>,
{
    fn equals(&self, (first, second): &(&'a U1, &'a U2)) -> bool {
        self.0.equals(first) && self.1.equals(second)
    }
}

#[derive(Clone, Debug, Parameter)]
#[param(name = "value", regex = r".+")]
pub struct ValueParse(String);

impl ValueParse {
    pub fn into_value(self, value_type: ValueType) -> Value {
        match value_type {
            ValueType::Boolean => Value::Boolean(self.0.parse().unwrap()),
            ValueType::Double => Value::Double(self.0.parse().unwrap()),
            ValueType::Long => Value::Long(self.0.parse().unwrap()),
            ValueType::String => Value::String(self.0),
            ValueType::DateTime => {
                Value::DateTime(NaiveDateTime::parse_from_str(&self.0, "%Y-%m-%d %H:%M:%S").unwrap())
            }
            ValueType::Object => unreachable!(),
        }
    }
}

impl FromStr for ValueParse {
    type Err = Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, Parameter)]
#[param(name = "value_type", regex = r"boolean|long|double|string|datetime")]
pub struct ValueTypeParse {
    pub value_type: ValueType,
}

impl FromStr for ValueTypeParse {
    type Err = Infallible;

    fn from_str(type_: &str) -> Result<Self, Self::Err> {
        Ok(match type_ {
            "boolean" => Self { value_type: ValueType::Boolean },
            "long" => Self { value_type: ValueType::Long },
            "double" => Self { value_type: ValueType::Double },
            "string" => Self { value_type: ValueType::String },
            "datetime" => Self { value_type: ValueType::DateTime },
            _ => unreachable!("`{type_}` is not a valid value type"),
        })
    }
}

#[derive(Clone, Copy, Debug, Parameter)]
#[param(name = "maybe_value_type", regex = r" as\((boolean|long|double|string|datetime)\)|()")]
pub struct AsValueTypeParse {
    pub value_type: Option<ValueType>,
}

impl FromStr for AsValueTypeParse {
    type Err = Infallible;

    fn from_str(type_: &str) -> Result<Self, Self::Err> {
        Ok(Self { value_type: type_.is_empty().not().then(|| type_.parse::<ValueTypeParse>().unwrap().value_type) })
    }
}

#[derive(Clone, Copy, Debug, Parameter)]
#[param(name = "maybe_explicit", regex = r" explicit|")]
pub struct TransitivityParse {
    pub transitivity: Transitivity,
}

impl FromStr for TransitivityParse {
    type Err = Infallible;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(match text {
            "" => Self { transitivity: Transitivity::Transitive },
            " explicit" => Self { transitivity: Transitivity::Explicit },
            _ => unreachable!(),
        })
    }
}

#[derive(Clone, Copy, Debug, Parameter)]
#[param(name = "transaction_type", regex = r"write|read")]
pub struct TransactionTypeParse {
    pub transaction_type: TransactionType,
}

impl FromStr for TransactionTypeParse {
    type Err = Infallible;

    fn from_str(type_: &str) -> Result<Self, Self::Err> {
        Ok(match type_ {
            "write" => Self { transaction_type: TransactionType::Write },
            "read" => Self { transaction_type: TransactionType::Read },
            _ => unreachable!("`{type_}` is not a valid transaction type"),
        })
    }
}

#[derive(Clone, Debug, Parameter)]
#[param(name = "var", regex = r"(\$[\w_-]+)")]
pub struct VarParse {
    pub name: String,
}

impl FromStr for VarParse {
    type Err = Infallible;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Ok(Self { name: name.to_owned() })
    }
}

#[derive(Clone, Debug, Parameter)]
#[param(name = "label", regex = r"[\w_-]+")]
pub struct LabelParse {
    pub name: String,
}

impl FromStr for LabelParse {
    type Err = Infallible;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Ok(Self { name: name.to_owned() })
    }
}

#[derive(Clone, Debug, Parameter)]
#[param(name = "override_label", regex = r" as ([\w-]+)|()")]
pub struct OverrideLabelParse {
    pub name: Option<String>,
}

impl FromStr for OverrideLabelParse {
    type Err = Infallible;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        if name.is_empty() {
            Ok(Self { name: None })
        } else {
            Ok(Self { name: Some(name.to_owned()) })
        }
    }
}

#[derive(Clone, Debug, Parameter)]
#[param(name = "scoped_label", regex = r"\S+:\S+")]
pub struct ScopedLabelParse {
    pub label: ScopedLabel,
}

impl FromStr for ScopedLabelParse {
    type Err = Infallible;

    fn from_str(label: &str) -> Result<Self, Self::Err> {
        let Some((scope, name)) = label.split_once(':') else { unreachable!() };
        Ok(Self { label: ScopedLabel { scope: scope.to_owned(), name: name.to_owned() } })
    }
}

#[derive(Clone, Debug, Parameter)]
#[param(name = "override_scoped_label", regex = r" as (\S+:\S+)|()")]
pub struct OverrideScopedLabelParse {
    pub label: Option<ScopedLabel>,
}

impl FromStr for OverrideScopedLabelParse {
    type Err = Infallible;

    fn from_str(label: &str) -> Result<Self, Self::Err> {
        if let Some((scope, name)) = label.split_once(':') {
            Ok(Self { label: Some(ScopedLabel { scope: scope.to_owned(), name: name.to_owned() }) })
        } else {
            Ok(Self { label: None })
        }
    }
}

#[derive(Clone, Debug, Parameter)]
#[param(name = "annotations", regex = r", with annotations: ([\w-]+(?:, (?:[\w-]+))*)|()")]
pub struct AnnotationsParse {
    pub annotations: Vec<Annotation>,
}

impl FromStr for AnnotationsParse {
    type Err = Infallible;

    fn from_str(annotations: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            annotations: annotations
                .trim()
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|annotation| match annotation {
                    "key" => Annotation::Key,
                    "unique" => Annotation::Unique,
                    _ => unreachable!("Unrecognized annotation: {annotation:?}"),
                })
                .collect(),
        })
    }
}

#[derive(Clone, Debug, Parameter)]
#[param(name = "maybe_role", regex = r" for role\(\s*(\S+)\s*\)|()")]
pub struct RoleParse {
    pub role: Option<String>,
}

impl FromStr for RoleParse {
    type Err = Infallible;

    fn from_str(role: &str) -> Result<Self, Self::Err> {
        Ok(Self { role: role.is_empty().not().then(|| role.to_owned()) })
    }
}
