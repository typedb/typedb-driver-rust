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
    concept::{ScopedLabel, Value, ValueType},
    Annotation, TransactionType, Transitivity,
};

#[derive(Debug, Parameter)]
#[param(name = "maybe_contain", regex = r"(?:do not )?contain")]
pub struct ContainmentParse(bool);

impl ContainmentParse {
    pub fn assert<T, U>(&self, actuals: &[T], item: U)
    where
        T: Contains<U> + fmt::Debug,
        U: PartialEq + fmt::Debug,
    {
        if self.0 {
            assert!(actuals.iter().any(|actual| actual.test(&item)), "{item:?} not found in {actuals:?}")
        } else {
            assert!(actuals.iter().all(|actual| !actual.test(&item)), "{item:?} found in {actuals:?}")
        }
    }
}

impl FromStr for ContainmentParse {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s == "contain"))
    }
}

// FIXME meaningful names
pub trait Contains<U: ?Sized> {
    fn test(&self, item: &U) -> bool;
}

impl<T: Borrow<U>, U: PartialEq + ?Sized> Contains<&U> for T {
    fn test(&self, item: &&U) -> bool {
        self.borrow() == *item
    }
}

impl<'a, T1, T2, U1, U2> Contains<(&'a U1, &'a U2)> for (T1, T2)
where
    T1: Contains<&'a U1>,
    T2: Contains<&'a U2>,
{
    fn test(&self, (first, second): &(&'a U1, &'a U2)) -> bool {
        self.0.test(first) && self.1.test(second)
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
pub struct ValueTypeParse(ValueType);

impl From<ValueTypeParse> for ValueType {
    fn from(val: ValueTypeParse) -> Self {
        val.0
    }
}

impl FromStr for ValueTypeParse {
    type Err = Infallible;

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

#[derive(Clone, Copy, Debug, Parameter)]
#[param(name = "maybe_value_type", regex = r" as\((boolean|long|double|string|datetime)\)|()")]
pub struct AsValueTypeParse(pub Option<ValueType>);

impl FromStr for AsValueTypeParse {
    type Err = Infallible;

    fn from_str(type_: &str) -> Result<Self, Self::Err> {
        Ok(Self(type_.is_empty().not().then(|| type_.parse::<ValueTypeParse>().unwrap().0)))
    }
}

#[derive(Clone, Copy, Debug, Parameter)]
#[param(name = "maybe_explicit", regex = r" explicit|")]
pub struct TransitivityParse(Transitivity);

impl From<TransitivityParse> for Transitivity {
    fn from(val: TransitivityParse) -> Self {
        val.0
    }
}

impl FromStr for TransitivityParse {
    type Err = Infallible;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(match text {
            "" => Self(Transitivity::Transitive),
            " explicit" => Self(Transitivity::Explicit),
            _ => unreachable!("Unrecognized transitivity modifier: {text:?}"),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TransactionTypeParse(TransactionType);

impl From<TransactionTypeParse> for TransactionType {
    fn from(val: TransactionTypeParse) -> Self {
        val.0
    }
}

impl FromStr for TransactionTypeParse {
    type Err = Infallible;

    fn from_str(type_: &str) -> Result<Self, Self::Err> {
        Ok(match type_ {
            "write" => Self(TransactionType::Write),
            "read" => Self(TransactionType::Read),
            _ => unreachable!("`{type_}` is not a valid transaction type"),
        })
    }
}

#[derive(Clone, Debug, Parameter)]
#[param(name = "var", regex = r"(\$[\w_-]+)")]
pub struct VarParse {
    pub name: String,
}

impl From<VarParse> for String {
    fn from(val: VarParse) -> Self {
        let VarParse { name } = val;
        name
    }
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

impl From<LabelParse> for String {
    fn from(val: LabelParse) -> Self {
        let LabelParse { name } = val;
        name
    }
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

impl From<OverrideLabelParse> for Option<String> {
    fn from(val: OverrideLabelParse) -> Self {
        let OverrideLabelParse { name } = val;
        name
    }
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
    pub scope: String,
    pub name: String,
}

impl From<ScopedLabelParse> for ScopedLabel {
    fn from(val: ScopedLabelParse) -> Self {
        let ScopedLabelParse { scope, name } = val;
        ScopedLabel { scope, name }
    }
}

impl FromStr for ScopedLabelParse {
    type Err = Infallible;

    fn from_str(label: &str) -> Result<Self, Self::Err> {
        let Some((scope, name)) = label.split_once(':') else { unreachable!() };
        Ok(Self { scope: scope.to_owned(), name: name.to_owned() })
    }
}

#[derive(Clone, Debug, Parameter)]
#[param(name = "override_scoped_label", regex = r" as (\S+:\S+)|()")]
pub struct OverrideScopedLabelParse {
    pub scope: Option<String>,
    pub name: Option<String>,
}

impl From<OverrideScopedLabelParse> for Option<ScopedLabel> {
    fn from(val: OverrideScopedLabelParse) -> Self {
        let OverrideScopedLabelParse { scope, name } = val;
        if let (Some(scope), Some(name)) = (scope, name) {
            Some(ScopedLabel { scope, name })
        } else {
            None
        }
    }
}

impl FromStr for OverrideScopedLabelParse {
    type Err = Infallible;

    fn from_str(label: &str) -> Result<Self, Self::Err> {
        if let Some((scope, name)) = label.split_once(':') {
            Ok(Self { scope: Some(scope.to_owned()), name: Some(name.to_owned()) })
        } else {
            Ok(Self { scope: None, name: None })
        }
    }
}

#[derive(Clone, Debug, Parameter)]
#[param(name = "annotations", regex = r", with annotations: ([\w-]+(?:, (?:[\w-]+))*)|()")]
pub struct AnnotationsParse(Vec<Annotation>);

impl From<AnnotationsParse> for Vec<Annotation> {
    fn from(val: AnnotationsParse) -> Self {
        val.0
    }
}

impl FromStr for AnnotationsParse {
    type Err = Infallible;

    fn from_str(annotations: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            annotations
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
        ))
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
