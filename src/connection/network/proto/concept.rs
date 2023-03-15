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

use std::collections::HashMap;

use chrono::NaiveDateTime;
use typedb_protocol::{
    attribute::value::Value as ValueProto, attribute_type::ValueType as ValueTypeProto, concept as concept_proto,
    numeric::Value as NumericValue, r#type::Encoding, Concept as ConceptProto, ConceptMap as ConceptMapProto,
    Numeric as NumericProto, Thing as ThingProto, Type as TypeProto,
};

use super::TryFromProto;
use crate::{
    answer::{ConceptMap, Numeric},
    concept::{
        Attribute, AttributeType, Concept, Entity, EntityType, Relation, RelationType, RoleType, RootThingType,
        ScopedLabel, Value, ValueType,
    },
    connection::network::proto::FromProto,
    error::{ConnectionError, InternalError},
    Result,
};

impl TryFromProto<NumericProto> for Numeric {
    fn try_from_proto(proto: NumericProto) -> Result<Self> {
        match proto.value {
            Some(NumericValue::LongValue(long)) => Ok(Numeric::Long(long)),
            Some(NumericValue::DoubleValue(double)) => Ok(Numeric::Double(double)),
            Some(NumericValue::Nan(_)) => Ok(Numeric::NaN),
            None => Err(ConnectionError::MissingResponseField("value").into()),
        }
    }
}

impl TryFromProto<ConceptMapProto> for ConceptMap {
    fn try_from_proto(proto: ConceptMapProto) -> Result<Self> {
        let mut map = HashMap::with_capacity(proto.map.len());
        for (k, v) in proto.map {
            map.insert(k, Concept::try_from_proto(v)?);
        }
        Ok(Self { map })
    }
}

impl TryFromProto<ConceptProto> for Concept {
    fn try_from_proto(proto: ConceptProto) -> Result<Self> {
        let concept = proto.concept.ok_or(ConnectionError::MissingResponseField("concept"))?;
        match concept {
            concept_proto::Concept::Thing(thing) => {
                let encoding = thing.r#type.clone().ok_or(ConnectionError::MissingResponseField("type"))?.encoding;
                match Encoding::try_from_proto(encoding)? {
                    Encoding::EntityType => Ok(Self::Entity(Entity::try_from_proto(thing)?)),
                    Encoding::RelationType => Ok(Self::Relation(Relation::try_from_proto(thing)?)),
                    Encoding::AttributeType => Ok(Self::Attribute(Attribute::try_from_proto(thing)?)),
                    _ => todo!(),
                }
            }
            concept_proto::Concept::Type(type_) => match Encoding::try_from_proto(type_.encoding)? {
                Encoding::ThingType => Ok(Self::ThingType(RootThingType::new())),
                Encoding::EntityType => Ok(Self::EntityType(EntityType::from_proto(type_))),
                Encoding::RelationType => Ok(Self::RelationType(RelationType::from_proto(type_))),
                Encoding::AttributeType => Ok(Self::AttributeType(AttributeType::try_from_proto(type_)?)),
                Encoding::RoleType => Ok(Self::RoleType(RoleType::from_proto(type_))),
            },
        }
    }
}

impl TryFromProto<i32> for Encoding {
    fn try_from_proto(proto: i32) -> Result<Self> {
        Self::from_i32(proto).ok_or(InternalError::EnumOutOfBounds(proto, "Encoding").into())
    }
}

impl FromProto<TypeProto> for EntityType {
    fn from_proto(proto: TypeProto) -> Self {
        Self::new(proto.label)
    }
}

impl FromProto<TypeProto> for RelationType {
    fn from_proto(proto: TypeProto) -> Self {
        Self::new(proto.label)
    }
}

impl TryFromProto<i32> for ValueTypeProto {
    fn try_from_proto(proto: i32) -> Result<Self> {
        Self::from_i32(proto).ok_or(InternalError::EnumOutOfBounds(proto, "ValueType").into())
    }
}

impl TryFromProto<i32> for ValueType {
    fn try_from_proto(proto: i32) -> Result<Self> {
        match ValueTypeProto::try_from_proto(proto)? {
            ValueTypeProto::Object => Ok(Self::Object),
            ValueTypeProto::Boolean => Ok(Self::Boolean),
            ValueTypeProto::Long => Ok(Self::Long),
            ValueTypeProto::Double => Ok(Self::Double),
            ValueTypeProto::String => Ok(Self::String),
            ValueTypeProto::Datetime => Ok(Self::DateTime),
        }
    }
}

impl TryFromProto<TypeProto> for AttributeType {
    fn try_from_proto(proto: TypeProto) -> Result<Self> {
        Ok(Self::new(proto.label, ValueType::try_from_proto(proto.value_type)?))
    }
}

impl FromProto<TypeProto> for RoleType {
    fn from_proto(proto: TypeProto) -> Self {
        Self::new(ScopedLabel::new(proto.scope, proto.label))
    }
}

impl TryFromProto<ThingProto> for Entity {
    fn try_from_proto(proto: ThingProto) -> Result<Self> {
        Ok(Self::new(
            proto.iid.into(),
            EntityType::from_proto(proto.r#type.ok_or(ConnectionError::MissingResponseField("type"))?),
        ))
    }
}

impl TryFromProto<ThingProto> for Relation {
    fn try_from_proto(proto: ThingProto) -> Result<Self> {
        Ok(Self::new(
            proto.iid.into(),
            RelationType::from_proto(proto.r#type.ok_or(ConnectionError::MissingResponseField("type"))?),
        ))
    }
}

impl TryFromProto<ThingProto> for Attribute {
    fn try_from_proto(proto: ThingProto) -> Result<Self> {
        Ok(Self::new(
            proto.iid.into(),
            AttributeType::try_from_proto(proto.r#type.ok_or(ConnectionError::MissingResponseField("type"))?)?,
            Value::try_from_proto(
                proto.value.and_then(|v| v.value).ok_or(ConnectionError::MissingResponseField("value"))?,
            )?,
        ))
    }
}

impl TryFromProto<ValueProto> for Value {
    fn try_from_proto(proto: ValueProto) -> Result<Self> {
        match proto {
            ValueProto::Boolean(value) => Ok(Self::Boolean(value)),
            ValueProto::Long(value) => Ok(Self::Long(value)),
            ValueProto::Double(value) => Ok(Self::Double(value)),
            ValueProto::String(value) => Ok(Self::String(value)),
            ValueProto::DateTime(value) => Ok(Self::DateTime(
                NaiveDateTime::from_timestamp_opt(value / 1000, (value % 1000) as u32 * 1_000_000).unwrap(),
            )),
        }
    }
}
