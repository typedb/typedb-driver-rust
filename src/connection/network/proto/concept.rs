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
    numeric::Value as NumericValue, Attribute as AttributeProto, AttributeType as AttributeTypeProto,
    Concept as ConceptProto, ConceptMap as ConceptMapProto, Entity as EntityProto, EntityType as EntityTypeProto,
    Numeric as NumericProto, Relation as RelationProto, RelationType as RelationTypeProto, RoleType as RoleTypeProto,
};

use super::TryFromProto;
use crate::{
    answer::{ConceptMap, Numeric},
    concept::{
        Attribute, AttributeType, Concept, Entity, EntityType, Relation, RelationType, RoleType, ScopedLabel, Value,
        ValueType,
    },
    connection::network::proto::FromProto,
    error::{ConnectionError, InternalError},
    Result,
};

impl TryFromProto<NumericProto> for Numeric {
    fn try_from_proto(proto: NumericProto) -> Result<Self> {
        match proto.value {
            Some(NumericValue::LongValue(long)) => Ok(Self::Long(long)),
            Some(NumericValue::DoubleValue(double)) => Ok(Self::Double(double)),
            Some(NumericValue::Nan(_)) => Ok(Self::NaN),
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
            concept_proto::Concept::EntityType(entity_type_proto) => {
                Ok(Self::EntityType(EntityType::from_proto(entity_type_proto)))
            }
            concept_proto::Concept::RelationType(relation_type_proto) => {
                Ok(Self::RelationType(RelationType::from_proto(relation_type_proto)))
            }
            concept_proto::Concept::AttributeType(attribute_type_proto) => {
                AttributeType::try_from_proto(attribute_type_proto).map(Concept::AttributeType)
            }

            concept_proto::Concept::RoleType(role_type_proto) => {
                Ok(Self::RoleType(RoleType::from_proto(role_type_proto)))
            }

            concept_proto::Concept::Entity(entity_proto) => Entity::try_from_proto(entity_proto).map(Concept::Entity),
            concept_proto::Concept::Relation(relation_proto) => {
                Relation::try_from_proto(relation_proto).map(Concept::Relation)
            }
            concept_proto::Concept::Attribute(attribute_proto) => {
                Attribute::try_from_proto(attribute_proto).map(Concept::Attribute)
            }

            concept_proto::Concept::RootThingType(_root_thing_type_proto) => todo!(),
        }
    }
}

impl FromProto<EntityTypeProto> for EntityType {
    fn from_proto(proto: EntityTypeProto) -> Self {
        let EntityTypeProto { label, is_root, is_abstract } = proto;
        Self::new(label, is_root, is_abstract)
    }
}

impl FromProto<RelationTypeProto> for RelationType {
    fn from_proto(proto: RelationTypeProto) -> Self {
        let RelationTypeProto { label, is_root: _, is_abstract: _ } = proto;
        Self::new(label)
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

impl TryFromProto<AttributeTypeProto> for AttributeType {
    fn try_from_proto(proto: AttributeTypeProto) -> Result<Self> {
        Ok(Self::new(proto.label, ValueType::try_from_proto(proto.value_type)?))
    }
}

impl FromProto<RoleTypeProto> for RoleType {
    fn from_proto(proto: RoleTypeProto) -> Self {
        let RoleTypeProto { label, is_root: _, is_abstract: _, scope } = proto;
        Self::new(ScopedLabel::new(scope, label))
    }
}

impl TryFromProto<EntityProto> for Entity {
    fn try_from_proto(proto: EntityProto) -> Result<Self> {
        Ok(Self::new(
            proto.iid.into(),
            EntityType::from_proto(proto.entity_type.ok_or(ConnectionError::MissingResponseField("type"))?),
        ))
    }
}

impl TryFromProto<RelationProto> for Relation {
    fn try_from_proto(proto: RelationProto) -> Result<Self> {
        Ok(Self::new(
            proto.iid.into(),
            RelationType::from_proto(proto.relation_type.ok_or(ConnectionError::MissingResponseField("type"))?),
        ))
    }
}

impl TryFromProto<AttributeProto> for Attribute {
    fn try_from_proto(proto: AttributeProto) -> Result<Self> {
        Ok(Self::new(
            proto.iid.into(),
            AttributeType::try_from_proto(proto.attribute_type.ok_or(ConnectionError::MissingResponseField("type"))?)?,
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
