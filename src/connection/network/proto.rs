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
use itertools::Itertools;
use typedb_protocol::{
    attribute::value::Value as ValueProto,
    attribute_type::ValueType,
    cluster_database::Replica as ReplicaProto,
    concept as concept_proto,
    numeric::Value,
    options::{
        ExplainOpt::Explain, InferOpt::Infer, ParallelOpt::Parallel, PrefetchOpt::Prefetch,
        PrefetchSizeOpt::PrefetchSize, ReadAnyReplicaOpt::ReadAnyReplica,
        SchemaLockAcquireTimeoutOpt::SchemaLockAcquireTimeoutMillis, SessionIdleTimeoutOpt::SessionIdleTimeoutMillis,
        TraceInferenceOpt::TraceInference, TransactionTimeoutOpt::TransactionTimeoutMillis,
    },
    r#type::Encoding,
    session, transaction, ClusterDatabase as DatabaseProto, Concept as ConceptProto, ConceptMap as ConceptMapProto,
    Numeric as NumericProto, Options as OptionsProto, Thing as ThingProto, Type as TypeProto,
};

use crate::{
    answer::{ConceptMap, Numeric},
    common::info::{DatabaseInfo, ReplicaInfo},
    concept::{
        Attribute, AttributeType, BooleanAttribute, BooleanAttributeType, Concept, DateTimeAttribute,
        DateTimeAttributeType, DoubleAttribute, DoubleAttributeType, Entity, EntityType, LongAttribute,
        LongAttributeType, Relation, RelationType, RoleType, RootAttributeType, RootThingType, ScopedLabel,
        StringAttribute, StringAttributeType, Thing, ThingType, Type,
    },
    error::{ClientError, InternalError},
    Options, Result, SessionType, TransactionType,
};

pub(super) trait IntoProto {
    type Proto;
    fn into_proto(self) -> Self::Proto;
}

impl IntoProto for SessionType {
    type Proto = session::Type;
    fn into_proto(self) -> Self::Proto {
        match self {
            SessionType::Data => session::Type::Data,
            SessionType::Schema => session::Type::Schema,
        }
    }
}

impl IntoProto for TransactionType {
    type Proto = transaction::Type;
    fn into_proto(self) -> transaction::Type {
        match self {
            TransactionType::Read => transaction::Type::Read,
            TransactionType::Write => transaction::Type::Write,
        }
    }
}

impl IntoProto for Options {
    type Proto = OptionsProto;
    fn into_proto(self) -> Self::Proto {
        OptionsProto {
            infer_opt: self.infer.map(Infer),
            trace_inference_opt: self.trace_inference.map(TraceInference),
            explain_opt: self.explain.map(Explain),
            parallel_opt: self.parallel.map(Parallel),
            prefetch_size_opt: self.prefetch_size.map(PrefetchSize),
            prefetch_opt: self.prefetch.map(Prefetch),
            session_idle_timeout_opt: self
                .session_idle_timeout
                .map(|val| SessionIdleTimeoutMillis(val.as_millis() as i32)),
            transaction_timeout_opt: self
                .transaction_timeout
                .map(|val| TransactionTimeoutMillis(val.as_millis() as i32)),
            schema_lock_acquire_timeout_opt: self
                .schema_lock_acquire_timeout
                .map(|val| SchemaLockAcquireTimeoutMillis(val.as_millis() as i32)),
            read_any_replica_opt: self.read_any_replica.map(ReadAnyReplica),
        }
    }
}

pub(super) trait TryFromProto: Sized {
    type Proto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self>;
}

impl TryFromProto for DatabaseInfo {
    type Proto = DatabaseProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        Ok(Self {
            name: proto.name,
            replicas: proto.replicas.into_iter().map(ReplicaInfo::try_from_proto).try_collect()?,
        })
    }
}

impl TryFromProto for ReplicaInfo {
    type Proto = ReplicaProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        Ok(Self {
            address: proto.address.as_str().parse()?,
            is_primary: proto.primary,
            is_preferred: proto.preferred,
            term: proto.term,
        })
    }
}

impl TryFromProto for Numeric {
    type Proto = NumericProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        match proto.value {
            Some(Value::LongValue(long)) => Ok(Numeric::Long(long)),
            Some(Value::DoubleValue(double)) => Ok(Numeric::Double(double)),
            Some(Value::Nan(_)) => Ok(Numeric::NaN),
            None => Err(ClientError::MissingResponseField("value").into()),
        }
    }
}

impl TryFromProto for ConceptMap {
    type Proto = ConceptMapProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        let mut map = HashMap::with_capacity(proto.map.len());
        for (k, v) in proto.map {
            map.insert(k, Concept::try_from_proto(v)?);
        }
        Ok(Self { map })
    }
}

impl TryFromProto for Concept {
    type Proto = ConceptProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        let concept = proto.concept.ok_or(ClientError::MissingResponseField("concept"))?;
        match concept {
            concept_proto::Concept::Thing(thing) => Ok(Self::Thing(Thing::try_from_proto(thing)?)),
            concept_proto::Concept::Type(type_) => Ok(Self::Type(Type::try_from_proto(type_)?)),
        }
    }
}

impl TryFromProto for Type {
    type Proto = TypeProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        match Encoding::from_i32(proto.encoding) {
            Some(Encoding::ThingType) => Ok(Self::Thing(ThingType::Root(RootThingType::default()))),
            Some(Encoding::EntityType) => Ok(Self::Thing(ThingType::Entity(EntityType::try_from_proto(proto)?))),
            Some(Encoding::RelationType) => Ok(Self::Thing(ThingType::Relation(RelationType::try_from_proto(proto)?))),
            Some(Encoding::AttributeType) => {
                Ok(Self::Thing(ThingType::Attribute(AttributeType::try_from_proto(proto)?)))
            }
            Some(Encoding::RoleType) => Ok(Self::Role(RoleType::try_from_proto(proto)?)),
            None => Err(InternalError::EnumOutOfBounds(proto.encoding).into()),
        }
    }
}

impl TryFromProto for EntityType {
    type Proto = TypeProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        Ok(Self::new(proto.label))
    }
}

impl TryFromProto for RelationType {
    type Proto = TypeProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        Ok(Self::new(proto.label))
    }
}

impl TryFromProto for AttributeType {
    type Proto = TypeProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        match ValueType::from_i32(proto.value_type) {
            Some(ValueType::Object) => Ok(Self::Root(RootAttributeType::default())),
            Some(ValueType::Boolean) => Ok(Self::Boolean(BooleanAttributeType { label: proto.label })),
            Some(ValueType::Long) => Ok(Self::Long(LongAttributeType { label: proto.label })),
            Some(ValueType::Double) => Ok(Self::Double(DoubleAttributeType { label: proto.label })),
            Some(ValueType::String) => Ok(Self::String(StringAttributeType { label: proto.label })),
            Some(ValueType::Datetime) => Ok(Self::DateTime(DateTimeAttributeType { label: proto.label })),
            None => Err(InternalError::EnumOutOfBounds(proto.value_type).into()),
        }
    }
}

impl TryFromProto for RoleType {
    type Proto = TypeProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        Ok(Self::new(ScopedLabel::new(proto.scope, proto.label)))
    }
}

impl TryFromProto for Thing {
    type Proto = ThingProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        let encoding = proto.r#type.clone().ok_or_else(|| ClientError::MissingResponseField("type"))?.encoding;
        match Encoding::from_i32(encoding) {
            Some(Encoding::EntityType) => Ok(Self::Entity(Entity::try_from_proto(proto)?)),
            Some(Encoding::RelationType) => Ok(Self::Relation(Relation::try_from_proto(proto)?)),
            Some(Encoding::AttributeType) => Ok(Self::Attribute(Attribute::try_from_proto(proto)?)),
            Some(_) => todo!(),
            None => Err(InternalError::EnumOutOfBounds(encoding).into()),
        }
    }
}

impl TryFromProto for Entity {
    type Proto = ThingProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        Ok(Self {
            type_: EntityType::try_from_proto(proto.r#type.ok_or_else(|| ClientError::MissingResponseField("type"))?)?,
            iid: proto.iid,
        })
    }
}

impl TryFromProto for Relation {
    type Proto = ThingProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        Ok(Self {
            type_: RelationType::try_from_proto(
                proto.r#type.ok_or_else(|| ClientError::MissingResponseField("type"))?,
            )?,
            iid: proto.iid,
        })
    }
}

impl TryFromProto for Attribute {
    type Proto = ThingProto;
    fn try_from_proto(proto: Self::Proto) -> Result<Self> {
        let value = proto.value.and_then(|v| v.value).ok_or_else(|| ClientError::MissingResponseField("value"))?;

        let value_type = proto.r#type.ok_or_else(|| ClientError::MissingResponseField("type"))?.value_type;
        let iid = proto.iid;

        match ValueType::from_i32(value_type) {
            Some(ValueType::Object) => todo!(),
            Some(ValueType::Boolean) => Ok(Self::Boolean(BooleanAttribute {
                value: if let ValueProto::Boolean(value) = value { value } else { todo!() },
                iid,
            })),
            Some(ValueType::Long) => Ok(Self::Long(LongAttribute {
                value: if let ValueProto::Long(value) = value { value } else { todo!() },
                iid,
            })),
            Some(ValueType::Double) => Ok(Self::Double(DoubleAttribute {
                value: if let ValueProto::Double(value) = value { value } else { todo!() },
                iid,
            })),
            Some(ValueType::String) => Ok(Self::String(StringAttribute {
                value: if let ValueProto::String(value) = value { value } else { todo!() },
                iid,
            })),
            Some(ValueType::Datetime) => Ok(Self::DateTime(DateTimeAttribute {
                value: if let ValueProto::DateTime(value) = value {
                    NaiveDateTime::from_timestamp_opt(value / 1000, (value % 1000) as u32 * 1_000_000).unwrap()
                } else {
                    todo!()
                },
                iid,
            })),
            None => Err(InternalError::EnumOutOfBounds(value_type).into()),
        }
    }
}
