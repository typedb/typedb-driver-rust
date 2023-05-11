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

use std::time::Duration;

use itertools::Itertools;
use typedb_protocol::{
    attribute_type, concept_manager, database, database_manager, entity_type, query_manager, r#type, relation_type,
    server_manager, session, thing_type, transaction, AttributeType as AttributeTypeProto,
    EntityType as EntityTypeProto, RelationType as RelationTypeProto,
};

use super::{FromProto, IntoProto, TryFromProto, TryIntoProto};
use crate::{
    answer::{ConceptMap, Numeric},
    common::{info::DatabaseInfo, RequestID, Result},
    concept::{Attribute, AttributeType, Entity, EntityType, Relation, RelationType, ValueType},
    connection::message::{
        ConceptRequest, ConceptResponse, QueryRequest, QueryResponse, Request, Response, ThingTypeRequest,
        ThingTypeResponse, TransactionRequest, TransactionResponse,
    },
    error::{ConnectionError, InternalError},
    Annotation, SchemaException,
};

impl TryIntoProto<server_manager::all::Req> for Request {
    fn try_into_proto(self) -> Result<server_manager::all::Req> {
        match self {
            Self::ServersAll => Ok(server_manager::all::Req {}),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database_manager::contains::Req> for Request {
    fn try_into_proto(self) -> Result<database_manager::contains::Req> {
        match self {
            Self::DatabasesContains { database_name } => Ok(database_manager::contains::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database_manager::create::Req> for Request {
    fn try_into_proto(self) -> Result<database_manager::create::Req> {
        match self {
            Self::DatabaseCreate { database_name } => Ok(database_manager::create::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database_manager::get::Req> for Request {
    fn try_into_proto(self) -> Result<database_manager::get::Req> {
        match self {
            Self::DatabaseGet { database_name } => Ok(database_manager::get::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database_manager::all::Req> for Request {
    fn try_into_proto(self) -> Result<database_manager::all::Req> {
        match self {
            Self::DatabasesAll => Ok(database_manager::all::Req {}),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database::delete::Req> for Request {
    fn try_into_proto(self) -> Result<database::delete::Req> {
        match self {
            Self::DatabaseDelete { database_name } => Ok(database::delete::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database::schema::Req> for Request {
    fn try_into_proto(self) -> Result<database::schema::Req> {
        match self {
            Self::DatabaseSchema { database_name } => Ok(database::schema::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database::type_schema::Req> for Request {
    fn try_into_proto(self) -> Result<database::type_schema::Req> {
        match self {
            Self::DatabaseTypeSchema { database_name } => Ok(database::type_schema::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<database::rule_schema::Req> for Request {
    fn try_into_proto(self) -> Result<database::rule_schema::Req> {
        match self {
            Self::DatabaseRuleSchema { database_name } => Ok(database::rule_schema::Req { name: database_name }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<session::open::Req> for Request {
    fn try_into_proto(self) -> Result<session::open::Req> {
        match self {
            Self::SessionOpen { database_name, session_type, options } => Ok(session::open::Req {
                database: database_name,
                r#type: session_type.into_proto(),
                options: Some(options.into_proto()),
            }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<session::pulse::Req> for Request {
    fn try_into_proto(self) -> Result<session::pulse::Req> {
        match self {
            Self::SessionPulse { session_id } => Ok(session::pulse::Req { session_id: session_id.into() }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<session::close::Req> for Request {
    fn try_into_proto(self) -> Result<session::close::Req> {
        match self {
            Self::SessionClose { session_id } => Ok(session::close::Req { session_id: session_id.into() }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryIntoProto<transaction::Client> for Request {
    fn try_into_proto(self) -> Result<transaction::Client> {
        match self {
            Self::Transaction(transaction_req) => Ok(transaction::Client { reqs: vec![transaction_req.into_proto()] }),
            other => Err(InternalError::UnexpectedRequestType(format!("{other:?}")).into()),
        }
    }
}

impl TryFromProto<server_manager::all::Res> for Response {
    fn try_from_proto(proto: server_manager::all::Res) -> Result<Self> {
        let servers = proto.servers.into_iter().map(|server| server.address.parse()).try_collect()?;
        Ok(Self::ServersAll { servers })
    }
}

impl FromProto<database_manager::contains::Res> for Response {
    fn from_proto(proto: database_manager::contains::Res) -> Self {
        Self::DatabasesContains { contains: proto.contains }
    }
}

impl FromProto<database_manager::create::Res> for Response {
    fn from_proto(_proto: database_manager::create::Res) -> Self {
        Self::DatabaseCreate
    }
}

impl TryFromProto<database_manager::get::Res> for Response {
    fn try_from_proto(proto: database_manager::get::Res) -> Result<Self> {
        Ok(Self::DatabaseGet {
            database: DatabaseInfo::try_from_proto(
                proto.database.ok_or(ConnectionError::MissingResponseField("database"))?,
            )?,
        })
    }
}

impl TryFromProto<database_manager::all::Res> for Response {
    fn try_from_proto(proto: database_manager::all::Res) -> Result<Self> {
        Ok(Self::DatabasesAll {
            databases: proto.databases.into_iter().map(DatabaseInfo::try_from_proto).try_collect()?,
        })
    }
}

impl FromProto<database::delete::Res> for Response {
    fn from_proto(_proto: database::delete::Res) -> Self {
        Self::DatabaseDelete
    }
}

impl FromProto<database::schema::Res> for Response {
    fn from_proto(proto: database::schema::Res) -> Self {
        Self::DatabaseSchema { schema: proto.schema }
    }
}

impl FromProto<database::type_schema::Res> for Response {
    fn from_proto(proto: database::type_schema::Res) -> Self {
        Self::DatabaseTypeSchema { schema: proto.schema }
    }
}

impl FromProto<database::rule_schema::Res> for Response {
    fn from_proto(proto: database::rule_schema::Res) -> Self {
        Self::DatabaseRuleSchema { schema: proto.schema }
    }
}

impl FromProto<session::open::Res> for Response {
    fn from_proto(proto: session::open::Res) -> Self {
        Self::SessionOpen {
            session_id: proto.session_id.into(),
            server_duration: Duration::from_millis(proto.server_duration_millis as u64),
        }
    }
}

impl FromProto<session::pulse::Res> for Response {
    fn from_proto(_proto: session::pulse::Res) -> Self {
        Self::SessionPulse
    }
}

impl FromProto<session::close::Res> for Response {
    fn from_proto(_proto: session::close::Res) -> Self {
        Self::SessionClose
    }
}

impl IntoProto<transaction::Req> for TransactionRequest {
    fn into_proto(self) -> transaction::Req {
        let mut request_id = None;

        let req = match self {
            Self::Open { session_id, transaction_type, options, network_latency } => {
                transaction::req::Req::OpenReq(transaction::open::Req {
                    session_id: session_id.into(),
                    r#type: transaction_type.into_proto(),
                    options: Some(options.into_proto()),
                    network_latency_millis: network_latency.as_millis() as i32,
                })
            }
            Self::Commit => transaction::req::Req::CommitReq(transaction::commit::Req {}),
            Self::Rollback => transaction::req::Req::RollbackReq(transaction::rollback::Req {}),
            Self::Query(query_request) => transaction::req::Req::QueryManagerReq(query_request.into_proto()),
            Self::Concept(concept_request) => transaction::req::Req::ConceptManagerReq(concept_request.into_proto()),
            Self::ThingType(thing_type_request) => transaction::req::Req::TypeReq(thing_type_request.into_proto()),
            Self::Stream { request_id: req_id } => {
                request_id = Some(req_id);
                transaction::req::Req::StreamReq(transaction::stream::Req {})
            }
        };

        transaction::Req {
            req_id: request_id.unwrap_or_else(RequestID::generate).into(),
            metadata: Default::default(),
            req: Some(req),
        }
    }
}

impl TryFromProto<transaction::Res> for TransactionResponse {
    fn try_from_proto(proto: transaction::Res) -> Result<Self> {
        match proto.res {
            Some(transaction::res::Res::OpenRes(_)) => Ok(Self::Open),
            Some(transaction::res::Res::CommitRes(_)) => Ok(Self::Commit),
            Some(transaction::res::Res::RollbackRes(_)) => Ok(Self::Rollback),
            Some(transaction::res::Res::QueryManagerRes(res)) => Ok(Self::Query(QueryResponse::try_from_proto(res)?)),
            Some(transaction::res::Res::ConceptManagerRes(res)) => {
                Ok(Self::Concept(ConceptResponse::try_from_proto(res)?))
            }
            Some(transaction::res::Res::TypeRes(r#type::Res { res: Some(r#type::res::Res::ThingTypeRes(res)) })) => {
                Ok(Self::ThingType(ThingTypeResponse::try_from_proto(res)?))
            }
            Some(_) => todo!(),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}

impl TryFromProto<transaction::ResPart> for TransactionResponse {
    fn try_from_proto(proto: transaction::ResPart) -> Result<Self> {
        match proto.res {
            Some(transaction::res_part::Res::QueryManagerResPart(res_part)) => {
                Ok(Self::Query(QueryResponse::try_from_proto(res_part)?))
            }
            Some(transaction::res_part::Res::TypeResPart(r#type::ResPart {
                res: Some(r#type::res_part::Res::ThingTypeResPart(res)),
            })) => Ok(Self::ThingType(ThingTypeResponse::try_from_proto(res)?)),
            Some(_) => todo!(),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}

impl IntoProto<query_manager::Req> for QueryRequest {
    fn into_proto(self) -> query_manager::Req {
        let (req, options) = match self {
            Self::Define { query, options } => {
                (query_manager::req::Req::DefineReq(query_manager::define::Req { query }), options)
            }
            Self::Undefine { query, options } => {
                (query_manager::req::Req::UndefineReq(query_manager::undefine::Req { query }), options)
            }
            Self::Delete { query, options } => {
                (query_manager::req::Req::DeleteReq(query_manager::delete::Req { query }), options)
            }

            Self::Match { query, options } => {
                (query_manager::req::Req::MatchReq(query_manager::r#match::Req { query }), options)
            }
            Self::Insert { query, options } => {
                (query_manager::req::Req::InsertReq(query_manager::insert::Req { query }), options)
            }
            Self::Update { query, options } => {
                (query_manager::req::Req::UpdateReq(query_manager::update::Req { query }), options)
            }

            Self::MatchAggregate { query, options } => {
                (query_manager::req::Req::MatchAggregateReq(query_manager::match_aggregate::Req { query }), options)
            }

            _ => todo!(),
        };
        query_manager::Req { req: Some(req), options: Some(options.into_proto()) }
    }
}

impl TryFromProto<query_manager::Res> for QueryResponse {
    fn try_from_proto(proto: query_manager::Res) -> Result<Self> {
        match proto.res {
            Some(query_manager::res::Res::DefineRes(_)) => Ok(Self::Define),
            Some(query_manager::res::Res::UndefineRes(_)) => Ok(Self::Undefine),
            Some(query_manager::res::Res::DeleteRes(_)) => Ok(Self::Delete),
            Some(query_manager::res::Res::MatchAggregateRes(res)) => Ok(Self::MatchAggregate {
                answer: Numeric::try_from_proto(res.answer.ok_or(ConnectionError::MissingResponseField("answer"))?)?,
            }),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}

impl TryFromProto<query_manager::ResPart> for QueryResponse {
    fn try_from_proto(proto: query_manager::ResPart) -> Result<Self> {
        match proto.res {
            Some(query_manager::res_part::Res::MatchResPart(res)) => {
                Ok(Self::Match { answers: res.answers.into_iter().map(ConceptMap::try_from_proto).try_collect()? })
            }
            Some(query_manager::res_part::Res::InsertResPart(res)) => {
                Ok(Self::Insert { answers: res.answers.into_iter().map(ConceptMap::try_from_proto).try_collect()? })
            }
            Some(query_manager::res_part::Res::UpdateResPart(res)) => {
                Ok(Self::Update { answers: res.answers.into_iter().map(ConceptMap::try_from_proto).try_collect()? })
            }
            Some(_) => todo!(),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}

impl IntoProto<concept_manager::Req> for ConceptRequest {
    fn into_proto(self) -> concept_manager::Req {
        let req = match self {
            Self::GetEntityType { label } => {
                concept_manager::req::Req::GetEntityTypeReq(concept_manager::get_entity_type::Req { label })
            }
            Self::GetRelationType { label } => {
                concept_manager::req::Req::GetRelationTypeReq(concept_manager::get_relation_type::Req { label })
            }
            Self::GetAttributeType { label } => {
                concept_manager::req::Req::GetAttributeTypeReq(concept_manager::get_attribute_type::Req { label })
            }
            Self::PutEntityType { label } => {
                concept_manager::req::Req::PutEntityTypeReq(concept_manager::put_entity_type::Req { label })
            }
            Self::PutRelationType { label } => {
                concept_manager::req::Req::PutRelationTypeReq(concept_manager::put_relation_type::Req { label })
            }
            Self::PutAttributeType { label, value_type } => {
                concept_manager::req::Req::PutAttributeTypeReq(concept_manager::put_attribute_type::Req {
                    label,
                    value_type: value_type.into_proto(),
                })
            }
            Self::GetEntity { iid } => {
                concept_manager::req::Req::GetEntityReq(concept_manager::get_entity::Req { iid: iid.into() })
            }
            Self::GetRelation { iid } => {
                concept_manager::req::Req::GetRelationReq(concept_manager::get_relation::Req { iid: iid.into() })
            }
            Self::GetAttribute { iid } => {
                concept_manager::req::Req::GetAttributeReq(concept_manager::get_attribute::Req { iid: iid.into() })
            }
            Self::GetSchemaExceptions => {
                concept_manager::req::Req::GetSchemaExceptionsReq(concept_manager::get_schema_exceptions::Req {})
            }
        };
        concept_manager::Req { req: Some(req) }
    }
}

impl TryFromProto<concept_manager::Res> for ConceptResponse {
    fn try_from_proto(proto: concept_manager::Res) -> Result<Self> {
        match proto.res {
            Some(concept_manager::res::Res::GetEntityTypeRes(concept_manager::get_entity_type::Res {
                entity_type,
            })) => Ok(Self::GetEntityType { entity_type: entity_type.map(EntityType::from_proto) }),
            Some(concept_manager::res::Res::GetRelationTypeRes(concept_manager::get_relation_type::Res {
                relation_type,
            })) => Ok(Self::GetRelationType { relation_type: relation_type.map(RelationType::from_proto) }),
            Some(concept_manager::res::Res::GetAttributeTypeRes(concept_manager::get_attribute_type::Res {
                attribute_type,
            })) => Ok(Self::GetAttributeType {
                attribute_type: attribute_type.map(AttributeType::try_from_proto).transpose()?,
            }),
            Some(concept_manager::res::Res::PutEntityTypeRes(concept_manager::put_entity_type::Res {
                entity_type,
            })) => Ok(Self::PutEntityType {
                entity_type: EntityType::from_proto(
                    entity_type.ok_or(ConnectionError::MissingResponseField("entity_type"))?,
                ),
            }),
            Some(concept_manager::res::Res::PutRelationTypeRes(concept_manager::put_relation_type::Res {
                relation_type,
            })) => Ok(Self::PutRelationType {
                relation_type: RelationType::from_proto(
                    relation_type.ok_or(ConnectionError::MissingResponseField("relation_type"))?,
                ),
            }),
            Some(concept_manager::res::Res::PutAttributeTypeRes(concept_manager::put_attribute_type::Res {
                attribute_type,
            })) => Ok(Self::PutAttributeType {
                attribute_type: AttributeType::try_from_proto(
                    attribute_type.ok_or(ConnectionError::MissingResponseField("attribute_type"))?,
                )?,
            }),
            Some(concept_manager::res::Res::GetEntityRes(concept_manager::get_entity::Res { entity })) => {
                Ok(Self::GetEntity { entity: entity.map(Entity::try_from_proto).transpose()? })
            }
            Some(concept_manager::res::Res::GetRelationRes(concept_manager::get_relation::Res { relation })) => {
                Ok(Self::GetRelation { relation: relation.map(Relation::try_from_proto).transpose()? })
            }
            Some(concept_manager::res::Res::GetAttributeRes(concept_manager::get_attribute::Res { attribute })) => {
                Ok(Self::GetAttribute { attribute: attribute.map(Attribute::try_from_proto).transpose()? })
            }
            Some(concept_manager::res::Res::GetSchemaExceptionsRes(concept_manager::get_schema_exceptions::Res {
                exceptions,
            })) => Ok(Self::GetSchemaExceptions {
                exceptions: exceptions.into_iter().map(SchemaException::from_proto).collect(),
            }),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}

impl IntoProto<r#type::Req> for ThingTypeRequest {
    fn into_proto(self) -> r#type::Req {
        let (req, label) = match self {
            Self::ThingTypeDelete { label } => {
                (thing_type::req::Req::ThingTypeDeleteReq(thing_type::delete::Req {}), label)
            }
            Self::ThingTypeSetLabel { old_label, new_label } => {
                (thing_type::req::Req::ThingTypeSetLabelReq(thing_type::set_label::Req { label: new_label }), old_label)
            }
            Self::ThingTypeSetAbstract { label } => {
                (thing_type::req::Req::ThingTypeSetAbstractReq(thing_type::set_abstract::Req {}), label)
            }
            Self::ThingTypeUnsetAbstract { label } => {
                (thing_type::req::Req::ThingTypeUnsetAbstractReq(thing_type::unset_abstract::Req {}), label)
            }
            Self::ThingTypeGetOwns { label, value_type, transitivity, annotation_filter } => (
                thing_type::req::Req::ThingTypeGetOwnsReq(thing_type::get_owns::Req {
                    filter: value_type.map(IntoProto::into_proto),
                    transitivity: transitivity.into_proto(),
                    annotations: annotation_filter.into_iter().map(Annotation::into_proto).collect(),
                }),
                label,
            ),
            Self::ThingTypeGetOwnsOverridden { label, overridden_attribute_label } => (
                thing_type::req::Req::ThingTypeGetOwnsOverriddenReq(thing_type::get_owns_overridden::Req {
                    attribute_type: Some(AttributeTypeProto {
                        label: overridden_attribute_label,
                        ..Default::default()
                    }),
                }),
                label,
            ),
            Self::ThingTypeSetOwns { label, attribute_label, overridden_attribute_label, annotations } => (
                thing_type::req::Req::ThingTypeSetOwnsReq(thing_type::set_owns::Req {
                    attribute_type: Some(AttributeTypeProto { label: attribute_label, ..Default::default() }),
                    overridden_type: overridden_attribute_label
                        .map(|label| AttributeTypeProto { label, ..Default::default() }),
                    annotations: annotations.into_iter().map(|anno| anno.into_proto()).collect(),
                }),
                label,
            ),
            Self::ThingTypeUnsetOwns { label, attribute_label } => (
                thing_type::req::Req::ThingTypeUnsetOwnsReq(thing_type::unset_owns::Req {
                    attribute_type: Some(AttributeTypeProto { label: attribute_label, ..Default::default() }),
                }),
                label,
            ),
            Self::EntityTypeCreate { label } => {
                (thing_type::req::Req::EntityTypeCreateReq(entity_type::create::Req {}), label)
            }
            Self::EntityTypeGetSupertype { label } => {
                (thing_type::req::Req::EntityTypeGetSupertypeReq(entity_type::get_supertype::Req {}), label)
            }
            Self::EntityTypeSetSupertype { label, supertype_label } => (
                thing_type::req::Req::EntityTypeSetSupertypeReq(entity_type::set_supertype::Req {
                    entity_type: Some(EntityTypeProto { label: supertype_label, ..Default::default() }),
                }),
                label,
            ),
            Self::EntityTypeGetSupertypes { label } => {
                (thing_type::req::Req::EntityTypeGetSupertypesReq(entity_type::get_supertypes::Req {}), label)
            }
            Self::EntityTypeGetSubtypes { label, transitivity } => (
                thing_type::req::Req::EntityTypeGetSubtypesReq(entity_type::get_subtypes::Req {
                    transitivity: transitivity.into_proto(),
                }),
                label,
            ),
            Self::EntityTypeGetInstances { label, transitivity } => (
                thing_type::req::Req::EntityTypeGetInstancesReq(entity_type::get_instances::Req {
                    transitivity: transitivity.into_proto(),
                }),
                label,
            ),
            Self::RelationTypeCreate { label } => {
                (thing_type::req::Req::RelationTypeCreateReq(relation_type::create::Req {}), label)
            }
            Self::RelationTypeGetSupertype { label } => {
                (thing_type::req::Req::RelationTypeGetSupertypeReq(relation_type::get_supertype::Req {}), label)
            }
            Self::RelationTypeSetSupertype { label, supertype_label } => (
                thing_type::req::Req::RelationTypeSetSupertypeReq(relation_type::set_supertype::Req {
                    relation_type: Some(RelationTypeProto { label: supertype_label, ..Default::default() }),
                }),
                label,
            ),
            Self::RelationTypeGetSupertypes { label } => {
                (thing_type::req::Req::RelationTypeGetSupertypesReq(relation_type::get_supertypes::Req {}), label)
            }
            Self::RelationTypeGetSubtypes { label, transitivity } => (
                thing_type::req::Req::RelationTypeGetSubtypesReq(relation_type::get_subtypes::Req {
                    transitivity: transitivity.into_proto(),
                }),
                label,
            ),
            Self::RelationTypeGetInstances { label, transitivity } => (
                thing_type::req::Req::RelationTypeGetInstancesReq(relation_type::get_instances::Req {
                    transitivity: transitivity.into_proto(),
                }),
                label,
            ),
            Self::AttributeTypePut { label, value } => (
                thing_type::req::Req::AttributeTypePutReq(attribute_type::put::Req { value: Some(value.into_proto()) }),
                label,
            ),
            Self::AttributeTypeGet { label, value } => (
                thing_type::req::Req::AttributeTypeGetReq(attribute_type::get::Req { value: Some(value.into_proto()) }),
                label,
            ),
            Self::AttributeTypeGetSupertype { label } => {
                (thing_type::req::Req::AttributeTypeGetSupertypeReq(attribute_type::get_supertype::Req {}), label)
            }
            Self::AttributeTypeSetSupertype { label, supertype_label } => (
                thing_type::req::Req::AttributeTypeSetSupertypeReq(attribute_type::set_supertype::Req {
                    attribute_type: Some(AttributeTypeProto { label: supertype_label, ..Default::default() }),
                }),
                label,
            ),
            Self::AttributeTypeGetSupertypes { label } => {
                (thing_type::req::Req::AttributeTypeGetSupertypesReq(attribute_type::get_supertypes::Req {}), label)
            }
            Self::AttributeTypeGetSubtypes { label, transitivity, value_type } => (
                thing_type::req::Req::AttributeTypeGetSubtypesReq(attribute_type::get_subtypes::Req {
                    transitivity: transitivity.into_proto(),
                    value_type: value_type.map(ValueType::into_proto),
                }),
                label,
            ),
            Self::AttributeTypeGetInstances { label, transitivity, value_type } => (
                thing_type::req::Req::AttributeTypeGetInstancesReq(attribute_type::get_instances::Req {
                    transitivity: transitivity.into_proto(),
                    value_type: value_type.map(ValueType::into_proto),
                }),
                label,
            ),
        };
        r#type::Req { req: Some(r#type::req::Req::ThingTypeReq(thing_type::Req { label, req: Some(req) })) }
    }
}

impl TryFromProto<thing_type::Res> for ThingTypeResponse {
    fn try_from_proto(proto: thing_type::Res) -> Result<Self> {
        match proto.res {
            Some(thing_type::res::Res::ThingTypeDeleteRes(_)) => Ok(Self::ThingTypeDelete),
            Some(thing_type::res::Res::ThingTypeSetLabelRes(_)) => Ok(Self::ThingTypeSetLabel),
            Some(thing_type::res::Res::ThingTypeSetAbstractRes(_)) => Ok(Self::ThingTypeSetAbstract),
            Some(thing_type::res::Res::ThingTypeUnsetAbstractRes(_)) => Ok(Self::ThingTypeUnsetAbstract),
            Some(thing_type::res::Res::ThingTypeGetOwnsOverriddenRes(thing_type::get_owns_overridden::Res {
                attribute_type,
            })) => Ok(Self::ThingTypeGetOwnsOverridden {
                attribute_type: attribute_type.map(AttributeType::try_from_proto).transpose()?,
            }),
            Some(thing_type::res::Res::ThingTypeSetOwnsRes(_)) => Ok(Self::ThingTypeSetOwns),
            Some(thing_type::res::Res::ThingTypeUnsetOwnsRes(_)) => Ok(Self::ThingTypeUnsetOwns),
            Some(thing_type::res::Res::EntityTypeCreateRes(entity_type::create::Res { entity })) => {
                Ok(Self::EntityTypeCreate {
                    entity: Entity::try_from_proto(entity.ok_or(ConnectionError::MissingResponseField("entity"))?)?,
                })
            }
            Some(thing_type::res::Res::EntityTypeGetSupertypeRes(entity_type::get_supertype::Res { entity_type })) => {
                Ok(Self::EntityTypeGetSupertype {
                    supertype: EntityType::from_proto(
                        entity_type.ok_or(ConnectionError::MissingResponseField("entity_type"))?,
                    ),
                })
            }
            Some(thing_type::res::Res::EntityTypeSetSupertypeRes(_)) => Ok(Self::EntityTypeSetSupertype),
            Some(thing_type::res::Res::RelationTypeCreateRes(relation_type::create::Res { relation })) => {
                Ok(Self::RelationTypeCreate {
                    relation: Relation::try_from_proto(
                        relation.ok_or(ConnectionError::MissingResponseField("relation"))?,
                    )?,
                })
            }
            Some(thing_type::res::Res::RelationTypeGetSupertypeRes(relation_type::get_supertype::Res {
                relation_type,
            })) => Ok(Self::RelationTypeGetSupertype {
                supertype: RelationType::from_proto(
                    relation_type.ok_or(ConnectionError::MissingResponseField("relation_type"))?,
                ),
            }),
            Some(thing_type::res::Res::RelationTypeSetSupertypeRes(_)) => Ok(Self::RelationTypeSetSupertype),
            Some(thing_type::res::Res::AttributeTypePutRes(attribute_type::put::Res { attribute })) => {
                Ok(Self::AttributeTypePut {
                    attribute: Attribute::try_from_proto(
                        attribute.ok_or(ConnectionError::MissingResponseField("attribute"))?,
                    )?,
                })
            }
            Some(thing_type::res::Res::AttributeTypeGetRes(attribute_type::get::Res { attribute })) => {
                Ok(Self::AttributeTypeGet { attribute: attribute.map(Attribute::try_from_proto).transpose()? })
            }
            Some(thing_type::res::Res::AttributeTypeGetSupertypeRes(attribute_type::get_supertype::Res {
                attribute_type,
            })) => Ok(Self::AttributeTypeGetSupertype {
                supertype: AttributeType::try_from_proto(
                    attribute_type.ok_or(ConnectionError::MissingResponseField("attribute_type"))?,
                )?,
            }),
            Some(thing_type::res::Res::AttributeTypeSetSupertypeRes(_)) => Ok(Self::AttributeTypeSetSupertype),
            Some(_) => todo!(),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}

impl TryFromProto<thing_type::ResPart> for ThingTypeResponse {
    fn try_from_proto(proto: thing_type::ResPart) -> Result<Self> {
        match proto.res {
            Some(thing_type::res_part::Res::ThingTypeGetOwnsResPart(thing_type::get_owns::ResPart {
                attribute_types,
            })) => Ok(Self::ThingTypeGetOwns {
                attribute_types: attribute_types.into_iter().map(AttributeType::try_from_proto).try_collect()?,
            }),
            Some(thing_type::res_part::Res::EntityTypeGetSupertypesResPart(entity_type::get_supertypes::ResPart {
                entity_types,
            })) => Ok(Self::EntityTypeGetSupertypes {
                supertypes: entity_types.into_iter().map(EntityType::from_proto).collect(),
            }),
            Some(thing_type::res_part::Res::EntityTypeGetSubtypesResPart(entity_type::get_subtypes::ResPart {
                entity_types,
            })) => Ok(Self::EntityTypeGetSubtypes {
                subtypes: entity_types.into_iter().map(EntityType::from_proto).collect(),
            }),
            Some(thing_type::res_part::Res::EntityTypeGetInstancesResPart(entity_type::get_instances::ResPart {
                entities,
            })) => Ok(Self::EntityTypeGetInstances {
                entities: entities.into_iter().map(Entity::try_from_proto).try_collect()?,
            }),
            Some(thing_type::res_part::Res::RelationTypeGetSupertypesResPart(
                relation_type::get_supertypes::ResPart { relation_types },
            )) => Ok(Self::RelationTypeGetSupertypes {
                supertypes: relation_types.into_iter().map(RelationType::from_proto).collect(),
            }),
            Some(thing_type::res_part::Res::RelationTypeGetSubtypesResPart(relation_type::get_subtypes::ResPart {
                relation_types,
            })) => Ok(Self::RelationTypeGetSubtypes {
                subtypes: relation_types.into_iter().map(RelationType::from_proto).collect(),
            }),
            Some(thing_type::res_part::Res::RelationTypeGetInstancesResPart(
                relation_type::get_instances::ResPart { relations },
            )) => Ok(Self::RelationTypeGetInstances {
                relations: relations.into_iter().map(Relation::try_from_proto).try_collect()?,
            }),
            Some(thing_type::res_part::Res::AttributeTypeGetSupertypesResPart(
                attribute_type::get_supertypes::ResPart { attribute_types },
            )) => Ok(Self::AttributeTypeGetSupertypes {
                supertypes: attribute_types.into_iter().map(AttributeType::try_from_proto).try_collect()?,
            }),
            Some(thing_type::res_part::Res::AttributeTypeGetSubtypesResPart(
                attribute_type::get_subtypes::ResPart { attribute_types },
            )) => Ok(Self::AttributeTypeGetSubtypes {
                subtypes: attribute_types.into_iter().map(AttributeType::try_from_proto).try_collect()?,
            }),
            Some(thing_type::res_part::Res::AttributeTypeGetInstancesResPart(
                attribute_type::get_instances::ResPart { attributes },
            )) => Ok(Self::AttributeTypeGetInstances {
                attributes: attributes.into_iter().map(Attribute::try_from_proto).try_collect()?,
            }),
            Some(_) => todo!(),
            None => Err(ConnectionError::MissingResponseField("res").into()),
        }
    }
}
