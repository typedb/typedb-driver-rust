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

use tokio::sync::mpsc::UnboundedSender;
use tonic::Streaming;
use typedb_protocol::transaction;

use crate::{
    answer::{ConceptMap, Numeric},
    common::{address::Address, info::DatabaseInfo, RequestID, SessionID, Transitivity},
    concept::{AttributeType, Entity, EntityType, Relation, RelationType, ValueType},
    Annotation, Options, SessionType, TransactionType,
};

#[derive(Debug)]
pub(super) enum Request {
    ServersAll,

    DatabasesContains { database_name: String },
    DatabaseCreate { database_name: String },
    DatabaseGet { database_name: String },
    DatabasesAll,

    DatabaseSchema { database_name: String },
    DatabaseTypeSchema { database_name: String },
    DatabaseRuleSchema { database_name: String },
    DatabaseDelete { database_name: String },

    SessionOpen { database_name: String, session_type: SessionType, options: Options },
    SessionClose { session_id: SessionID },
    SessionPulse { session_id: SessionID },

    Transaction(TransactionRequest),
}

#[derive(Debug)]
pub(super) enum Response {
    ServersAll {
        servers: Vec<Address>,
    },

    DatabasesContains {
        contains: bool,
    },
    DatabaseCreate,
    DatabaseGet {
        database: DatabaseInfo,
    },
    DatabasesAll {
        databases: Vec<DatabaseInfo>,
    },

    DatabaseDelete,
    DatabaseSchema {
        schema: String,
    },
    DatabaseTypeSchema {
        schema: String,
    },
    DatabaseRuleSchema {
        schema: String,
    },

    SessionOpen {
        session_id: SessionID,
        server_duration: Duration,
    },
    SessionPulse,
    SessionClose,

    TransactionOpen {
        request_sink: UnboundedSender<transaction::Client>,
        response_source: Streaming<transaction::Server>,
    },
}

#[derive(Debug)]
pub(super) enum TransactionRequest {
    Open { session_id: SessionID, transaction_type: TransactionType, options: Options, network_latency: Duration },
    Commit,
    Rollback,
    Query(QueryRequest),
    Concept(ConceptRequest),
    ThingType(ThingTypeRequest),
    Stream { request_id: RequestID },
}

#[derive(Debug)]
pub(super) enum TransactionResponse {
    Open,
    Commit,
    Rollback,
    Query(QueryResponse),
    Concept(ConceptResponse),
    ThingType(ThingTypeResponse),
}

#[derive(Debug)]
pub(super) enum QueryRequest {
    Define { query: String, options: Options },
    Undefine { query: String, options: Options },
    Delete { query: String, options: Options },

    Match { query: String, options: Options },
    Insert { query: String, options: Options },
    Update { query: String, options: Options },

    MatchAggregate { query: String, options: Options },

    Explain { explainable_id: i64, options: Options }, // TODO: ID type

    MatchGroup { query: String, options: Options },
    MatchGroupAggregate { query: String, options: Options },
}

#[derive(Debug)]
pub(super) enum QueryResponse {
    Define,
    Undefine,
    Delete,

    Match { answers: Vec<ConceptMap> },
    Insert { answers: Vec<ConceptMap> },
    Update { answers: Vec<ConceptMap> },

    MatchAggregate { answer: Numeric },

    Explain {}, // TODO: explanations

    MatchGroup {},          // TODO: ConceptMapGroup
    MatchGroupAggregate {}, // TODO: NumericGroup
}

#[derive(Debug)]
pub(super) enum ConceptRequest {
    GetEntityType { label: String },
    GetRelationType { label: String },
    GetAttributeType { label: String },
    PutEntityType { label: String },
    PutRelationType { label: String },
    PutAttributeType { label: String, value_type: ValueType },
    // GetEntity,
    // GetRelation,
    // GetAttribute,
    // GetSchemaExceptions,
}

#[derive(Debug)]
pub(super) enum ConceptResponse {
    GetEntityType { entity_type: Option<EntityType> },
    GetRelationType { relation_type: Option<RelationType> },
    GetAttributeType { attribute_type: Option<AttributeType> },
    PutEntityType { entity_type: EntityType },
    PutRelationType { relation_type: RelationType },
    PutAttributeType { attribute_type: AttributeType },
    // GetEntity,
    // GetRelation,
    // GetAttribute,
    // GetSchemaExceptions,
}

#[derive(Debug)]
pub(super) enum ThingTypeRequest {
    ThingTypeDelete {
        label: String,
    },
    ThingTypeSetLabel {
        old_label: String,
        new_label: String,
    },
    ThingTypeSetAbstract {
        label: String,
    },
    ThingTypeUnsetAbstract {
        label: String,
    },
    ThingTypeGetOwns {
        label: String,
        value_type: Option<ValueType>,
        transitivity: Transitivity,
        annotation_filter: Vec<Annotation>,
    },
    ThingTypeGetOwnsOverridden {
        label: String,
        overridden_attribute_label: String,
    },
    ThingTypeSetOwns {
        label: String,
        attribute_label: String,
        overridden_attribute_label: Option<String>,
        annotations: Vec<Annotation>,
    },
    ThingTypeUnsetOwns {
        label: String,
        attribute_label: String,
    },
    // ThingTypeGetPlays,
    // ThingTypeGetPlaysOverridden,
    // ThingTypeSetPlays,
    // ThingTypeUnsetPlays,
    // ThingTypeGetSyntax,
    EntityTypeCreate {
        label: String,
    },
    EntityTypeGetSupertype {
        label: String,
    },
    EntityTypeSetSupertype {
        label: String,
        supertype_label: String,
    },
    EntityTypeGetSupertypes {
        label: String,
    },
    EntityTypeGetSubtypes {
        label: String,
        transitivity: Transitivity,
    },
    // EntityTypeGetInstances,
    RelationTypeCreate {
        label: String,
    },
    RelationTypeGetSupertype {
        label: String,
    },
    RelationTypeSetSupertype {
        label: String,
        supertype_label: String,
    },
    RelationTypeGetSupertypes {
        label: String,
    },
    RelationTypeGetSubtypes {
        label: String,
        transitivity: Transitivity,
    },
    // RelationTypeGetInstances,
    // RelationTypeGetRelates,
    // RelationTypeGetRelatesForRoleLabel,
    // RelationTypeGetRelatesOverridden,
    // RelationTypeSetRelates,
    // RelationTypeUnsetRelates,

    // AttributeTypePut,
    // AttributeTypeGet,
    // AttributeTypeGetSupertype,
    // AttributeTypeSetSupertype,
    // AttributeTypeGetSupertypes,
    // AttributeTypeGetSubtypes,
    // AttributeTypeGetInstances,
    // AttributeTypeGetRegex,
    // AttributeTypeSetRegex,
    // AttributeTypeGetOwners,
}

#[derive(Debug)]
pub(super) enum ThingTypeResponse {
    ThingTypeDelete,
    ThingTypeSetLabel,
    ThingTypeSetAbstract,
    ThingTypeUnsetAbstract,
    ThingTypeGetOwns { attribute_types: Vec<AttributeType> },
    ThingTypeGetOwnsOverridden { attribute_type: Option<AttributeType> },
    ThingTypeSetOwns,
    ThingTypeUnsetOwns,
    // ThingTypeGetPlays,
    // ThingTypeGetPlaysOverridden,
    // ThingTypeSetPlays,
    // ThingTypeUnsetPlays,
    // ThingTypeGetSyntax,
    EntityTypeCreate { entity: Entity },
    EntityTypeGetSupertype { supertype: EntityType },
    EntityTypeSetSupertype,
    EntityTypeGetSupertypes { supertypes: Vec<EntityType> },
    EntityTypeGetSubtypes { subtypes: Vec<EntityType> },
    // EntityTypeGetInstances,
    RelationTypeCreate { relation: Relation },
    RelationTypeGetSupertype { supertype: RelationType },
    RelationTypeSetSupertype,
    RelationTypeGetSupertypes { supertypes: Vec<RelationType> },
    RelationTypeGetSubtypes { subtypes: Vec<RelationType> },
    // RelationTypeGetInstances,
    // RelationTypeGetRelates,
    // RelationTypeGetRelatesForRoleLabel,
    // RelationTypeGetRelatesOverridden,
    // RelationTypeSetRelates,
    // RelationTypeUnsetRelates,

    // AttributeTypePut,
    // AttributeTypeGet,
    // AttributeTypeGetSupertype,
    // AttributeTypeSetSupertype,
    // AttributeTypeGetSupertypes,
    // AttributeTypeGetSubtypes,
    // AttributeTypeGetInstances,
    // AttributeTypeGetRegex,
    // AttributeTypeSetRegex,
    // AttributeTypeGetOwners,
}
