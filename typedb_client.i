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

%module(directors="1") typedb_client_jni
%{
extern "C" {
#include "typedb_client.h"
}
%}
%include "stdint.i"
%include "carrays.i"
%include "typemaps.i"

#ifdef SWIGJAVA
%include "swig/typedb_client_java.swg"
#endif

%nodefaultctor;

#define %proxy(Foo, foo)                        \
struct Foo {};                                  \
%ignore foo ## _drop;                           \
%extend Foo { ~Foo() { foo ## _drop(self); } }  \

%proxy(Error, error)

%proxy(Credential, credential)
%proxy(Options, options)

#define connection_drop connection_close

%proxy(Connection, connection)
%proxy(Session, session)
%proxy(Transaction, transaction)

%proxy(DatabaseManager, database_manager);
%proxy(Database, database)
%proxy(DatabaseIterator, database_iterator)

%proxy(UserManager, user_manager);
%proxy(User, user)
%proxy(UserIterator, user_iterator)


%proxy(Concept, concept)
%proxy(ConceptIterator, concept_iterator)

%proxy(Value, value)

%proxy(RolePlayer, role_player)
%proxy(RolePlayerIterator, role_player_iterator)

%proxy(ConceptMap, concept_map)
%proxy(ConceptMapIterator, concept_map_iterator)
%proxy(Explainables, explainables)
%proxy(Explainable, explainable)

%proxy(ConceptMapGroup, concept_map_group)
%proxy(ConceptMapGroupIterator, concept_map_group_iterator)

%proxy(StringIterator, string_iterator)
%proxy(StringPairIterator, string_pair_iterator)

%proxy(Numeric, numeric)

%proxy(NumericGroup, numeric_group)
%proxy(NumericGroupIterator, numeric_group_iterator)

%proxy(Explanation, explanation)
%proxy(ExplanationIterator, explanation_iterator)

%proxy(Rule, rule)
%proxy(RuleIterator, rule_iterator)

%feature("director") SessionCallbackDirector;
%inline %{
struct SessionCallbackDirector {
    SessionCallbackDirector() {}
    virtual ~SessionCallbackDirector() {}
    virtual void callback() = 0;
};
%}

%{
#include <memory>
#include <unordered_map>
static std::unordered_map<size_t, SessionCallbackDirector*> registeredSessionCallbacks {};
static void session_callback_helper(size_t address) {
    registeredSessionCallbacks.at(address)->callback();
    registeredSessionCallbacks.erase(address);
}
%}

%rename(session_on_close) session_on_close_wrapper;
%ignore session_on_close;
%inline %{
void session_on_close_wrapper(const Session* session, SessionCallbackDirector* handler) {
    registeredSessionCallbacks.insert({(size_t)session, handler});
    session_on_close(session, &session_callback_helper);
}
%}

%feature("director") TransactionCallbackDirector;
%inline %{
struct TransactionCallbackDirector {
    TransactionCallbackDirector() {}
    virtual ~TransactionCallbackDirector() {}
    virtual void callback(Error*) = 0;
};
%}

%{
#include <memory>
#include <unordered_map>
static std::unordered_map<size_t, TransactionCallbackDirector*> registeredTransactionCallbacks {};
static void transaction_callback_helper(size_t address, Error* error) {
    registeredTransactionCallbacks.at(address)->callback(error);
    registeredTransactionCallbacks.erase(address);
}
%}

%rename(transaction_on_close) transaction_on_close_wrapper;
%ignore transaction_on_close;
%inline %{
void transaction_on_close_wrapper(const Transaction* transaction, TransactionCallbackDirector* handler) {
    registeredTransactionCallbacks.insert({(size_t)transaction, handler});
    transaction_on_close(transaction, &transaction_callback_helper);
}
%}

%delobject database_delete;

%delobject transaction_commit;

%typemap(newfree) char* "string_free($1);";
%ignore string_free;

%newobject concept_map_get_variables;
%newobject concept_map_get_values;
%newobject concept_map_get;
%newobject concept_map_get_explainables;
%newobject concept_map_to_string;

%newobject explainables_get_relation;
%newobject explainables_get_attribute;
%newobject explainables_get_ownership;
%newobject explainables_get_relations_keys;
%newobject explainables_get_attributes_keys;
%newobject explainables_get_ownerships_keys;

%newobject explainable_get_conjunction;

%newobject explanation_get_rule;
%newobject explanation_get_conclusion;
%newobject explanation_get_condition;

%newobject concept_map_group_get_owner;
%newobject concept_map_group_get_concept_maps;

%newobject numeric_group_get_owner;
%newobject numeric_group_get_numeric;

%newobject string_iterator_next;

%newobject string_pair_iterator_next;

%newobject value_new_boolean;
%newobject value_new_long;
%newobject value_new_double;
%newobject value_new_string;
%newobject value_new_date_time_from_millis;
%newobject value_get_string;

%newobject concept_to_string;

%newobject concepts_get_entity_type;
%newobject concepts_get_relation_type;
%newobject concepts_get_attribute_type;
%newobject concepts_put_entity_type;
%newobject concepts_put_relation_type;
%newobject concepts_put_attribute_type;
%newobject concepts_get_entity;
%newobject concepts_get_relation;
%newobject concepts_get_attribute;

%newobject concept_iterator_next;

%newobject role_player_iterator_next;

%newobject role_player_get_role_type;
%newobject role_player_get_player;

%newobject thing_get_iid;

%newobject entity_get_type;
%newobject relation_get_type;
%newobject attribute_get_type;

%newobject attribute_get_value;

%newobject thing_get_has;
%newobject thing_get_relations;
%newobject thing_get_playing;

%newobject relation_get_players_by_role_type;
%newobject relation_get_role_players;
%newobject relation_get_relating;

%newobject attribute_get_owners;

%newobject thing_type_get_label;
%newobject thing_type_get_owns;
%newobject thing_type_get_owns_overridden;
%newobject thing_type_get_plays;
%newobject thing_type_get_plays_overridden;
%newobject thing_type_get_syntax;

%newobject entity_type_create;
%newobject entity_type_get_supertype;
%newobject entity_type_get_supertypes;
%newobject entity_type_get_subtypes;
%newobject entity_type_get_instances;

%newobject relation_type_create;
%newobject relation_type_get_supertype;
%newobject relation_type_get_supertypes;
%newobject relation_type_get_subtypes;
%newobject relation_type_get_instances;
%newobject relation_type_get_relates;
%newobject relation_type_get_relates_for_role_label;
%newobject relation_type_get_relates_overridden;

%newobject attribute_type_put;
%newobject attribute_type_get;
%newobject attribute_type_get_supertype;
%newobject attribute_type_get_supertypes;
%newobject attribute_type_get_subtypes;
%newobject attribute_type_get_subtypes_with_value_type;
%newobject attribute_type_get_instances;
%newobject attribute_type_get_regex;
%newobject attribute_type_get_owners;

%newobject role_type_get_relation_type;
%newobject role_type_get_scope;
%newobject role_type_get_name;
%newobject role_type_get_supertype;
%newobject role_type_get_supertypes;
%newobject role_type_get_subtypes;
%newobject role_type_get_relation_types;
%newobject role_type_get_player_types;
%newobject role_type_get_relation_instances;
%newobject role_type_get_player_instances;

%newobject connection_open_plaintext;
%newobject connection_open_encrypted;

%newobject credential_new;

%newobject database_get_name;
%newobject database_schema;
%newobject database_type_schema;
%newobject database_rule_schema;
%newobject database_manager_new;
%newobject database_iterator_next;

%newobject databases_all;
%newobject databases_get;

%newobject get_last_error;
%newobject error_code;
%newobject error_message;

%newobject rule_get_label;
%newobject rule_get_when;
%newobject rule_get_then;

%newobject logic_manager_put_rule;
%newobject logic_manager_get_rule;

%newobject rule_iterator_next;

%newobject logic_manager_get_rules;

%newobject options_new;

%newobject concept_map_iterator_next;

%newobject query_match;
%newobject query_insert;
%newobject query_update;
%newobject query_match_aggregate;
%newobject query_match_group;
%newobject query_match_group_aggregate;
%newobject query_explain;

%newobject concept_map_group_iterator_next;
%newobject numeric_group_iterator_next;
%newobject explanation_iterator_next;

%newobject session_new;
%newobject session_get_database_name;

%newobject transaction_new;

%include "typedb_client.h"
