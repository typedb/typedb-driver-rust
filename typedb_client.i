%module(directors="1") typedb_client_jni
%{
#define PACKAGE_ "com.vaticle.typedb.client.jni"
#define PACKAGE_PATH_ "com/vaticle/typedb/client/jni"

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

#define connection_drop connection_close

%proxy(Connection, connection)
%proxy(Session, session)
%proxy(Transaction, transaction)

%proxy(DatabaseManager, database_manager);

%proxy(Database, database)
%proxy(DatabaseIterator, database_iterator)

%proxy(Concept, concept)
%proxy(ConceptIterator, concept_iterator)

%proxy(Value, value)

%proxy(ConceptMap, concept_map)
%proxy(ConceptMapIterator, concept_map_iterator)

%proxy(ConceptMapGroup, concept_map_group)
%proxy(ConceptMapGroupIterator, concept_map_group_iterator)

%proxy(Numeric, numeric)

%proxy(NumericGroup, numeric_group)
%proxy(NumericGroupIterator, numeric_group_iterator)

%proxy(Explanation, explanation)
%proxy(ExplanationIterator, explanation_iterator)

%proxy(RolePlayer, role_player)
%proxy(RolePlayerIterator, role_player_iterator)

%proxy(Error, error)

%proxy(Options, options)

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

%newobject connection_open_plaintext;
%newobject connection_open_encrypted;

%newobject database_manager_new;

%newobject databases_all;
%newobject databases_get;

%newobject database_iterator_next;

%delobject database_delete;
%newobject database_get_name;

%newobject session_new;

%newobject transaction_new;
%delobject transaction_commit;

%newobject options_new;

%newobject concept_iterator_next;

%newobject role_player_iterator_next;

%newobject get_last_error;
%newobject error_code;
%newobject error_message;

%typemap(newfree) char* "string_free($1);";
%ignore string_free;

%include "typedb_client.h"
