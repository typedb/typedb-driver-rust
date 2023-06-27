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

struct Connection {};
%newobject connection_open_plaintext;
%newobject connection_open_encrypted;
%ignore connection_close;
%extend Connection { ~Connection() { connection_close(self); } }

struct DatabaseManager {};
%newobject database_manager_new;
%ignore database_manager_drop;
%extend DatabaseManager { ~DatabaseManager() { database_manager_drop(self); } }

%newobject databases_all;
%newobject databases_get;

struct DatabaseIterator {};
%ignore database_iterator_drop;
%extend DatabaseIterator { ~DatabaseIterator() { database_iterator_drop(self); } }

%newobject database_iterator_next;

struct Database {};
%ignore database_drop;
%extend Database { ~Database() { database_drop(self); } }

%delobject database_delete;
%newobject database_get_name;

struct Session {};
%newobject session_new;
%ignore session_drop;
%extend Session { ~Session() { session_drop(self); } }

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

struct Transaction {};
%newobject transaction_new;
%ignore transaction_drop;
%extend Transaction { ~Transaction() { transaction_drop(self); } }
%delobject transaction_commit;

struct Options {};
%newobject options_new;
%ignore options_drop;
%extend Options { ~Options() { options_drop(self); } }

struct ConceptIterator {};
%ignore concept_iterator_drop;
%extend ConceptIterator { ~ConceptIterator() { concept_iterator_drop(self); } }

%newobject concept_iterator_next;

struct Concept {};
%ignore concept_drop;
%extend Concept { ~Concept() { concept_drop(self); } }

struct RolePlayerIterator {};
%ignore role_player_iterator_drop;
%extend RolePlayerIterator { ~RolePlayerIterator() { role_player_iterator_drop(self); } }

%newobject role_player_iterator_next;

struct RolePlayer {};
%ignore role_player_drop;
%extend RolePlayer { ~RolePlayer() { role_player_drop(self); } }

struct Error {};
%newobject get_last_error;
%ignore error_drop;
%extend Error { ~Error() { error_drop(self); } }
%newobject error_code;
%newobject error_message;

%typemap(newfree) char* "string_free($1);";
%ignore string_free;

%include "typedb_client.h"
