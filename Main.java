import com.vaticle.typedb.client.jni.Error;
import com.vaticle.typedb.client.jni.*;
import static com.vaticle.typedb.client.jni.typedb_client_jni.*;

public class Main {
    static {
        System.loadLibrary("typedb_client_jni");
    }

    static class CB extends SessionCallbackDirector {
        final String a;
        CB(String b) { a = b; }
        @Override
        public void callback() {
            System.out.println("closed " + a + ", cb owned: " + swigCMemOwn);
        }
    }

    public static void main(String[] args) throws Error {
        try {
            Connection conn = connection_open_plaintext("0.0.0.0:1729");
            DatabaseManager dbmgr = database_manager_new(conn);

            databases_all(dbmgr).stream().forEach(db -> database_delete(db.released()));
            databases_create(dbmgr, "test");
            Session ses = session_new(databases_get(dbmgr, "test").released(), SessionType.Data, options_new());
            session_on_close(ses, new CB("session 1").released());
            Session ses2 = session_new(databases_get(dbmgr, "test").released(), SessionType.Data, options_new());
            session_on_close(ses2, new CB("session 2").released());
            ses2.delete();
            ses.delete();
        } catch (java.lang.RuntimeException e) {
            System.out.println(e);
        }
    }
}
