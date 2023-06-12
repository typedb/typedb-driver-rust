import com.vaticle.typedb.client.jni.typedb_client_java;

public class Main {
    static {
        System.loadLibrary("typedb_client_java");
    }

    public static void main(String[] args) {
        typedb_client_java.hello();
    }
}
