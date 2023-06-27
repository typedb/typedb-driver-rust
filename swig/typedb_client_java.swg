%javaconst(1);

%include "various.i"
%include "enums.swg"

%typemap(javacode) SWIGTYPE %{
    public $javaclassname released() {
        if (swigCMemOwn) {
            swigCMemOwn = false;
            return this;
        }
        return null;
    }
%}

/* This tells SWIG to treat char ** as a special case when used as a parameter
      in a function call */
%typemap(in) char ** (jint size) {
    int i = 0;
    size = jenv->GetArrayLength($input);
    $1 = (char **) malloc((size+1)*sizeof(char *));
    /* make a copy of each string */
    for (i = 0; i<size; i++) {
        jstring j_string = (jstring)jenv->GetObjectArrayElement($input, i);
        const char * c_string = jenv->GetStringUTFChars(j_string, 0);
        $1[i] = (char*)malloc((strlen(c_string)+1)*sizeof(char));
        strcpy($1[i], c_string);
        jenv->ReleaseStringUTFChars(j_string, c_string);
        jenv->DeleteLocalRef(j_string);
    }
    $1[i] = 0;
}

/* This cleans up the memory we malloc'd before the function call */
%typemap(freearg) char ** {
    int i;
    for (i=0; i<size$argnum-1; i++)
        free($1[i]);
    free($1);
}

/* This allows a C function to return a char ** as a Java String array */
%typemap(out) char ** {
    int i;
    int len=0;
    jstring temp_string;
    const jclass clazz = jenv->FindClass("java/lang/String");

    while ($1[len]) len++;
    jresult = jenv->NewObjectArray(len, clazz, NULL);
    /* error checking omitted */

    for (i=0; i<len; i++) {
        temp_string = jenv->NewStringUTF(*result++);
        jenv->SetObjectArrayElement(jresult, i, temp_string);
        jenv->DeleteLocalRef(temp_string);
    }
}

/* These 3 typemaps tell SWIG what JNI and Java types to use */
%typemap(jni) char ** "jobjectArray"
%typemap(jtype) char ** "String[]"
%typemap(jstype) char ** "String[]"

/* These 2 typemaps handle the conversion of the jtype to jstype typemap type
      and vice versa */
%typemap(javain) char ** "$javainput"
%typemap(javaout) char ** {
    return $jnicall;
}

%ignore get_last_error;
%javaexception("Error") {
    $action
    if (check_error()) {
        jenv->ExceptionClear();

        jlong jerror = 0;
        Error* error = get_last_error();
        *(Error**)&jerror = error;
        jclass exceptionClass = jenv->FindClass(PACKAGE_PATH_ "/Error");
        jmethodID ctor = jenv->GetMethodID(exceptionClass, "<init>", "(JZ)V");
        jthrowable throwable = (jthrowable)jenv->NewObject(exceptionClass, ctor, jerror, true);

        jenv->Throw(throwable);

        return $null;
    }
}

/*
%nojavaexception ~Connection;
%nojavaexception ~DatabaseManager;
%nojavaexception ~Database;
%nojavaexception ~Error;
%nojavaexception error_code;
%nojavaexception error_message;
%nojavaexception database_manager_new;
%nojavaexception database_get_name;
*/

%typemap(javabase) Error "java.lang.RuntimeException";
%typemap(javabody) Error %{
    private transient long swigCPtr;
    protected transient boolean swigCMemOwn;

    protected $javaclassname(long cPtr, boolean cMemoryOwn) {
        super(typedb_client_jniJNI.error_code(cPtr, null) + " " + typedb_client_jniJNI.error_message(cPtr, null));
        swigCMemOwn = cMemoryOwn;
        swigCPtr = cPtr;
    }

    protected static long getCPtr($javaclassname obj) {
        return (obj == null) ? 0 : obj.swigCPtr;
    } 
%}

%typemap(javaimports) DatabaseIterator "
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
"
%typemap(javainterfaces) DatabaseIterator "java.util.Iterator<Database>";
%typemap(javacode) DatabaseIterator %{
    private boolean isFinished = false;
    private Database next = null;

    private Database fetchNext() {
        next = typedb_client_jni.database_iterator_next(this);
        return next;
    }

    @Override
    public boolean hasNext() {
        if (isFinished) return false;
        if (next == null) {
            if (fetchNext() == null) {
                isFinished = true;
                return false;
            }
        }
        assert(next != null);
        return true;
    }

    @Override
    public Database next() {
        if (next == null && !hasNext()) return null;
        Database ret = next;
        next = null;
        return ret;
    }

    public Stream<Database> stream() {
        return StreamSupport.stream(
            spliteratorUnknownSize(this, NONNULL | IMMUTABLE | ORDERED),
            false
        );
    }
%}

%typemap(javaimports) ConceptIterator "
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
"
%typemap(javainterfaces) ConceptIterator "java.util.Iterator<Concept>";
%typemap(javacode) ConceptIterator %{
    private boolean isFinished = false;
    private Concept next = null;

    private Concept fetchNext() {
        next = typedb_client_jni.concept_iterator_next(this);
        return next;
    }

    @Override
    public boolean hasNext() {
        if (isFinished) return false;
        if (next == null) {
            if (fetchNext() == null) {
                isFinished = true;
                return false;
            }
        }
        assert(next != null);
        return true;
    }

    @Override
    public Concept next() {
        if (next == null && !hasNext()) return null;
        Concept ret = next;
        next = null;
        return ret;
    }

    public Stream<Concept> stream() {
        return StreamSupport.stream(
            spliteratorUnknownSize(this, NONNULL | IMMUTABLE | ORDERED),
            false
        );
    }
%}

%typemap(javaimports) RolePlayerIterator "
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
"
%typemap(javainterfaces) RolePlayerIterator "java.util.Iterator<RolePlayer>";
%typemap(javacode) RolePlayerIterator %{
    private boolean isFinished = false;
    private RolePlayer next = null;

    private RolePlayer fetchNext() {
        next = typedb_client_jni.role_player_iterator_next(this);
        return next;
    }

    @Override
    public boolean hasNext() {
        if (isFinished) return false;
        if (next == null) {
            if (fetchNext() == null) {
                isFinished = true;
                return false;
            }
        }
        assert(next != null);
        return true;
    }

    @Override
    public RolePlayer next() {
        if (next == null && !hasNext()) return null;
        RolePlayer ret = next;
        next = null;
        return ret;
    }

    public Stream<RolePlayer> stream() {
        return StreamSupport.stream(
            spliteratorUnknownSize(this, NONNULL | IMMUTABLE | ORDERED),
            false
        );
    }
%}
