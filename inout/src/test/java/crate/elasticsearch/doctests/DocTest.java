package crate.elasticsearch.doctests;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;
import static com.github.tlrx.elasticsearch.test.EsSetup.fromClassPath;
import junit.framework.TestCase;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.python.core.Py;
import org.python.core.PyArray;
import org.python.core.PyList;
import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;

public class DocTest extends TestCase {

    StoreEsSetup esSetup, esSetup2;

    private static final String PY_TEST = "src/test/python/tests.py";

    private PythonInterpreter interp;
    private PySystemState sys;

    private void resetInterpreter() {
        interp = new PythonInterpreter(null, new PySystemState());
        sys = Py.getSystemState();
    }

    private void execFile(String filePath, String... arguments) {
        interp.cleanup();
        interp.set("__file__", filePath);
        sys.argv = new PyList(new PyString[]{new PyString(filePath)});
        sys.argv.extend(new PyArray(PyString.class, arguments));
        interp.execfile(filePath);
    }

    private void execDocFile(String name) {
        execFile(PY_TEST, name);
    }

    @Before
    public void setUp() {
        if (interp == null) {
            resetInterpreter();
        }
        Settings s1 = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "a")
                .put("node.local", false)
                .build();

        esSetup = new StoreEsSetup(s1);
        esSetup.execute(deleteAll(), createIndex("users").withSettings(
                fromClassPath("essetup/settings/test_a.json")).withMapping("d",
                fromClassPath("essetup/mappings/test_a.json")).withData(
                fromClassPath("essetup/data/test_a.json")));
        esSetup.client().admin().indices().prepareRefresh("users").execute().actionGet();

        Settings s2 = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "b")
                .put("node.local", false)
                .build();
        esSetup2 = new StoreEsSetup(s2);
        esSetup2.execute(deleteAll());
    }

    @After
    public void tearDown() {
        esSetup.terminate();
        if (esSetup2 != null) {
            esSetup2.terminate();
        }
    }

    public void testSearchInto() throws Exception {
        execDocFile("search_into.rst");
    }

    public void testReindex() throws Exception {
        execDocFile("reindex.rst");
    }

}
