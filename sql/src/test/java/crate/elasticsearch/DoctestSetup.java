package crate.elasticsearch;

import org.junit.Before;
import org.python.core.*;
import org.python.util.PythonInterpreter;

public abstract class DoctestSetup extends TestSetup {

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

    protected void execDocFile(String name) {
        execFile(PY_TEST, name);
    }

    @Before
    @Override
    public void setUp() {
        if (interp == null) {
            resetInterpreter();
        }

        super.setUp();
    }
}
