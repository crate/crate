package org.cratedb.test.integration;

import org.elasticsearch.common.Classes;
import org.python.core.*;
import org.python.util.PythonInterpreter;

import java.io.InputStream;
import java.net.URL;

public class DoctestRunner {

    private PythonInterpreter interp;
    private PySystemState sys;


    private void resetInterpreter() {
        interp = new PythonInterpreter(null, new PySystemState());
        sys = Py.getSystemState();
    }

    private void execFile(String... arguments) {
        interp.cleanup();
        InputStream s = DoctestRunner.class.getResourceAsStream("tests.py");
        // TODO: probably set __file__ to something that makes relative paths happy?
        //interp.set("__file__", filePath);
        sys.argv = new PyList(new PyString[]{new PyString("tests.py")});
        sys.argv.extend(new PyArray(PyString.class, arguments));
        interp.execfile(s, "tests.py");
    }

    protected void execDocFile(String name, Class<?> aClass) {
        URL url;
        if (aClass == null) {
            ClassLoader classLoader = Classes.getDefaultClassLoader();
            url = classLoader.getResource(name);
        } else {
            url = aClass.getResource(name);
        }
        if (url == null) {
            throw new RuntimeException("docfile resource not found: " + name);
        }
        execFile(url.getFile());
    }

    /**
     * initialize the DoctestRunner
     *
     * call this in your tests @Before method
     *
     */
    public void setUp() {
        if (interp == null) {
            resetInterpreter();
        }
    }

    /**
     * clear state that a Doctest run left behind
     *
     * call this in your tests @After method
     */
    public void tearDown() {
        // the test base classes verify that tests don't set any system properties
        // the properties here are set by the PythonInterpreter and have to be cleared
        // in order for the tests to pass
        System.clearProperty("python.cachedir.skip");
        System.clearProperty("python.console.encoding");
    }
}
