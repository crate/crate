package org.cratedb.test.integration;

import org.junit.After;
import org.junit.Before;

/**
 * Doctest Testcase with full control over configuring, starting and stopping crate nodes
 */
public class DoctestTestCase extends CrateIntegrationTest {
    private DoctestRunner doctestRunner = new DoctestRunner();

    @Before
    public void setupDoctests() throws Exception {
        doctestRunner.setUp();
    }

    @After
    public void tearDownDoctests() throws Exception {
        doctestRunner.tearDown();
    }

    public void execDocFile(String filePath, Class<?> klass) {
        doctestRunner.execDocFile(filePath, klass);
    }
}
