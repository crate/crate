package crate.elasticsearch.integrationtests;

import crate.test.integration.DoctestTestCase;
import org.junit.Before;
import org.junit.Test;

public class IntegrationTest extends DoctestTestCase {

    @Before
    public void setUpIndex() throws Exception {
        new Setup(this).setUpLocations();
    }

    @Test
    public void testSQLSelect() throws Exception {
        execDocFile("select.rst");
    }

}
