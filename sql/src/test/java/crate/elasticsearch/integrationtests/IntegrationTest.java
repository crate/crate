package crate.elasticsearch.integrationtests;

import crate.elasticsearch.DoctestSetup;
import org.junit.Test;

public class IntegrationTest extends DoctestSetup {

    @Test
    public void testSQLSelect() throws Exception {
        execDocFile("../java/crate/elasticsearch/integrationtests/select.rst");
    }
}
