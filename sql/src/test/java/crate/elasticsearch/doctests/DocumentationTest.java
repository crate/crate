package crate.elasticsearch.doctests;

import crate.elasticsearch.DoctestSetup;
import org.junit.Test;

public class DocumentationTest extends DoctestSetup {

    @Test
    public void testIndex() throws Exception {
        execDocFile("../../../docs/index.txt");
    }

}
