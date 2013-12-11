package org.cratedb.test.integration;

import org.junit.Test;

public class IntegrationTestTest extends DoctestClusterTestCase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testBlobIndexGetsCreated() throws Exception {
        createBlobIndex("blobby");
        assert (clusterService().state().getMetaData().indices().containsKey("blobby"));
    }

    @Test
    public void testReadme() throws Exception {
        execDocFile("README.txt", getClass());
    }

}
