package org.cratedb.test.integration;

import org.junit.Test;

public class IntegrationTestTest extends DoctestTestCase {

    @Test
    public void testBlobIndexGetsCreated() throws Exception {
        createBlobIndex("blobby");
        assert(clusterState().getMetaData().indices().containsKey("blobby"));
    }

    @Test
    public void testReadme() throws Exception {
        execDocFile("README.txt", getClass());
    }

}
