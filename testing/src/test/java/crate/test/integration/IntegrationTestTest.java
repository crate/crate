package crate.test.integration;

import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import java.net.URL;

public class IntegrationTestTest extends DoctestTestCase {

    @Test
    public void testBlobIndexGetsCreated() throws Exception {
        createBlobIndex("blobby");
        assert(clusterState().getMetaData().indices().containsKey("blobby"));
    }

    @Test
    public void testReadme() throws Exception {
        execDocFile("README.txt");
    }

}
