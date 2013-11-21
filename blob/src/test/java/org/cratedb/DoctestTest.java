package org.cratedb;

import org.apache.lucene.util.AbstractRandomizedTest;
import org.cratedb.test.integration.DoctestClusterTestCase;
import org.junit.Test;


@AbstractRandomizedTest.IntegrationTests
public class DoctestTest extends DoctestClusterTestCase {

    @Test
    public void testBlob() throws Exception {

        createIndex(randomSettingsBuilder()
                .put("blobs.enabled", true)
                .put("number_of_shards", 2)
                .put("number_of_replicas", 0).build(), "test", "test_blobs2");

        createIndex(randomSettingsBuilder()
                .put("blobs.enabled", false)
                .put("number_of_shards", 2)
                .put("number_of_replicas", 0).build(), "test_no_blobs");

        execDocFile("integrationtests/blob.rst", getClass());

    }

}
