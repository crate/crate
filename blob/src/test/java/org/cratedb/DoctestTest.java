package org.cratedb;

import org.cratedb.test.integration.DoctestTestCase;
import org.junit.Test;


public class DoctestTest extends DoctestTestCase {

    @Test
    public void testBlob() throws Exception {

        createIndex(settingsBuilder()
                .put("blobs.enabled", true)
                .put("number_of_shards", 2)
                .put("number_of_replicas", 0).build(), "test", "test_blobs2");

        createIndex(settingsBuilder()
                .put("blobs.enabled", false)
                .put("number_of_shards", 2)
                .put("number_of_replicas", 0).build(), "test_no_blobs");

        execDocFile("integrationtests/blob.rst");

    }

}
