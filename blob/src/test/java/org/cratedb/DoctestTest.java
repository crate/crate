package org.cratedb;

import org.cratedb.test.integration.CrateIntegrationTest;
import org.cratedb.test.integration.DoctestClusterTestCase;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Test;


@CrateIntegrationTest.ClusterScope(numNodes = 2, scope = CrateIntegrationTest.Scope.GLOBAL, transportClientRatio = 0)
public class DoctestTest extends DoctestClusterTestCase {

    @Test
    public void testBlob() throws Exception {

        client().admin().indices().prepareCreate("test")
            .setSettings(
                ImmutableSettings.builder()
                    .put("blobs.enabled", true)
                    .put("number_of_shards", 2)
                    .put("number_of_replicas", 0).build()).execute().actionGet();

        client().admin().indices().prepareCreate("test_blobs2")
            .setSettings(
                ImmutableSettings.builder()
                    .put("blobs.enabled", true)
                    .put("number_of_shards", 2)
                    .put("number_of_replicas", 0).build()).execute().actionGet();

        client().admin().indices().prepareCreate("test_no_blobs")
            .setSettings(
                ImmutableSettings.builder()
                .put("blobs.enabled", false)
                .put("number_of_shards", 2)
                .put("number_of_replicas", 0).build()).execute().actionGet();

        execDocFile("integrationtests/blob.rst", getClass());
    }

}
