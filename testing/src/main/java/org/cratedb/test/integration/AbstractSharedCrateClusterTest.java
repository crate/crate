package org.cratedb.test.integration;


import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.AbstractSharedClusterTest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.junit.Before;

import static org.cratedb.test.integration.PathAccessor.bytesFromPath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@AbstractRandomizedTest.IntegrationTests
public abstract class AbstractSharedCrateClusterTest extends AbstractSharedClusterTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public RefreshResponse refresh() {
        return super.refresh();
    }

    public static ClusterState clusterState() {
        return client().admin().cluster().prepareState().execute().actionGet().getState();
    }

    public void createBlobIndex(String... names) {
        ImmutableSettings.Builder builder = randomSettingsBuilder();
        builder.put("blobs.enabled", true);
        createIndex(builder.build(), names);
    }

    public void createIndex(Settings settings, String... names) {
        for (String name : names) {
            try {
                assertAcked(prepareCreate(name).setSettings(settings));
                continue;
            } catch (IndexAlreadyExistsException ex) {
                wipeIndex(name);
            }
            assertAcked(prepareCreate(name).setSettings(settings));
        }
    }

    public BulkResponse loadBulk(String path, Class<?> aClass) throws Exception {
        byte[] bulkPayload = bytesFromPath(path, aClass);
        BulkResponse bulk = client().prepareBulk().add(bulkPayload, 0, bulkPayload.length, false, null, null).execute().actionGet();
        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : String.format("unable to index data {}", item);
        }
        return bulk;
    }
}
