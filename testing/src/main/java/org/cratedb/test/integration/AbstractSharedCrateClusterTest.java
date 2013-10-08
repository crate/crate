package org.cratedb.test.integration;


import com.google.common.base.Charsets;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.AbstractSharedClusterTest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Classes;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.junit.Before;

import java.io.*;

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

    public byte[] bytesFromPath(String path, Class<?> aClass) throws IOException {
        InputStream is = getInputStream(path, aClass);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Streams.copy(is, out);
        is.close();
        out.close();
        return out.toByteArray();
    }

    public InputStream getInputStream(String path, Class<?> aClass) throws FileNotFoundException {
        InputStream is = aClass.getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
        }
        return is;
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

    public String stringFromPath(String path, Class<?> aClass) throws IOException {
        return Streams.copyToString(new InputStreamReader(
                getInputStream(path, aClass),
                Charsets.UTF_8));
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
