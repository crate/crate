package org.cratedb.test.integration;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.integration.ClusterManager;
import org.elasticsearch.test.integration.ElasticsearchTestCase;
import org.elasticsearch.test.integration.TestCluster;
import org.junit.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 * taken from see org.elasticsearch.test.integration
 * <p/>
 * This abstract base testcase reuses a cluster instance internally and might
 * start an abitrary number of nodes in the background. This class might in the
 * future add random configureation options to created indices etc. unless
 * unless they are explicitly defined by the test.
 * <p/>
 * <p>
 * This test wipes all indices before a testcase is executed and uses
 * elasticsearch features like allocation filters to ensure an index is
 * allocated only on a certain number of nodes. The test doesn't expose explicit
 * information about the client or which client is returned, clients might be
 * node clients or transport clients and the returned client might be rotated.
 * </p>
 * <p/>
 * Tests that need more explict control over the cluster or that need to change
 * the cluster state aside of per-index settings should not use this class as a
 * baseclass. If your test modifies the cluster state with persistent or
 * transient settings the baseclass will raise and error.
 */
@Ignore
public abstract class AbstractSharedCrateClusterTest extends ElasticsearchTestCase {


    private static class DataDirectoryCleaner implements Runnable {
        private CrateTestCluster crateTestCluster;

        private DataDirectoryCleaner(CrateTestCluster crateTestCluster) {
            this.crateTestCluster = crateTestCluster;
        }

        public void run() {
            crateTestCluster.deleteTemporaryDataDirectory();
        }
    }

    public static class CrateTestCluster extends TestCluster {

        private Path tmpDataDir = null;
        private DataDirectoryCleaner dataDirectoryCleaner = new DataDirectoryCleaner(this);

        public CrateTestCluster(Random random) {
            super(random);

            // Create temporary directory and use it as the data directory
            try {
                tmpDataDir = Files.createTempDirectory(null);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public Node buildNode(Settings settings) {
            ImmutableSettings.Builder builder = ImmutableSettings.builder();
            builder.put(settings);

            if (tmpDataDir != null) {
                builder.put("path.data", tmpDataDir.toAbsolutePath());
            }

            return super.buildNode(builder.build());
        }

        public void deleteTemporaryDataDirectory() {
            if (tmpDataDir != null) {
                FileSystemUtils.deleteRecursively(tmpDataDir.toFile(), true);
                tmpDataDir = null;
            }
        }

        public Runnable getShutdownRunnable() {
            return dataDirectoryCleaner;
        }

    }

    public static class CrateClusterManager extends ClusterManager {

        private static CrateTestCluster cluster;

        public synchronized static CrateTestCluster accquireCluster(Random random) {
            if (cluster == null) {
                cluster = new CrateTestCluster(random);

                Thread hook = new Thread(cluster.getShutdownRunnable());
                Runtime.getRuntime().addShutdownHook(hook);
            }
            cluster.reset(random);
            return cluster;

        }
    }


    private static CrateTestCluster cluster;

    @BeforeClass
    public static void beforeClass() throws Exception {
        cluster();
    }

    @Before
    public final void before() {
        cluster.ensureAtLeastNumNodes(numberOfNodes());
        wipeIndices();
        wipeTemplates();
    }

    @After
    public void after() {
        MetaData metaData = client().admin().cluster().prepareState().execute().actionGet().getState().getMetaData();
        assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsMap(), metaData
                .persistentSettings().getAsMap().size(), equalTo(0));
        assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsMap(), metaData
                .persistentSettings().getAsMap().size(), equalTo(0));
    }

    public static CrateTestCluster cluster() {
        if (cluster == null) {
            cluster = CrateClusterManager.accquireCluster(getRandom());
        }
        return cluster;
    }

    public ClusterService clusterService() {
        return cluster().clusterService();
    }

    @AfterClass
    public static void afterClass() {
        cluster = null;
        ClusterManager.releaseCluster();
    }

    public static Client client() {
        return cluster().client();
    }

    public static Iterable<Client> clients() {
        return cluster().clients();
    }

    public ImmutableSettings.Builder settingsBuilder() {
        return ImmutableSettings.builder();
    }

    public Settings getSettings() {
        return settingsBuilder().build();
    }

    public static void wipeIndices(String... names) {
        // crate does not allow deletion on root, so we put a star which matches all indices
        if (names.length == 0) {
            names = new String[]{"*"};
        }
        try {
            client().admin().indices().prepareDelete(names).execute().actionGet();
        } catch (IndexMissingException e) {
            // ignore
        }
    }

    public static void wipeIndex(String name) {
        wipeIndices(name);
    }

    /**
     * Deletes index templates, support wildcard notation.
     */
    public static void wipeTemplates(String... templates) {
        // if nothing is provided, delete all
        if (templates.length == 0) {
            templates = new String[]{"*"};
        }
        for (String template : templates) {
            try {
                client().admin().indices().prepareDeleteTemplate(template).execute().actionGet();
            } catch (IndexTemplateMissingException e) {
                // ignore
            }
        }
    }

    public void createBlobIndex(String... names) {
        ImmutableSettings.Builder builder = settingsBuilder();
        builder.put("blobs.enabled", true);
        createIndex(builder.build(), names);
    }

    public void createIndex(Settings settings, String... names) {
        for (String name : names) {
            try {
                prepareCreate(name).setSettings(settings).execute().actionGet();
                continue;
            } catch (IndexAlreadyExistsException ex) {
                wipeIndex(name);
            }
            prepareCreate(name).setSettings(settings).execute().actionGet();
        }
    }

    public void createIndex(String... names) {
        for (String name : names) {
            try {
                prepareCreate(name).setSettings(getSettings()).execute().actionGet();
                continue;
            } catch (IndexAlreadyExistsException ex) {
                wipeIndex(name);
            }
            prepareCreate(name).setSettings(getSettings()).execute().actionGet();
        }
    }

    public void createIndexMapped(String name, String type, String... simpleMapping) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().startObject(type).startObject("properties");
        for (int i = 0; i < simpleMapping.length; i++) {
            builder.startObject(simpleMapping[i++]).field("type", simpleMapping[i]).endObject();
        }
        builder.endObject().endObject().endObject();
        try {
            prepareCreate(name).setSettings(getSettings()).addMapping(type, builder).execute().actionGet();
            return;
        } catch (IndexAlreadyExistsException ex) {
            wipeIndex(name);
        }
        prepareCreate(name).setSettings(getSettings()).addMapping(type, builder).execute().actionGet();
    }

    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes) {
        return prepareCreate(index, numNodes, ImmutableSettings.builder());
    }

    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes, ImmutableSettings.Builder builder) {
        cluster().ensureAtLeastNumNodes(numNodes);
        Settings settings = getSettings();
        builder.put(settings);
        if (numNodes > 0) {
            getExcludeSettings(index, numNodes, builder);
        }
        return client().admin().indices().prepareCreate(index).setSettings(builder.build());
    }

    public CreateIndexRequestBuilder addMapping(CreateIndexRequestBuilder builder, String type, Object[]... mapping) throws IOException {
        XContentBuilder mappingBuilder = jsonBuilder();
        mappingBuilder.startObject().startObject(type).startObject("properties");
        for (Object[] objects : mapping) {
            mappingBuilder.startObject(objects[0].toString());
            for (int i = 1; i < objects.length; i++) {
                String name = objects[i++].toString();
                Object value = objects[i];
                mappingBuilder.field(name, value);
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject().endObject().endObject();
        builder.addMapping(type, mappingBuilder);
        return builder;
    }

    private ImmutableSettings.Builder getExcludeSettings(String index, int num, ImmutableSettings.Builder builder) {
        String exclude = Joiner.on(',').join(cluster().allButN(num));
        builder.put("index.routing.allocation.exclude._name", exclude);
        return builder;
    }

    public Set<String> getExcludeNodes(String index, int num) {
        Set<String> nodeExclude = cluster().nodeExclude(index);
        Set<String> nodesInclude = cluster().nodesInclude(index);
        if (nodesInclude.size() < num) {
            Iterator<String> limit = Iterators.limit(nodeExclude.iterator(), num - nodesInclude.size());
            while (limit.hasNext()) {
                limit.next();
                limit.remove();
            }
        } else {
            Iterator<String> limit = Iterators.limit(nodesInclude.iterator(), nodesInclude.size() - num);
            while (limit.hasNext()) {
                nodeExclude.add(limit.next());
                limit.remove();
            }
        }
        return nodeExclude;
    }

    public void allowNodes(String index, int numNodes) {
        cluster().ensureAtLeastNumNodes(numNodes);
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (numNodes > 0) {
            getExcludeSettings(index, numNodes, builder);
        }
        Settings build = builder.build();
        if (!build.getAsMap().isEmpty()) {
            client().admin().indices().prepareUpdateSettings(index).setSettings(build).execute().actionGet();
        }
    }

    public CreateIndexRequestBuilder prepareCreate(String index) {
        return client().admin().indices().prepareCreate(index).setSettings(getSettings());
    }

    public void updateClusterSettings(Settings settings) {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();
    }

    public ClusterHealthStatus ensureGreen() {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(
                        0)).actionGet();
        assertThat(actionGet.isTimedOut(), equalTo(false));
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        return actionGet.getStatus();
    }

    public ClusterHealthStatus waitForRelocation() {
        return waitForRelocation(null);
    }

    public ClusterHealthStatus waitForRelocation(ClusterHealthStatus status) {
        ClusterHealthRequest request = Requests.clusterHealthRequest().waitForRelocatingShards(0);
        if (status != null) {
            request.waitForStatus(status);
        }
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(request).actionGet();
        assertThat(actionGet.isTimedOut(), equalTo(false));
        if (status != null) {
            assertThat(actionGet.getStatus(), equalTo(status));
        }
        return actionGet.getStatus();
    }

    public ClusterHealthStatus ensureYellow() {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForRelocatingShards(0).waitForYellowStatus().waitForEvents(
                        Priority.LANGUID)).actionGet();
        assertThat(actionGet.isTimedOut(), equalTo(false));
        return actionGet.getStatus();
    }

    public static String commaString(Iterable<String> strings) {
        return Joiner.on(',').join(strings);
    }

    protected int numberOfNodes() {
        return 2;
    }

    // utils
    protected void index(String index, String type, XContentBuilder source) {
        client().prepareIndex(index, type).setSource(source).execute().actionGet();
    }

    public RefreshResponse refresh() {
        waitForRelocation();
        // TODO RANDOMIZE with flush?
        RefreshResponse actionGet = client().admin().indices().prepareRefresh().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected FlushResponse flush() {
        waitForRelocation();
        FlushResponse actionGet = client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected OptimizeResponse optimize() {
        waitForRelocation();
        OptimizeResponse actionGet = client().admin().indices().prepareOptimize().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected Set<String> nodeIdsWithIndex(String... indices) {
        ClusterState state = clusterState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        Set<String> nodes = new HashSet<String>();
        for (ShardIterator shardIterator : allAssignedShardsGrouped) {
            for (ShardRouting routing : shardIterator.asUnordered()) {
                if (routing.active()) {
                    nodes.add(routing.currentNodeId());
                }

            }
        }
        return nodes;
    }

    public static ClusterState clusterState() {
        return client().admin().cluster().prepareState().execute().actionGet().getState();
    }

    protected int numAssignedShards(String... indices) {
        ClusterState state = clusterState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        return allAssignedShardsGrouped.size();
    }

    protected boolean indexExists(String index) {
        IndicesExistsResponse actionGet = client().admin().indices().prepareExists(index).execute().actionGet();
        return actionGet.isExists();
    }

    protected AdminClient admin() {
        return client().admin();
    }

    protected <Res extends ActionResponse> Res run(ActionRequestBuilder<?, Res, ?> builder) {
        Res actionGet = builder.execute().actionGet();
        return actionGet;
    }

    protected <Res extends BroadcastOperationResponse> Res run(BroadcastOperationRequestBuilder<?, Res, ?> builder) {
        Res actionGet = builder.execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    // TODO move this into a base class for integration tests
    public void indexRandom(String index, boolean forceRefresh, IndexRequestBuilder... builders) throws InterruptedException,
            ExecutionException {
        Random random = getRandom();
        List<IndexRequestBuilder> list = Arrays.asList(builders);
        Collections.shuffle(list, random);
        for (IndexRequestBuilder indexRequestBuilder : list) {
            indexRequestBuilder.execute().actionGet();
            if (frequently()) {
                if (rarely()) {
                    client().admin().indices().prepareFlush(index).execute().get();
                } else if (rarely()) {
                    client().admin().indices().prepareOptimize(index).setMaxNumSegments(between(1, 10)).setFlush(
                            random.nextBoolean()).execute().get();
                }
                client().admin().indices().prepareRefresh(index).execute().get();
            }
        }
        if (forceRefresh) {
            client().admin().indices().prepareRefresh(index).execute().get();
        }
    }

    public byte[] bytesFromPath(String path) throws IOException {
        InputStream is = getInputStream(path);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Streams.copy(is, out);
        is.close();
        out.close();
        return out.toByteArray();
    }

    public InputStream getInputStream(String path) throws FileNotFoundException {
        InputStream is = getClass().getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
        }
        return is;
    }

    public String stringFromPath(String path) throws IOException {
        return Streams.copyToString(new InputStreamReader(
                getInputStream("/essetup/mappings/test_a.json"),
                Charsets.UTF_8));
    }

    public BulkResponse loadBulk(String path) throws Exception {
        byte[] bulkPayload = bytesFromPath(path);
        BulkResponse bulk = client().prepareBulk().add(bulkPayload, 0, bulkPayload.length, false, null, null).execute().actionGet();
        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : String.format("unable to index data {}", item);
        }
        return bulk;
    }
}
