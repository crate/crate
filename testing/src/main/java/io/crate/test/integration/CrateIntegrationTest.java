/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.test.integration;

import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.randomizedtesting.SeedUtils;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.google.common.base.Joiner;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.MulticastChannel;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.test.*;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.core.IsEqual.equalTo;

@ThreadLeakFilters(defaultFilters = true, filters = { CrateIntegrationTest.TestThreadFilter.class })
public class CrateIntegrationTest extends ElasticsearchTestCase {

    public static class TestThreadFilter implements ThreadFilter {

        private final Pattern nodePrefix = Pattern.compile("\\[(" +
                "(" + Pattern.quote(InternalTestCluster.TRANSPORT_CLIENT_PREFIX) + ")?(" +
                Pattern.quote(CrateTestCluster.GLOBAL_CLUSTER_NODE_PREFIX) + "|" +
                Pattern.quote(ElasticsearchIntegrationTest.SUITE_CLUSTER_NODE_PREFIX) + "|" +
                Pattern.quote(ElasticsearchIntegrationTest.TEST_CLUSTER_NODE_PREFIX) + "|" +
                Pattern.quote(ExternalTestCluster.EXTERNAL_CLUSTER_PREFIX) + ")"
                + ")\\d+\\]");

        @Override
        public boolean reject(Thread t) {
            String threadName = t.getName();
            if (threadName.contains("[" + MulticastChannel.SHARED_CHANNEL_NAME + "]")
                    || threadName.contains("[" + ElasticsearchSingleNodeTest.nodeName() + "]")
                    || threadName.contains("Keep-Alive-Timer")) {
                return true;
            }
            return nodePrefix.matcher(t.getName()).find();
        }
    }

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
        ESLoggerFactory.getRootLogger().setLevel("WARN");
        Loggers.getLogger("org.elasticsearch.http").setLevel("INFO");
    }

    public static void deleteAll() {
        wipeIndices("_all");
    }

    public Client client(String nodeName) {
        return cluster().client(nodeName);
    }

    /**
     * The random seed for the shared  test cluster used in the current JVM.
     */
    public static final long SHARED_CLUSTER_SEED = clusterSeed();

    public static final CrateTestCluster GLOBAL_CLUSTER = new CrateTestCluster(
        SHARED_CLUSTER_SEED, CrateTestCluster.clusterName("shared",
            Integer.toString(CHILD_JVM_ID), SHARED_CLUSTER_SEED));

    /**
     * Key used to set the shared cluster random seed via the commandline -D{@value #TESTS_CLUSTER_SEED}
     */
    public static final String TESTS_CLUSTER_SEED = "tests.cluster_seed";


    /**
     * The current cluster depending on the configured {@link Scope}.
     * By default if no {@link ClusterScope} is configured this will hold a reference to the global cluster carried
     * on across test suites.
     */
    private static CrateTestCluster currentCluster;

    private static final Map<Class<?>, CrateTestCluster> clusters = new IdentityHashMap<Class<?>, CrateTestCluster>();

    @Before
    public synchronized void before() throws IOException {
        final Scope currentClusterScope = getCurrentClusterScope();
        switch (currentClusterScope) {
            case GLOBAL:
                clearClusters();
                currentCluster = GLOBAL_CLUSTER;
                break;
            case SUITE:
                currentCluster = buildAndPutCluster(currentClusterScope, false);
                break;
            case TEST:
                currentCluster = buildAndPutCluster(currentClusterScope, true);
                break;
            default:
                assert false : "Unknown Scope: [" + currentClusterScope + "]";
        }
        currentCluster.beforeTest(getRandom());
        wipeIndices("_all");
        wipeTemplates();
        logger.info("[{}#{}]: before test", getTestClass().getSimpleName(), getTestName());
    }

    public CrateTestCluster buildAndPutCluster(Scope currentClusterScope, boolean createIfExists) throws IOException {
        CrateTestCluster testCluster = clusters.get(this.getClass());
        if (createIfExists || testCluster == null) {
            testCluster = buildTestCluster(currentClusterScope);
        } else {
            assert testCluster != null;
            clusters.remove(this.getClass());
        }
        clearClusters();
        clusters.put(this.getClass(), testCluster);
        return testCluster;
    }

    private void clearClusters() throws IOException {
        if (!clusters.isEmpty()) {
            for(CrateTestCluster cluster : clusters.values()) {
                cluster.close();
            }
            clusters.clear();
        }
    }

    @After
    public synchronized void after() throws IOException {
        try {
            logger.info("[{}#{}]: cleaning up after test", getTestClass().getSimpleName(), getTestName());
            Scope currentClusterScope = getCurrentClusterScope();
            if (currentClusterScope == Scope.TEST) {
                clearClusters(); // it is ok to leave persistent / transient cluster state behind if scope is TEST
            } else if (cluster() != null) {
                MetaData metaData = cluster().client().admin().cluster().prepareState().execute().actionGet().getState().getMetaData();
                // TODO: enable assert
                // assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsMap(), metaData
                //     .persistentSettings().getAsMap().size(), equalTo(0));
                // assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsMap(), metaData
                //     .persistentSettings().getAsMap().size(), equalTo(0));

            }
            wipeIndices("_all"); // wipe after to make sure we fail in the test that
            // didn't ack the delete
            wipeTemplates();
            wipeRepositories();
            logger.info("[{}#{}]: cleaned up after test", getTestClass().getSimpleName(), getTestName());
        } finally {
            if (currentCluster != null) {
                currentCluster.afterTest();
                currentCluster = null;
            }
        }
    }

    public static CrateTestCluster cluster() {
        return currentCluster;
    }

    public ClusterService clusterService() {
        return cluster().clusterService();
    }

    public static Client client() {
        return cluster().client();
    }

    public static Iterable<Client> clients() {
        return cluster();
    }

    /**
     * Returns a settings object used in {@link #createIndex(String...)} and {@link #prepareCreate(String)} and friends.
     * This method can be overwritten by subclasses to set defaults for the indices that are created by the test.
     * By default it returns an empty settings object.
     */
    public Settings indexSettings() {
        return ImmutableSettings.EMPTY;
    }

    /**
     * Deletes the given indices from the tests cluster. If no index name is passed to this method
     * all indices are removed.
     */
    public static void wipeIndices(String... indices) {
        assert indices != null && indices.length > 0;
        if (cluster() != null && cluster().size() > 0) {
            try {
                assertAcked(cluster().client().admin().indices().prepareDelete(indices));
            } catch (IndexMissingException e) {
                // ignore
            } catch (ElasticsearchIllegalArgumentException e) {
                // Happens if `action.destructive_requires_name` is set to true
                // which is the case in the CloseIndexDisableCloseAllTests
                if ("_all".equals(indices[0])) {
                    ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
                    ObjectArrayList<String> concreteIndices = new ObjectArrayList<String>();
                    for (IndexMetaData indexMetaData : clusterStateResponse.getState().metaData()) {
                        concreteIndices.add(indexMetaData.getIndex());
                    }
                    if (!concreteIndices.isEmpty()) {
                        assertAcked(client().admin().indices().prepareDelete(concreteIndices.toArray(String.class)));
                    }
                }
            }
        }
    }

    /**
     * Deletes index templates, support wildcard notation.
     * If no template name is passed to this method all templates are removed.
     */
    public static void wipeTemplates(String... templates) {
        if (cluster() != null && cluster().size() > 0) {
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
    }

    /**
     * Deletes repositories, supports wildcard notation.
     */
    public static void wipeRepositories(String... repositories) {
        if (cluster() != null && cluster().size() > 0) {
            // if nothing is provided, delete all
            if (repositories.length == 0) {
                repositories = new String[]{"*"};
            }
            for (String repository : repositories) {
                try {
                    client().admin().cluster().prepareDeleteRepository(repository).execute().actionGet();
                } catch (RepositoryMissingException ex) {
                    // ignore
                }
            }
        }
    }

    /**
     * Creates one or more indices and asserts that the indices are acknowledged. If one of the indices
     * already exists this method will fail and wipe all the indices created so far.
     */
    public final void createIndex(String... names) {

        List<String> created = new ArrayList<>();
        for (String name : names) {
            boolean success = false;
            try {
                assertAcked(prepareCreate(name));
                created.add(name);
                success = true;
            } finally {
                if (!success) {
                    wipeIndices(created.toArray(new String[0]));
                }
            }
        }
    }


    public BulkResponse loadBulk(Client client, String path, Class<?> aClass) throws Exception {
        byte[] bulkPayload = PathAccessor.bytesFromPath(path, aClass);
        BulkResponse bulk = client.prepareBulk().add(bulkPayload, 0, bulkPayload.length, false, null, null).execute().actionGet();
        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : String.format("unable to index data {}", item);
        }
        return bulk;
    }

    public BulkResponse loadBulk(String path, Class<?> aClass) throws Exception {
        return loadBulk(client(), path, aClass);
    }


    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     */
    public final CreateIndexRequestBuilder prepareCreate(String index) {
        return client().admin().indices().prepareCreate(index).setSettings(indexSettings());
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     * The index that is created with this builder will only be allowed to allocate on the number of nodes passed to this
     * method.
     * <p>
     * This method uses allocation deciders to filter out certain nodes to allocate the created index on. It defines allocation
     * rules based on <code>index.routing.allocation.exclude._name</code>.
     * </p>
     */
    public final CreateIndexRequestBuilder prepareCreate(String index, int numNodes) {
        return prepareCreate(index, numNodes, ImmutableSettings.builder());
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     * The index that is created with this builder will only be allowed to allocate on the number of nodes passed to this
     * method.
     * <p>
     * This method uses allocation deciders to filter out certain nodes to allocate the created index on. It defines allocation
     * rules based on <code>index.routing.allocation.exclude._name</code>.
     * </p>
     */
    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes, ImmutableSettings.Builder builder) {
        cluster().ensureAtLeastNumNodes(numNodes);
        Settings settings = indexSettings();
        builder.put(settings);
        if (numNodes > 0) {
            getExcludeSettings(index, numNodes, builder);
        }
        return client().admin().indices().prepareCreate(index).setSettings(builder.build());
    }

    private ImmutableSettings.Builder getExcludeSettings(String index, int num, ImmutableSettings.Builder builder) {
        String exclude = Joiner.on(',').join(cluster().allButN(num));
        builder.put("index.routing.allocation.exclude._name", exclude);
        return builder;
    }

    /**
     * Restricts the given index to be allocated on <code>n</code> nodes using the allocation deciders.
     * Yet if the shards can't be allocated on any other node shards for this index will remain allocated on
     * more than <code>n</code> nodes.
     */
    public void allowNodes(String index, int n) {
        assert index != null;
        cluster().ensureAtLeastNumNodes(n);
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (n > 0) {
            getExcludeSettings(index, n, builder);
        }
        Settings build = builder.build();
        if (!build.getAsMap().isEmpty()) {
            client().admin().indices().prepareUpdateSettings(index).setSettings(build).execute().actionGet();
        }
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     */
    public ClusterHealthStatus ensureGreen() {
        ClusterHealthResponse actionGet = client().admin().cluster()
            .health(Requests.clusterHealthRequest().waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        return actionGet.getStatus();
    }

    /**
     * Waits until all nodes have no pending tasks.
     */
    public void waitNoPendingTasksOnAll() throws Exception {
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
        assertBusy(new Runnable() {
            @Override
            public void run() {
                for (Client client : clients()) {
                    PendingClusterTasksResponse pendingTasks = client.admin().cluster().preparePendingClusterTasks().setLocal(true).get();
                    assertThat("client " + client + " still has pending tasks " + pendingTasks.prettyPrint(), pendingTasks, Matchers.emptyIterable());
                }
            }
        });
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
    }

    /**
     * Waits for all relocating shards to become active using the cluster health API.
     */
    public ClusterHealthStatus waitForRelocation() {
        return waitForRelocation(null);
    }

    /**
     * Waits for all relocating shards to become active and the cluster has reached the given health status
     * using the cluster health API.
     */
    public ClusterHealthStatus waitForRelocation(ClusterHealthStatus status) {
        ClusterHealthRequest request = Requests.clusterHealthRequest().waitForRelocatingShards(0);
        if (status != null) {
            request.waitForStatus(status);
        }
        ClusterHealthResponse actionGet = client().admin().cluster()
            .health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("waitForRelocation timed out (status={}), cluster state:\n{}\n{}", status, client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        if (status != null) {
            assertThat(actionGet.getStatus(), equalTo(status));
        }
        return actionGet.getStatus();
    }

    /**
     * Ensures the cluster has a yellow state via the cluster health API.
     */
    public ClusterHealthStatus ensureYellow() {
        ClusterHealthResponse actionGet = client().admin().cluster()
            .health(Requests.clusterHealthRequest().waitForRelocatingShards(0).waitForYellowStatus().waitForEvents(Priority.LANGUID)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureYellow timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for yellow", actionGet.isTimedOut(), equalTo(false));
        }
        return actionGet.getStatus();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   client().prepareIndex(index, type).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(String index, String type, XContentBuilder source) {
        return client().prepareIndex(index, type).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   client().prepareIndex(index, type).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(String index, String type, String id, Map<String, Object> source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   client().prepareGet(index, type, id).execute().actionGet();
     * </pre>
     */
    protected final GetResponse get(String index, String type, String id) {
        return client().prepareGet(index, type, id).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(String index, String type, String id, XContentBuilder source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(String index, String type, String id, Object... source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    /**
     * Waits for relocations and refreshes all indices in the cluster.
     * @see #waitForRelocation()
     */
    public final RefreshResponse refresh() {
        return refresh(client());
    }

    public RefreshResponse refresh(Client client) {
        waitForRelocation();
        return client.admin().indices().prepareRefresh().execute().actionGet();
    }

    /**
     * Flushes and refreshes all indices in the cluster
     */
    protected final void flushAndRefresh() {
        flush(true);
        refresh();
    }

    /**
     * Flushes all indices in the cluster
     */
    protected final FlushResponse flush() {
        return flush(true);
    }

    private FlushResponse flush(boolean ignoreNotAllowed) {
        waitForRelocation();
        FlushResponse actionGet = client().admin().indices().prepareFlush().execute().actionGet();
        if (ignoreNotAllowed) {
            for (ShardOperationFailedException failure : actionGet.getShardFailures()) {
                if (!failure.reason().contains("FlushNotAllowed")) {
                    assert false : "unexpected failed flush " + failure.reason();
                }
            }
        } else {
            assertNoFailures(actionGet);
        }
        return actionGet;
    }

    /**
     * Waits for all relocations and optimized all indices in the cluster to 1 segment.
     */
    protected OptimizeResponse optimize() {
        waitForRelocation();
        OptimizeResponse actionGet = client().admin().indices().prepareOptimize().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    /**
     * Returns <code>true</code> iff the given index exists otherwise <code>false</code>
     */
    protected boolean indexExists(String index) {
        IndicesExistsResponse actionGet = client().admin().indices().prepareExists(index).execute().actionGet();
        return actionGet.isExists();
    }

    /**
     * Returns a random admin client. This client can either be a node or a transport client pointing to any of
     * the nodes in the cluster.
     */
    protected AdminClient admin() {
        return client().admin();
    }

    /**
     * Indexes the given {@link IndexRequestBuilder} instances randomly. It shuffles the given builders and either
     * indexes they in a blocking or async fashion. This is very useful to catch problems that relate to internal document
     * ids or index segment creations. Some features might have bug when a given document is the first or the last in a
     * segment or if only one document is in a segment etc. This method prevents issues like this by randomizing the index
     * layout.
     */
    public void indexRandom(boolean forceRefresh, IndexRequestBuilder... builders) throws InterruptedException, ExecutionException {
        if (builders.length == 0) {
            return;
        }

        Random random = getRandom();
        Set<String> indicesSet = new HashSet<String>();
        for (int i = 0; i < builders.length; i++) {
            indicesSet.add(builders[i].request().index());
        }
        final String[] indices = indicesSet.toArray(new String[0]);
        List<IndexRequestBuilder> list = Arrays.asList(builders);
        Collections.shuffle(list, random);
        final CopyOnWriteArrayList<Tuple<IndexRequestBuilder, Throwable>> errors = new CopyOnWriteArrayList<Tuple<IndexRequestBuilder, Throwable>>();
        List<CountDownLatch> latches = new ArrayList<CountDownLatch>();
        if (frequently()) {
            logger.info("Index [{}] docs async: [{}] bulk: [{}]", list.size(), true, false);
            final CountDownLatch latch = new CountDownLatch(list.size());
            latches.add(latch);
            for (IndexRequestBuilder indexRequestBuilder : list) {
                indexRequestBuilder.execute(new PayloadLatchedActionListener<IndexResponse, IndexRequestBuilder>(indexRequestBuilder, latch, errors));
                if (rarely()) {
                    if (rarely()) {
                        client().admin().indices().prepareRefresh(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen())
                                .execute(new LatchedActionListener<RefreshResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareFlush(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen())
                                .execute(new LatchedActionListener<FlushResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareOptimize(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen())
                                .setMaxNumSegments(between(1, 10)).setFlush(random.nextBoolean())
                                .execute(new LatchedActionListener<OptimizeResponse>(newLatch(latches)));
                    }
                }
            }

        } else if (randomBoolean()) {
            logger.info("Index [{}] docs async: [{}] bulk: [{}]", list.size(), false, false);
            for (IndexRequestBuilder indexRequestBuilder : list) {
                indexRequestBuilder.execute().actionGet();
                if (rarely()) {
                    if (rarely()) {
                        client().admin().indices().prepareRefresh(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen())
                                .execute(new LatchedActionListener<RefreshResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareFlush(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen())
                                .execute(new LatchedActionListener<FlushResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareOptimize(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen())
                                .setMaxNumSegments(between(1, 10)).setFlush(random.nextBoolean())
                                .execute(new LatchedActionListener<OptimizeResponse>(newLatch(latches)));
                    }
                }
            }
        } else {
            logger.info("Index [{}] docs async: [{}] bulk: [{}]", list.size(), false, true);
            BulkRequestBuilder bulkBuilder = client().prepareBulk();
            for (IndexRequestBuilder indexRequestBuilder : list) {
                bulkBuilder.add(indexRequestBuilder);
            }
            BulkResponse actionGet = bulkBuilder.execute().actionGet();
            assertThat(actionGet.hasFailures() ? actionGet.buildFailureMessage() : "", actionGet.hasFailures(), equalTo(false));
        }
        for (CountDownLatch countDownLatch : latches) {
            countDownLatch.await();
        }
        final List<Throwable> actualErrors = new ArrayList<Throwable>();
        for (Tuple<IndexRequestBuilder, Throwable> tuple : errors) {
            if (ExceptionsHelper.unwrapCause(tuple.v2()) instanceof EsRejectedExecutionException) {
                tuple.v1().execute().actionGet(); // re-index if rejected
            } else {
                actualErrors.add(tuple.v2());
            }
        }
        assertThat(actualErrors, emptyIterable());
        if (forceRefresh) {
            assertNoFailures(client().admin().indices()
                    .prepareRefresh(indices)
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .execute().get());
        }
    }

    private static final CountDownLatch newLatch(List<CountDownLatch> latches) {
        CountDownLatch l = new CountDownLatch(1);
        latches.add(l);
        return l;
    }

    private class LatchedActionListener<Response> implements ActionListener<Response> {
        private final CountDownLatch latch;

        public LatchedActionListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public final void onResponse(Response response) {
            latch.countDown();
        }

        @Override
        public final void onFailure(Throwable t) {
            try {
                logger.info("Action Failed", t);
                addError(t);
            } finally {
                latch.countDown();
            }
        }

        protected void addError(Throwable t) {
        }

    }

    private class PayloadLatchedActionListener<Response, T> extends LatchedActionListener<Response> {
        private final CopyOnWriteArrayList<Tuple<T, Throwable>> errors;
        private final T builder;

        public PayloadLatchedActionListener(T builder, CountDownLatch latch, CopyOnWriteArrayList<Tuple<T, Throwable>> errors) {
            super(latch);
            this.errors = errors;
            this.builder = builder;
        }

        protected void addError(Throwable t) {
            errors.add(new Tuple<T, Throwable>(builder, t));
        }

    }

    /**
     * Clears the given scroll Ids
     */
    public void clearScroll(String... scrollIds) {
        ClearScrollResponse clearResponse = client().prepareClearScroll()
            .setScrollIds(Arrays.asList(scrollIds)).get();
        assertThat(clearResponse.isSucceeded(), equalTo(true));
    }


    /**
     * The scope of a test cluster used together with
     * {@link ClusterScope} annonations on {@link io.crate.test.integration.CrateIntegrationTest} subclasses.
     */
    public static enum Scope {
        /**
         * A globally shared cluster. This cluster doesn't allow modification of transient or persistent
         * cluster settings.
         */
        GLOBAL,
        /**
         * A cluster shared across all method in a single test suite
         */
        SUITE,
        /**
         * A test exclusive test cluster
         */
        TEST;
    }

    private ClusterScope getAnnotation(Class<?> clazz) {
        if (clazz == Object.class || clazz == CrateIntegrationTest.class) {
            return null;
        }
        ClusterScope annotation = clazz.getAnnotation(ClusterScope.class);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass());
    }

    private Scope getCurrentClusterScope() {
        ClusterScope annotation = getAnnotation(this.getClass());
        // if we are not annotated assume global!
        return annotation == null ? Scope.GLOBAL : annotation.scope();
    }

    private int getNumNodes() {
        ClusterScope annotation = getAnnotation(this.getClass());
        return annotation == null ? -1 : annotation.numNodes();
    }

    /**
     * This method is used to obtain settings for the <tt>Nth</tt> node in the cluster.
     * Nodes in this cluster are associated with an ordinal number such that nodes can
     * be started with specific configurations. This method might be called multiple
     * times with the same ordinal and is expected to return the same value for each invocation.
     * In other words subclasses must ensure this method is idempotent.
     */
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.EMPTY;
    }

    private CrateTestCluster buildTestCluster(Scope scope) {
        long currentClusterSeed = randomLong();
        int numNodes = getNumNodes();
        NodeSettingsSource nodeSettingsSource;
        if (numNodes > 0) {
            NodeSettingsSource.Immutable.Builder nodesSettings = NodeSettingsSource.Immutable.builder();
            for (int i = 0; i < numNodes; i++) {
                nodesSettings.set(i, nodeSettings(i));
            }
            nodeSettingsSource = nodesSettings.build();
        } else {
            nodeSettingsSource = new NodeSettingsSource() {
                @Override
                public Settings settings(int nodeOrdinal) {
                    return nodeSettings(nodeOrdinal);
                }
            };
        }
        String mode = "network";
        if (scope == Scope.TEST || scope == Scope.SUITE) {
            mode = "local";
        }
        return new CrateTestCluster(
            currentClusterSeed,
            numNodes,
            mode,
            CrateTestCluster.clusterName(scope.name(), Integer.toString(CHILD_JVM_ID), currentClusterSeed),
            nodeSettingsSource);
    }

    /**
     * Defines a cluster scope for a {@link io.crate.test.integration.CrateIntegrationTest} subclass.
     * By default if no {@link ClusterScope} annotation is present {@link Scope#GLOBAL} is used
     * together with randomly chosen settings like number of nodes etc.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface ClusterScope {
        /**
         * Returns the scope. {@link Scope#GLOBAL} is default.
         */
        Scope scope() default Scope.GLOBAL;

        /**
         * Returns the number of nodes in the cluster. Default is <tt>-1</tt> which means
         * a random number of nodes but at least <code>2</code></tt> is used./
         */
        int numNodes() default -1;
    }

    private static long clusterSeed() {
        String property = System.getProperty(TESTS_CLUSTER_SEED);
        if (property == null || property.isEmpty()) {
            return System.nanoTime();
        }
        return SeedUtils.parseSeed(property);
    }
}
