package io.crate.executor.transport;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.Job;
import io.crate.planner.Plan;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsNode;
import io.crate.planner.node.ddl.ESCreateIndexNode;
import io.crate.planner.node.ddl.ESDeleteIndexNode;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.cluster.settings.ClusterDynamicSettings;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class TransportExecutorDDLTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private TransportExecutor executor;

    @Before
    public void transportSetup() {
        executor = cluster().getInstance(TransportExecutor.class);
    }

    @Test
    public void testCreateIndexTask() throws Exception {
        Map indexMapping = ImmutableMap.of(
                "properties", ImmutableMap.of(
                    "id", ImmutableMap.builder()
                        .put("type", "integer")
                        .put("store", false)
                        .put("index", "not_analyzed")
                        .put("doc_values", true).build(),
                    "name", ImmutableMap.builder()
                        .put("type", "string")
                        .put("store", false)
                        .put("index", "not_analyzed")
                        .put("doc_values", true).build(),
                    "names", ImmutableMap.builder()
                        .put("type", "string")
                        .put("store", false)
                        .put("index", "not_analyzed")
                        .put("doc_values", false).build()
                    )
                );

        ESCreateIndexNode createTableNode = new ESCreateIndexNode(
                "test",
                ImmutableSettings.settingsBuilder()
                    .put("number_of_replicas", 0)
                    .put("number_of_shards", 2).build(),
                indexMapping
        );
        Plan plan = new Plan();
        plan.add(createTableNode);

        Job job = executor.newJob(plan);
        List<ListenableFuture<Object[][]>> futures = executor.execute(job);
        ListenableFuture<List<Object[][]>> listenableFuture = Futures.allAsList(futures);
        Object[][] objects = listenableFuture.get().get(0);

        assertThat((Long)objects[0][0], Matchers.is(1L));

        execute("select * from information_schema.tables where table_name = 'test' and number_of_replicas = 0 and number_of_shards = 2");
        assertThat(response.rowCount(), Matchers.is(1L));

        execute("select count(*) from information_schema.columns where table_name = 'test'");
        assertThat((Long)response.rows()[0][0], Matchers.is(3L));
    }

    @Test
    public void testDeleteIndexTask() throws Exception {
        execute("create table t (id integer primary key, name string)");
        ensureGreen();

        execute("select * from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount(), Matchers.is(1L));

        ESDeleteIndexNode deleteIndexNode = new ESDeleteIndexNode("t");
        Plan plan = new Plan();
        plan.add(deleteIndexNode);
        plan.expectsAffectedRows(true);

        Job job = executor.newJob(plan);
        List<ListenableFuture<Object[][]>> futures = executor.execute(job);
        ListenableFuture<List<Object[][]>> listenableFuture = Futures.allAsList(futures);
        Object[][] objects = listenableFuture.get().get(0);
        assertThat((Long)objects[0][0], Matchers.is(1L));

        execute("select * from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount(), Matchers.is(0L));
    }

    /**
     * this case should not happen as closed indices aren't listed as TableInfo
     * but if it does maby because of stale cluster state - validate behaviour here
     *
     * cannot prevent this task from deleting closed indices.
     */
    @Test
    public void testDeleteIndexTaskClosed() throws Exception {
        execute("create table t (id integer primary key, name string)");
        ensureGreen();
        assertTrue(client().admin().indices().prepareClose("t").execute().actionGet().isAcknowledged());

        ESDeleteIndexNode deleteIndexNode = new ESDeleteIndexNode("t");
        Plan plan = new Plan();
        plan.add(deleteIndexNode);
        plan.expectsAffectedRows(true);

        Job job = executor.newJob(plan);
        List<ListenableFuture<Object[][]>> futures = executor.execute(job);
        ListenableFuture<List<Object[][]>> listenableFuture = Futures.allAsList(futures);
        Object[][] objects = listenableFuture.get().get(0);
        assertThat((Long) objects[0][0], Matchers.is(1L));

        execute("select * from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount(), Matchers.is(0L));
    }

    @Test
    public void testClusterUpdateSettingsTask() throws Exception {
        final String persistentSetting = "persistent.level";
        final String transientSetting = "transient.uptime";
        // allow our settings to be updated (at all nodes)
        DynamicSettings dynamicSettings;
        for (int i = 0; i < cluster().size(); i++) {
            dynamicSettings = cluster().getInstance(Key.get(DynamicSettings.class, ClusterDynamicSettings.class), "node_" + i);
            dynamicSettings.addDynamicSetting(persistentSetting);
            dynamicSettings.addDynamicSetting(transientSetting);
        }

        // Update persistent only
        Settings persistentSettings = ImmutableSettings.builder()
                .put(persistentSetting, "panic")
                .build();

        ESClusterUpdateSettingsNode node = new ESClusterUpdateSettingsNode(persistentSettings);

        Plan plan = new Plan();
        plan.add(node);
        plan.expectsAffectedRows(true);

        Job job = executor.newJob(plan);
        List<ListenableFuture<Object[][]>> futures = executor.execute(job);
        ListenableFuture<List<Object[][]>> listenableFuture = Futures.allAsList(futures);
        Object[][] objects = listenableFuture.get().get(0);
        assertThat((Long) objects[0][0], Matchers.is(1L));
        assertEquals("panic", client().admin().cluster().prepareState().execute().actionGet().getState().metaData().persistentSettings().get(persistentSetting));

        // Update transient only
        Settings transientSettings = ImmutableSettings.builder()
                .put(transientSetting, "123")
                .build();

        node = new ESClusterUpdateSettingsNode(EMPTY_SETTINGS, transientSettings);

        plan = new Plan();
        plan.add(node);
        plan.expectsAffectedRows(true);

        job = executor.newJob(plan);
        futures = executor.execute(job);
        listenableFuture = Futures.allAsList(futures);
        objects = listenableFuture.get().get(0);
        assertThat((Long) objects[0][0], Matchers.is(1L));
        assertEquals("123", client().admin().cluster().prepareState().execute().actionGet().getState().metaData().transientSettings().get(transientSetting));

        // Update persistent & transient
        persistentSettings = ImmutableSettings.builder()
                .put(persistentSetting, "normal")
                .build();
        transientSettings = ImmutableSettings.builder()
                .put(transientSetting, "243")
                .build();

        node = new ESClusterUpdateSettingsNode(persistentSettings, transientSettings);

        plan = new Plan();
        plan.add(node);
        plan.expectsAffectedRows(true);

        job = executor.newJob(plan);
        futures = executor.execute(job);
        listenableFuture = Futures.allAsList(futures);
        objects = listenableFuture.get().get(0);
        assertThat((Long) objects[0][0], Matchers.is(1L));
        assertEquals("normal", client().admin().cluster().prepareState().execute().actionGet().getState().metaData().persistentSettings().get(persistentSetting));
        assertEquals("243", client().admin().cluster().prepareState().execute().actionGet().getState().metaData().transientSettings().get(transientSetting));
    }
}
