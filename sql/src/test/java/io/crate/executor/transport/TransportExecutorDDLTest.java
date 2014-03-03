package io.crate.executor.transport;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.Job;
import io.crate.planner.Plan;
import io.crate.planner.node.ddl.ESCreateIndexNode;
import io.crate.planner.node.ddl.ESDeleteIndexNode;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

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
}
