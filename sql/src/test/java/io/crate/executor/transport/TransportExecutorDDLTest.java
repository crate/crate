package io.crate.executor.transport;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.Job;
import io.crate.planner.Plan;
import io.crate.planner.node.ddl.ESCreateTableNode;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
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
        Map indexMapping = new HashMap<String, Object>();

        ESCreateTableNode createTableNode = new ESCreateTableNode(
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
    }
}
