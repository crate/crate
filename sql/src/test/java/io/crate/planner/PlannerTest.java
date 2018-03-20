package io.crate.planner;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.sql.tree.LongLiteral;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static io.crate.analyze.TableDefinitions.shardRouting;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("ConstantConditions")
public class PlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).build();
    }

    @Test
    public void testSetPlan() throws Exception {
        ESClusterUpdateSettingsPlan plan = e.plan("set GLOBAL PERSISTENT stats.jobs_log_size=1024");

        // set transient settings too when setting persistent ones
        assertThat(plan.transientSettings().get("stats.jobs_log_size").get(0), Is.is(new LongLiteral("1024")));
        assertThat(plan.persistentSettings().get("stats.jobs_log_size").get(0), Is.is(new LongLiteral("1024")));

        plan = e.plan("set GLOBAL TRANSIENT stats.enabled=false,stats.jobs_log_size=0");

        assertThat(plan.persistentSettings().size(), is(0));
        assertThat(plan.transientSettings().size(), is(2));
    }

    @Test
    public void testSetSessionTransactionModeIsNoopPlan() throws Exception {
        Plan plan = e.plan("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testIndices() throws Exception {
        TableIdent custom = new TableIdent("custom", "table");
        String[] indices = Planner.indices(TestingTableInfo.builder(custom, shardRouting("t1")).add("id", DataTypes.INTEGER, null).build(), WhereClause.MATCH_ALL);
        assertThat(indices, arrayContainingInAnyOrder("custom.table"));

        indices = Planner.indices(TestingTableInfo.builder(new TableIdent(Schemas.DOC_SCHEMA_NAME, "table"), shardRouting("t1")).add("id", DataTypes.INTEGER, null).build(), WhereClause.MATCH_ALL);
        assertThat(indices, arrayContainingInAnyOrder("table"));

        indices = Planner.indices(TestingTableInfo.builder(custom, shardRouting("t1"))
            .add("id", DataTypes.INTEGER, null)
            .add("date", DataTypes.TIMESTAMP, null, true)
            .addPartitions(new PartitionName(custom, Collections.singletonList(new BytesRef("0"))).asIndexName())
            .addPartitions(new PartitionName(custom, Collections.singletonList(new BytesRef("12345"))).asIndexName())
            .build(), WhereClause.MATCH_ALL);
        assertThat(indices, arrayContainingInAnyOrder("custom..partitioned.table.04130", "custom..partitioned.table.04332chj6gqg"));
    }

    @Test
    public void testExecutionPhaseIdSequence() throws Exception {
        PlannerContext plannerContext = new PlannerContext(
            clusterService.state(),
            new RoutingProvider(Randomness.get().nextInt(), new String[0]),
            UUID.randomUUID(),
            e.functions(),
            new TransactionContext(SessionContext.create()),
            0,
            0);

        assertThat(plannerContext.nextExecutionPhaseId(), is(0));
        assertThat(plannerContext.nextExecutionPhaseId(), is(1));
    }

    @Test
    public void testKillPlanAll() throws Exception {
        KillPlan killPlan = e.plan("kill all");
        assertThat(killPlan, instanceOf(KillPlan.class));
        assertThat(killPlan.jobToKill().isPresent(), is(false));
    }

    @Test
    public void testKillPlanJobs() throws Exception {
        KillPlan killJobsPlan = e.plan("kill '6a3d6fb6-1401-4333-933d-b38c9322fca7'");
        assertThat(killJobsPlan.jobToKill().get().toString(), is("6a3d6fb6-1401-4333-933d-b38c9322fca7"));
    }

    @Test
    public void testDeallocate() {
        assertThat(e.plan("deallocate all"), instanceOf(NoopPlan.class));
        assertThat(e.plan("deallocate test_prep_stmt"), instanceOf(NoopPlan.class));
    }
}
