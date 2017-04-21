package io.crate.planner;

import com.carrotsearch.hppc.IntSet;
import com.google.common.collect.Multimap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.sql.tree.LongLiteral;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

import static io.crate.analyze.TableDefinitions.shardRouting;
import static io.crate.analyze.TableDefinitions.shardRoutingForReplicas;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

@SuppressWarnings("ConstantConditions")
public class PlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private EvaluatingNormalizer normalizer;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).build();
        normalizer = EvaluatingNormalizer.functionOnlyNormalizer(e.functions(), ReplaceMode.COPY);
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
    public void testIndices() throws Exception {
        TableIdent custom = new TableIdent("custom", "table");
        String[] indices = Planner.indices(TestingTableInfo.builder(custom, shardRouting("t1")).add("id", DataTypes.INTEGER, null).build(), WhereClause.MATCH_ALL);
        assertThat(indices, arrayContainingInAnyOrder("custom.table"));

        indices = Planner.indices(TestingTableInfo.builder(new TableIdent(null, "table"), shardRouting("t1")).add("id", DataTypes.INTEGER, null).build(), WhereClause.MATCH_ALL);
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
    public void testBuildReaderAllocations() throws Exception {
        TableIdent custom = new TableIdent("custom", "t1");
        TableInfo tableInfo = TestingTableInfo.builder(
            custom, shardRouting("t1")).add("id", DataTypes.INTEGER, null).build();
        Planner.Context plannerContext = new Planner.Context(
            e.planner,
            clusterService,
            UUID.randomUUID(),
            null,
            normalizer,
            new TransactionContext(SessionContext.SYSTEM_SESSION),
            0,
            0);
        plannerContext.allocateRouting(tableInfo, WhereClause.MATCH_ALL, null);

        ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();

        assertThat(readerAllocations.indices().size(), is(1));
        assertThat(readerAllocations.indices().get(0), is("t1"));
        assertThat(readerAllocations.nodeReaders().size(), is(2));

        IntSet n1 = readerAllocations.nodeReaders().get("nodeOne");
        assertThat(n1.size(), is(2));
        assertTrue(n1.contains(1));
        assertTrue(n1.contains(2));

        IntSet n2 = readerAllocations.nodeReaders().get("nodeTwo");
        assertThat(n2.size(), is(2));
        assertTrue(n2.contains(3));
        assertTrue(n2.contains(4));

        assertThat(readerAllocations.bases().get("t1"), is(0));

        // allocations must stay same on multiple calls
        ReaderAllocations readerAllocations2 = plannerContext.buildReaderAllocations();
        assertThat(readerAllocations, is(readerAllocations2));
    }

    @Test
    public void testAllocateRouting() throws Exception {
        TableIdent custom = new TableIdent("custom", "t1");
        TableInfo tableInfo1 =
            TestingTableInfo.builder(custom, shardRouting("t1")).add("id", DataTypes.INTEGER, null).build();
        TableInfo tableInfo2 =
            TestingTableInfo.builder(custom, shardRoutingForReplicas("t1")).add("id", DataTypes.INTEGER, null).build();
        Planner.Context plannerContext = new Planner.Context(
            e.planner,
            clusterService,
            UUID.randomUUID(),
            null,
            normalizer,
            new TransactionContext(SessionContext.SYSTEM_SESSION),
            0,
            0);

        WhereClause whereClause = new WhereClause(
            new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME,
                                  Arrays.asList(DataTypes.INTEGER, DataTypes.INTEGER)),
                DataTypes.BOOLEAN),
                         Arrays.asList(tableInfo1.getReference(new ColumnIdent("id")), Literal.of(2))
            ));

        plannerContext.allocateRouting(tableInfo1, WhereClause.MATCH_ALL, null);
        plannerContext.allocateRouting(tableInfo2, whereClause, null);

        // 2 routing allocations with different where clause must result in 2 allocated routings
        java.lang.reflect.Field tableRoutings = Planner.Context.class.getDeclaredField("tableRoutings");
        tableRoutings.setAccessible(true);
        Multimap<TableIdent, Planner.TableRouting> routing =
            (Multimap<TableIdent, Planner.TableRouting>) tableRoutings.get(plannerContext);
        assertThat(routing.size(), is(2));

        // The routings must be the same after merging the locations
        Iterator<Planner.TableRouting> iterator = routing.values().iterator();
        Routing routing1 = iterator.next().routing;
        Routing routing2 = iterator.next().routing;
        assertThat(routing1, is(routing2));
    }

    @Test
    public void testExecutionPhaseIdSequence() throws Exception {
        Planner.Context plannerContext = new Planner.Context(
            e.planner,
            clusterService,
            UUID.randomUUID(),
            null,
            normalizer,
            new TransactionContext(SessionContext.SYSTEM_SESSION.SYSTEM_SESSION),
            0,
            0);

        assertThat(plannerContext.nextExecutionPhaseId(), is(0));
        assertThat(plannerContext.nextExecutionPhaseId(), is(1));
    }

    @Test
    public void testKillPlanAll() throws Exception {
        KillPlan killPlan = e.plan("kill all");
        assertThat(killPlan, instanceOf(KillPlan.class));
        assertThat(killPlan.jobId(), notNullValue());
        assertThat(killPlan.jobToKill().isPresent(), is(false));
    }

    @Test
    public void testKillPlanJobs() throws Exception {
        KillPlan killJobsPlan = e.plan("kill '6a3d6fb6-1401-4333-933d-b38c9322fca7'");
        assertThat(killJobsPlan.jobId(), notNullValue());
        assertThat(killJobsPlan.jobToKill().get().toString(), is("6a3d6fb6-1401-4333-933d-b38c9322fca7"));
    }
}
