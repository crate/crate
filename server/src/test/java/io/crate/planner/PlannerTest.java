package io.crate.planner;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.elasticsearch.common.Randomness;
import org.junit.Before;
import org.junit.Test;

import io.crate.action.sql.SessionContext;
import io.crate.data.Row1;
import io.crate.exceptions.ConversionException;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RoutingProvider;
import io.crate.planner.node.ddl.UpdateSettingsPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Assignment;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import static org.mockito.Mockito.mock;

public class PlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).build();
    }

    @Test
    public void testSetPlan() throws Exception {
        UpdateSettingsPlan plan = e.plan("set GLOBAL PERSISTENT stats.jobs_log_size=1024");

        assertThat(plan.settings(), contains(new Assignment<>(Literal.of("stats.jobs_log_size"), List.of(Literal.of(1024)))));
        assertThat(plan.isPersistent(), is(true));

        plan = e.plan("set GLOBAL TRANSIENT stats.enabled=false,stats.jobs_log_size=0");

        assertThat(plan.settings().size(), is(2));
        assertThat(plan.isPersistent(), is(false));
    }

    @Test
    public void testSetSessionTransactionModeIsNoopPlan() throws Exception {
        Plan plan = e.plan("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testExecutionPhaseIdSequence() throws Exception {
        PlannerContext plannerContext = new PlannerContext(
            clusterService.state(),
            new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList()),
            UUID.randomUUID(),
            new CoordinatorTxnCtx(SessionContext.systemSessionContext()),
            e.nodeCtx,
            0,
            null
        );

        assertThat(plannerContext.nextExecutionPhaseId(), is(0));
        assertThat(plannerContext.nextExecutionPhaseId(), is(1));
    }

    @Test
    public void testDeallocate() {
        assertThat(e.plan("deallocate all"), instanceOf(NoopPlan.class));
        assertThat(e.plan("deallocate test_prep_stmt"), instanceOf(NoopPlan.class));
    }

    @Test
    public void test_invalid_any_param_leads_to_clear_error_message() throws Exception {
        LogicalPlan plan = e.logicalPlan("select name = ANY(?) from sys.cluster");
        Asserts.assertThrows(
            () -> {
                LogicalPlanner.getNodeOperationTree(
                    plan,
                    mock(DependencyCarrier.class),
                    e.getPlannerContext(clusterService.state()),
                    new Row1("foo"),
                    SubQueryResults.EMPTY
                );
            },
            ConversionException.class,
            "Cannot cast value `foo` to type `text_array`"
        );
    }
}
