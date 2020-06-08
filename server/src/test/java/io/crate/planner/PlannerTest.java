package io.crate.planner;

import io.crate.action.sql.SessionContext;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RoutingProvider;
import io.crate.planner.node.ddl.UpdateSettingsPlan;
import io.crate.sql.tree.Assignment;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.common.Randomness;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

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
        Plan plan = e.plan("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testExecutionPhaseIdSequence() throws Exception {
        PlannerContext plannerContext = new PlannerContext(
            clusterService.state(),
            new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList()),
            UUID.randomUUID(),
            e.functions(),
            new CoordinatorTxnCtx(SessionContext.systemSessionContext()),
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
}
