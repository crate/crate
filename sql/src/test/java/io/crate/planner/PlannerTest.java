package io.crate.planner;

import io.crate.action.sql.SessionContext;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RoutingProvider;
import io.crate.planner.node.ddl.UpdateSettingsPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.sql.tree.LongLiteral;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.common.Randomness;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
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
    public void testExecutionPhaseIdSequence() throws Exception {
        PlannerContext plannerContext = new PlannerContext(
            clusterService.state(),
            new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList()),
            UUID.randomUUID(),
            e.functions(),
            new CoordinatorTxnCtx(SessionContext.systemSessionContext()),
            0
        );

        assertThat(plannerContext.nextExecutionPhaseId(), is(0));
        assertThat(plannerContext.nextExecutionPhaseId(), is(1));
    }

    @Test
    public void testKillPlanAll() throws Exception {
        KillPlan killPlan = e.plan("kill all");
        assertThat(killPlan, instanceOf(KillPlan.class));
        assertThat(killPlan.jobId(), is(nullValue()));
    }

    @Test
    public void testKillPlanJobs() throws Exception {
        KillPlan killJobsPlan = e.plan("kill '6a3d6fb6-1401-4333-933d-b38c9322fca7'");
        assertThat(killJobsPlan.jobId().toString(), is("6a3d6fb6-1401-4333-933d-b38c9322fca7"));
    }

    @Test
    public void testDeallocate() {
        assertThat(e.plan("deallocate all"), instanceOf(NoopPlan.class));
        assertThat(e.plan("deallocate test_prep_stmt"), instanceOf(NoopPlan.class));
    }
}
