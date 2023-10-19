/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertSQLError;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Before;
import org.junit.Test;

import io.crate.action.sql.Cursors;
import io.crate.data.Row1;
import io.crate.exceptions.ConversionException;
import io.crate.exceptions.UnavailableShardsException;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.node.ddl.UpdateSettingsPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.SubQueryResults;
import io.crate.protocols.postgres.TransactionState;
import io.crate.sql.tree.Assignment;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class PlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE doc.tbl(a int)")
            .build();
    }

    @Test
    public void testSetPlan() throws Exception {
        UpdateSettingsPlan plan = e.plan("set GLOBAL PERSISTENT stats.jobs_log_size=1024");

        assertThat(plan.settings()).containsExactly(new Assignment<>(Literal.of("stats.jobs_log_size"), List.of(Literal.of(1024))));
        assertThat(plan.isPersistent()).isTrue();

        plan = e.plan("set GLOBAL TRANSIENT stats.enabled=false,stats.jobs_log_size=0");

        assertThat(plan.settings()).hasSize(2);
        assertThat(plan.isPersistent()).isFalse();
    }

    @Test
    public void testSetSessionTransactionModeIsNoopPlan() throws Exception {
        Plan plan = e.plan("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        assertThat(plan).isExactlyInstanceOf(NoopPlan.class);
    }

    @Test
    public void testSetTimeZone() throws Exception {
        Plan plan = e.plan("SET TIME ZONE 'Europe/Vienna'");
        assertThat(plan).isExactlyInstanceOf(NoopPlan.class);
    }

    @Test
    public void testExecutionPhaseIdSequence() throws Exception {
        PlannerContext plannerContext = new PlannerContext(
            clusterService.state(),
            new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList()),
            UUID.randomUUID(),
            new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults()),
            e.nodeCtx,
            0,
            null,
            Cursors.EMPTY,
            TransactionState.IDLE,
            e.planStats()
        );

        assertThat(plannerContext.nextExecutionPhaseId()).isEqualTo(0);
        assertThat(plannerContext.nextExecutionPhaseId()).isEqualTo(1);
    }

    @Test
    public void testDeallocate() {
        var plan = e.plan("deallocate all");
        assertThat(plan).isExactlyInstanceOf(NoopPlan.class);
        plan = e.plan("deallocate test_prep_stmt");
        assertThat(plan).isExactlyInstanceOf(NoopPlan.class);
    }

    @Test
    public void test_invalid_any_param_leads_to_clear_error_message() throws Exception {
        LogicalPlan plan = e.logicalPlan("select name = ANY(?) from sys.cluster");
        assertThatThrownBy(() -> LogicalPlanner.getNodeOperationTree(
                plan,
                mock(DependencyCarrier.class),
                e.getPlannerContext(clusterService.state()),
                new Row1("foo"),
                SubQueryResults.EMPTY
            ))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessageContaining("Cannot cast value `foo` to type `text_array`");
    }

    @Test
    public void test_execution_exception_is_not_wrapped_in_logical_planner() {
        LogicalPlan plan = e.logicalPlan("select * from doc.tbl");
        var mockedPlannerCtx = mock(PlannerContext.class);
        when(mockedPlannerCtx.transactionContext()).thenThrow(
            new UnavailableShardsException(new ShardId("tbl", "uuid", 11)));

        assertSQLError(() -> LogicalPlanner.getNodeOperationTree(
                plan,
                mock(DependencyCarrier.class),
                mockedPlannerCtx,
                new Row1("foo"),
                SubQueryResults.EMPTY
            ))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5002)
            .hasMessageContaining("the shard 11 of table [tbl/uuid] is not available");
    }
}
