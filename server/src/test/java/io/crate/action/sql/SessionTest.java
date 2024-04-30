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

package io.crate.action.sql;

import static io.crate.testing.Asserts.assertThat;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Answers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.crate.analyze.AnalyzedStatement;
import io.crate.common.unit.TimeValue;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.jobs.kill.KillJobsNodeAction;
import io.crate.execution.jobs.kill.KillJobsNodeRequest;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.protocols.postgres.Portal;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class SessionTest extends CrateDummyClusterServiceUnitTest {


    @Test
    public void test_out_of_bounds_getParamType_fails() throws Exception {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();
        try (Session session = sqlExecutor.createSession()) {
            session.parse("S_1", "Select 1 + ? + ?;", Collections.emptyList());
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> session.getParamType("S_1", 3),
                "foo"
            );
        }
    }

    @Test
    public void test_getParamType_returns_types_infered_from_statement() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();
        try (Session session = sqlExecutor.createSession()) {

            session.parse("S_1", "Select 1 + ? + ?;", Collections.emptyList());
            assertThat(session.getParamType("S_1", 0)).isEqualTo(DataTypes.INTEGER);
            assertThat(session.getParamType("S_1", 1)).isEqualTo(DataTypes.INTEGER);

            DescribeResult describe = session.describe('S', "S_1");
            assertThat(describe.getParameters()).isEqualTo(new DataType[]{DataTypes.INTEGER, DataTypes.INTEGER});

            assertThat(session.getParamType("S_1", 0)).isEqualTo(DataTypes.INTEGER);
            assertThat(session.getParamType("S_1", 1)).isEqualTo(DataTypes.INTEGER);
        }
    }

    @Test
    public void test_select_query_executed_on_session_execute_method() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();
        Session session = Mockito.spy(sqlExecutor.createSession());

        var activeExecutionFuture = CompletableFuture.completedFuture(null);
        doReturn(activeExecutionFuture)
            .when(session)
            .singleExec(any(Portal.class), any(ResultReceiver.class), anyInt());

        session.parse("S_1", "select name from sys.cluster;", List.of());
        session.bind("Portal", "S_1", Collections.emptyList(), null);
        session.describe('S', "S_1");
        session.execute("Portal", 1, new BaseResultReceiver());

        assertThat(session.portals.size()).isEqualTo(1);
        assertThat(session.preparedStatements.size()).isEqualTo(1);
        assertThat(session.deferredExecutionsByStmt.size()).isEqualTo(0);
        assertThat(session.activeExecution).isEqualTo(activeExecutionFuture);

        session.close();

        assertThat(session.portals.size()).isEqualTo(0);
        assertThat(session.preparedStatements.size()).isEqualTo(0);
        assertThat(session.deferredExecutionsByStmt.size()).isEqualTo(0);
        assertThat(session.activeExecution).isNull();
    }


    @Test
    public void test_flush_triggers_deferred_executions_and_sets_active_execution() throws Exception {
        Planner planner = mock(Planner.class, Answers.RETURNS_MOCKS);
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).setPlanner(planner)
            .build()
            .addTable("create table users (name text)");
        Session session = Mockito.spy(sqlExecutor.createSession());
        session.parse("", "insert into users (name) values (?)", List.of());
        session.bind("", "", List.of("Arthur"), null);
        session.execute("", -1, new BaseResultReceiver());
        assertThat(session.deferredExecutionsByStmt.size()).isEqualTo(1);
        session.flush();
        var activeExecution = session.activeExecution;
        assertThat(activeExecution).isNotNull();

        CompletableFuture<?> sync = session.sync();
        assertThat(sync).isSameAs(activeExecution);
    }

    @Test
    public void testProperCleanupOnSessionClose() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();

        Session session = sqlExecutor.createSession();

        session.parse("S_1", "select name from sys.cluster;", Collections.emptyList());
        session.bind("Portal", "S_1", Collections.emptyList(), null);
        session.describe('S', "S_1");

        session.parse("S_2", "select id from sys.cluster", Collections.emptyList());
        session.bind("", "S_2", Collections.emptyList(), null);
        session.describe('S', "S_2");

        assertThat(session.portals.size()).isEqualTo(2);
        assertThat(session.preparedStatements.size()).isEqualTo(2);
        assertThat(session.deferredExecutionsByStmt.size()).isEqualTo(0);

        session.close();

        assertThat(session.portals.size()).isEqualTo(0);
        assertThat(session.preparedStatements.size()).isEqualTo(0);
        assertThat(session.deferredExecutionsByStmt.size()).isEqualTo(0);
        assertThat(session.activeExecution).isNull();
    }

    @Test
    public void testDeallocateAllClearsAllPortalsAndPreparedStatements() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();

        DependencyCarrier executor = mock(DependencyCarrier.class);
        when(executor.threadPool()).thenReturn(mock(ThreadPool.class));
        Session session = sqlExecutor.createSession();

        session.parse("S_1", "select * from sys.cluster;", Collections.emptyList());
        session.bind("Portal", "S_1", Collections.emptyList(), null);
        session.describe('S', "S_1");

        session.parse("S_2", "DEALLOCATE ALL;", Collections.emptyList());
        session.bind("", "S_2", Collections.emptyList(), null);
        session.execute("", 0, new BaseResultReceiver());

        assertThat(session.portals).hasSizeGreaterThan(0);
        assertThat(session.preparedStatements).isEmpty();
    }

    @Test
    public void testDeallocatePreparedStatementClearsPreparedStatement() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();

        DependencyCarrier executor = mock(DependencyCarrier.class);
        when(executor.threadPool()).thenReturn(mock(ThreadPool.class));
        Session session = sqlExecutor.createSession();

        session.parse("test_prep_stmt", "select * from sys.cluster;", Collections.emptyList());
        session.bind("Portal", "test_prep_stmt", Collections.emptyList(), null);
        session.describe('S', "test_prep_stmt");

        session.parse("stmt", "DEALLOCATE test_prep_stmt;", Collections.emptyList());
        session.bind("", "stmt", Collections.emptyList(), null);
        session.execute("", 0, new BaseResultReceiver());

        assertThat(session.portals).hasSizeGreaterThan(0);
        assertThat(session.preparedStatements.size()).isEqualTo(1);
        assertThat(session.preparedStatements.get("stmt").rawStatement()).isEqualTo("DEALLOCATE test_prep_stmt;");
    }

    @Test
    public void test_closing_a_statement_closes_related_portals() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();
        Session session = sqlExecutor.createSession();

        session.parse("S_1", "SELECT 1", List.of());
        session.bind("P_1", "S_1", List.of(), null);

        assertThat(session.portals.size()).isEqualTo(1);
        assertThat(session.preparedStatements.size()).isEqualTo(1);

        session.close((byte) 'S', "S_1");
        assertThat(session.portals).isEmpty();
        assertThat(session.preparedStatements).isEmpty();
    }

    @Test
    public void test_can_describe_cursor_created_using_declare() throws Exception {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();
        Session session = sqlExecutor.createSession();
        session.parse("", "DECLARE c1 NO SCROLL CURSOR FOR select 1", List.of());
        session.bind("", "", List.of(), null);
        session.execute("", 0, new BaseResultReceiver());

        DescribeResult describe = session.describe('P', "c1");
        assertThat(describe.getFields()).satisfiesExactly(
            x -> assertThat(x).isLiteral(1, DataTypes.INTEGER)
        );

        assertThat(session.portals).containsOnlyKeys("", "c1");

        session.parse("", "CLOSE c1", List.of());
        session.bind("", "", List.of(), null);
        session.execute("", 0, new BaseResultReceiver());

        assertThat(session.portals).containsOnlyKeys("");
    }

    @Test
    public void test_discard_all_discards_all_portals_and_prepared_statements() throws Exception {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();
        Session session = sqlExecutor.createSession();

        session.parse("S_1", "SELECT 1", List.of());
        session.bind("P_1", "S_1", List.of(), null);

        assertThat(session.portals.size()).isEqualTo(1);
        assertThat(session.preparedStatements.size()).isEqualTo(1);

        session.parse("stmt", "DISCARD ALL", Collections.emptyList());
        session.bind("", "stmt", Collections.emptyList(), null);
        session.execute("", 0, new BaseResultReceiver());

        assertThat(session.portals).isEmpty();
        assertThat(session.preparedStatements).isEmpty();
    }

    @Test
    public void test_bulk_operations_result_in_jobslog_entries() throws Exception {
        Planner planner = mock(Planner.class, Answers.RETURNS_MOCKS);
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService)
            .setPlanner(planner)
            .build()
            .addTable("create table t1 (x int)");
        sqlExecutor.jobsLogsEnabled = true;
        Session session = sqlExecutor.createSession();
        when(planner.plan(any(AnalyzedStatement.class), any(PlannerContext.class)))
            .thenReturn(
                new Plan() {
                    @Override
                    public StatementType type() {
                        return StatementType.INSERT;
                    }

                    @Override
                    public void executeOrFail(DependencyCarrier dependencies,
                                              PlannerContext plannerContext,
                                              RowConsumer consumer,
                                              Row params,
                                              SubQueryResults subQueryResults) throws Exception {
                        // Make sure `quickExec()` below completes, and its job is moved to jobs_log
                        consumer.completionFuture().complete(null);
                    }

                    @Override
                    public List<CompletableFuture<Long>> executeBulk(DependencyCarrier executor,
                                                                     PlannerContext plannerContext,
                                                                     List<Row> bulkParams,
                                                                     SubQueryResults subQueryResults) {
                        // Do another execution to overwrite `mostRecentJobID`
                        session.quickExec("SELECT 1", new BaseResultReceiver(), null);
                        return List.of(completedFuture(1L), completedFuture(1L));
                    }
                }
            );

        session.parse("S_1", "INSERT INTO t1 (x) VALUES (1)", List.of());
        session.bind("P_1", "S_1", List.of(), null);
        session.execute("P_1", 0, new BaseResultReceiver());

        session.bind("P_1", "S_1", List.of(), null);
        session.execute("P_1", 0, new BaseResultReceiver());

        session.sync().get(5, TimeUnit.SECONDS);
        assertThat(sqlExecutor.jobsLogs.metrics().iterator().next().totalCount()).isEqualTo(2L);
        assertThat(sqlExecutor.jobsLogs.activeJobs().iterator().hasNext()).isFalse();
    }

    @Test
    public void test_kills_query_if_not_completed_within_statement_timeout() throws Exception {
        Planner planner = mock(Planner.class, Answers.RETURNS_MOCKS);
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService)
            .setPlanner(planner)
            .build();
        when(planner.plan(any(AnalyzedStatement.class), any(PlannerContext.class)))
            .thenReturn(
                new Plan() {
                    @Override
                    public StatementType type() {
                        return StatementType.INSERT;
                    }

                    @Override
                    public void executeOrFail(DependencyCarrier dependencies,
                                              PlannerContext plannerContext,
                                              RowConsumer consumer,
                                              Row params,
                                              SubQueryResults subQueryResults) throws Exception {
                    }

                    @Override
                    public List<CompletableFuture<Long>> executeBulk(DependencyCarrier executor,
                                                                     PlannerContext plannerContext,
                                                                     List<Row> bulkParams,
                                                                     SubQueryResults subQueryResults) {
                        return List.of(new CompletableFuture<>());
                    }
                }
            );

        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        DependencyCarrier dependencies = sqlExecutor.dependencyMock;
        when(dependencies.scheduler()).thenReturn(scheduler);

        Answer<ScheduledFuture<?>> triggerRunnable = new Answer<ScheduledFuture<?>>() {

            @Override
            public ScheduledFuture<?> answer(InvocationOnMock invocation) throws Throwable {
                Runnable runnable = invocation.getArgument(0);
                runnable.run();
                return null;
            }
        };
        when(scheduler.schedule(any(Runnable.class), Mockito.anyLong(), any(TimeUnit.class)))
            .thenAnswer(triggerRunnable);
        ElasticsearchClient client = mock(ElasticsearchClient.class);
        when(dependencies.client()).thenReturn(client);

        Session session = sqlExecutor.createSession();
        session.sessionSettings().statementTimeout(TimeValue.timeValueMillis(10));

        session.parse("S_1", "SELECT 1", List.of());
        session.bind("P_1", "S_1", List.of(), null);
        session.execute("P_1", 0, new BaseResultReceiver());
        session.sync();

        verify(client, times(1))
            .execute(Mockito.eq(KillJobsNodeAction.INSTANCE), any(KillJobsNodeRequest.class));
    }
}
