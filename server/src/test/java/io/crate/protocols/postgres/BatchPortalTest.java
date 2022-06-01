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

package io.crate.protocols.postgres;

import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.Session;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.AnalyzedStatement;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.statistics.TableStats;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.user.StubUserManager;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.mockito.Answers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class BatchPortalTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testEachStatementReceivesCorrectParams() throws Throwable {

        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int)")
            .build();

        Plan insertPlan = new Plan() {
            @Override
            public StatementType type() {
                return StatementType.INSERT;
            }

            @Override
            public void executeOrFail(DependencyCarrier executor,
                                      PlannerContext plannerContext,
                                      RowConsumer consumer,
                                      Row params,
                                      SubQueryResults subQueryResults) {
                consumer.accept(InMemoryBatchIterator.of(params, null), null);
            }
        };
        Planner planner = new Planner(
            Settings.EMPTY,
            clusterService,
            sqlExecutor.nodeCtx,
            new TableStats(),
            null,
            null,
            sqlExecutor.schemas(),
            new StubUserManager(),
            mock(SessionSettingRegistry.class)
            ) {

            @Override
            public Plan plan(AnalyzedStatement analyzedStatement, PlannerContext plannerContext) {
                return insertPlan;
            }
        };

        DependencyCarrier executor = mock(DependencyCarrier.class, Answers.RETURNS_MOCKS);
        Session session = new Session(
            sqlExecutor.nodeCtx,
            sqlExecutor.analyzer,
            planner,
            new JobsLogs(() -> false),
            false,
            executor,
            SessionContext.systemSessionContext());

        session.parse("S_1", "insert into t1(x) values(1)", Collections.emptyList());
        session.bind("Portal", "S_1", Collections.emptyList(), null);
        final ArrayList<Object[]> s1Rows = new ArrayList<>();
        session.execute("Portal", 0, new BaseResultReceiver() {
            @Override
            public void setNextRow(Row row) {
                s1Rows.add(row.materialize());
            }
        });

        session.parse("S_2", "insert into t1(x) values(?)", Collections.emptyList());
        session.bind("Portal", "S_2", Collections.singletonList(2), null);
        final ArrayList<Object[]> s2Rows = new ArrayList<>();
        session.execute("Portal", 0, new BaseResultReceiver() {
            @Override
            public void setNextRow(Row row) {
                s2Rows.add(row.materialize());
            }
        });
        session.sync().get(5, TimeUnit.SECONDS);

        assertThat(s1Rows, contains(emptyArray()));
        assertThat(s2Rows, contains(arrayContaining(is(2))));
    }
}
