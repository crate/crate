/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.protocols.postgres;

import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.Session;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.AnalyzedStatement;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.TableStats;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class BatchPortalTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testEachStatementReceivesCorrectParams() throws IOException {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).enableDefaultTables().build();

        AtomicReference<Row> lastParams = new AtomicReference<>();
        Plan insertPlan = new Plan() {
            @Override
            public StatementType type() {
                return StatementType.INSERT;
            }

            @Override
            public void execute(DependencyCarrier executor,
                                PlannerContext plannerContext,
                                RowConsumer consumer,
                                Row params,
                                SubQueryResults subQueryResults) {
                lastParams.set(params);
            }
        };
        Planner planner = new Planner(Settings.EMPTY, clusterService, sqlExecutor.functions(), new TableStats(), () -> true) {
            @Override
            public Plan plan(AnalyzedStatement analyzedStatement, PlannerContext plannerContext) {
                return insertPlan;
            }
        };

        DependencyCarrier executor = mock(DependencyCarrier.class);
        Session session = new Session(
            sqlExecutor.analyzer,
            planner,
            new JobsLogs(() -> false),
            false,
            executor,
            SessionContext.systemSessionContext());

        session.parse("S_1", "insert into t1(x) values(1)", Collections.emptyList());
        session.bind("Portal", "S_1", Collections.emptyList(), null);
        session.execute("Portal", 0, new BaseResultReceiver());

        session.parse("S_2", "insert into t1(x) values(?)", Collections.emptyList());
        session.bind("Portal", "S_2", Collections.singletonList(2), null);
        session.execute("Portal", 0, new BaseResultReceiver());
        session.sync();

        assertThat(lastParams.get().get(0), is(2));
    }
}
