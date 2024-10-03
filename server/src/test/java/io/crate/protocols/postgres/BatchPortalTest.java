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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;

import io.crate.session.BaseResultReceiver;
import io.crate.session.Session;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class BatchPortalTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testEachStatementReceivesCorrectParams() throws Throwable {

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
        Planner plannerMock = mock(Planner.class);
        when(plannerMock.plan(Mockito.any(), Mockito.any())).thenReturn(insertPlan);
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService)
            .setPlanner(plannerMock)
            .build()
            .addTable("create table t1 (x int)");

        Session session = sqlExecutor.createSession();

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

        assertThat(s1Rows).containsExactly(new Object[0]);
        assertThat(s2Rows).containsExactly(new Object[] { 2 });
    }
}
