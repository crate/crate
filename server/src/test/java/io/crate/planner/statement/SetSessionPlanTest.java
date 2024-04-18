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

package io.crate.planner.statement;

import static io.crate.planner.statement.SetSessionPlan.ensureNotGlobalSetting;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import io.crate.action.sql.Cursors;
import io.crate.data.Row;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.protocols.postgres.TransactionState;
import io.crate.sql.tree.Assignment;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class SetSessionPlanTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testSetSessionInvalidSetting() throws Exception {
        assertThatThrownBy(() -> ensureNotGlobalSetting("stats.operations_log_size"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("GLOBAL Cluster setting 'stats.operations_log_size' cannot be used with SET SESSION / LOCAL");
    }

    @Test
    public void test_set_session_allows_settings_that_exist_as_both_global_and_session_setting() throws Exception {
        Assignment<Symbol> assignment = new Assignment<Symbol>(Literal.of("statement_timeout"), Literal.of(10));
        SetSessionPlan setSessionPlan = new SetSessionPlan(List.of(assignment), new SessionSettingRegistry(Set.of()));
        TestingRowConsumer consumer = new TestingRowConsumer();
        NodeContext nodeCtx = new NodeContext(new Functions(Map.of()), () -> List.of());
        setSessionPlan.execute(
            mock(DependencyCarrier.class),
            new PlannerContext(
                clusterService.state(),
                new RoutingProvider(1, List.of()),
                UUID.randomUUID(),
                CoordinatorTxnCtx.systemTransactionContext(),
                nodeCtx,
                -1,
                Row.EMPTY,
                new Cursors(),
                TransactionState.IDLE,
                mock(PlanStats.class),
                (a,b) -> a
            ),
            consumer,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
        assertThat(consumer.getBucket())
            .as("Must not raise an exception")
            .isEmpty();
    }
}
