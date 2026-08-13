/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.node.ddl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.crate.analyze.AnalyzedCreateRole;
import io.crate.data.Row;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.role.StubRoleManager;
import io.crate.sql.tree.GenericProperties;

@RunWith(MockitoJUnitRunner.class)
public class CreateRolePlanTest {

    @Mock
    Client client;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    PlannerContext plannerCtx;
    TestingRowConsumer rowConsumer;
    @Mock
    DependencyCarrier dependencyCarrier;

    @Before
    public void setUp() {
        rowConsumer = new TestingRowConsumer();
    }

    @Test
    public void test_create_user_and_role_with_invalid_settings() {
        for (String userOrRole : List.of("USER", "ROLE")) {
            var plan = new CreateRolePlan(
                new AnalyzedCreateRole("new_user", true, new GenericProperties<>(Map.of("invalid", Literal.of("foo")))),
                new StubRoleManager(),
                new SessionSettingRegistry(Set.of()));
            assertThatThrownBy(() -> plan.executeOrFail(dependencyCarrier, plannerCtx, rowConsumer, Row.EMPTY, SubQueryResults.EMPTY))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Setting 'invalid' is not supported");
        }
    }
}
