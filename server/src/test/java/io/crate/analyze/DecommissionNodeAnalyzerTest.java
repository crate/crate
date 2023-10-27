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

package io.crate.analyze;

import static io.crate.testing.Asserts.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.crate.data.RowN;
import io.crate.planner.DecommissionNodePlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class DecommissionNodeAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void setup() {
        e = SQLExecutor.builder(clusterService).build();
        plannerContext = e.getPlannerContext(clusterService.state());
    }

    private String analyze(String stmt, Object... arguments) {
        AnalyzedDecommissionNode analyzedStatement = e.analyze(stmt);
        return DecommissionNodePlan.boundNodeIdOrName(
            analyzedStatement,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            new RowN(arguments),
            SubQueryResults.EMPTY
        );
    }

    @Test
    public void testDecommissionNodeUsingStringLiteral() {
        assertThat(analyze("ALTER CLUSTER DECOMMISSION 'aNodeIdOrName'")).isEqualTo("aNodeIdOrName");
    }

    @Test
    public void testDecommissionNodeUsingParameter() {
        assertThat(analyze("ALTER CLUSTER DECOMMISSION ?", "node_name")).isEqualTo("node_name");
    }
}
