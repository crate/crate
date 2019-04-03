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

package io.crate.analyze;

import io.crate.expression.symbol.ParameterSymbol;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isInputColumn;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DecommissionNodeAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutorWithT1AndT2() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .build();
    }

    @Test
    public void testDecommissionNodeUsingStringLiteral() throws Exception {
        AnalyzedDecommissionNodeStatement statement = e.analyze("alter cluster decommission 'aNodeIdOrName'");
        assertThat(statement.nodeIdOrName(), isLiteral("aNodeIdOrName"));
    }

    @Test
    public void testDecommissionNodeUsingParameter() throws Exception {
        AnalyzedDecommissionNodeStatement statement = e.analyze("alter cluster decommission ?");
        assertThat(statement.nodeIdOrName(), instanceOf(ParameterSymbol.class));
    }

    @Test
    public void testDecommissionNodeFailsIfNodeIsMissing() {
        expectedException.expectMessage("mismatched input '<EOF>'");
        e.analyze("alter cluster decommission");
    }
}
