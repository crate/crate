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

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.is;

public class SwapTableAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutorWithT1AndT2() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int)")
            .addTable("create table t2 (x long)")
            .build();
    }

    @Test
    public void testSwapTableStatementCanBeAnalysedIfBothTablesExist() throws Exception {
        AnalyzedSwapTable statement = e.analyze("alter cluster swap table t1 to t2 with (drop_source = true)");
        assertThat(statement.source().toString(), is("doc.t1"));
        assertThat(statement.target().toString(), is("doc.t2"));
        assertThat(statement.dropSource(), isLiteral(true));
    }

    @Test
    public void testDropSourceDefaultsToFalse() {
        AnalyzedSwapTable statement = e.analyze("alter cluster swap table t1 to t2");
        assertThat(statement.dropSource(), isLiteral(false));
    }

    @Test
    public void testSwapTableFailsIfOneTableIsMissing() {
        expectedException.expectMessage("Relation 't4' unknown");
        e.analyze("alter cluster swap table t1 to t4");
    }

    @Test
    public void testSwapTableStatementFailsWithInvalidOptions() {
        expectedException.expectMessage("Invalid options for ALTER CLUSTER SWAP TABLE: foo");
        e.analyze("alter cluster swap table t1 to t2 with (foo = 'bar')");
    }

    @Test
    public void testSwapTableDoesNotWorkOnSystemTables() {
        expectedException.expectMessage("The relation \"sys.cluster\" doesn't support or allow ALTER operations, as it is read-only.");
        e.analyze("alter cluster swap table sys.cluster to t2");
    }
}
