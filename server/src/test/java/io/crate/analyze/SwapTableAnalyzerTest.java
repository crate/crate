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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class SwapTableAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutorWithT1AndT2() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t1 (x int)")
            .addTable("create table t2 (x long)");
    }

    @Test
    public void testSwapTableStatementCanBeAnalysedIfBothTablesExist() throws Exception {
        AnalyzedSwapTable statement = e.analyze("alter cluster swap table t1 to t2 with (drop_source = true)");
        assertThat(statement.source()).hasToString("doc.t1");
        assertThat(statement.target()).hasToString("doc.t2");
        assertThat(statement.dropSource()).isLiteral(true);
    }

    @Test
    public void testDropSourceDefaultsToFalse() {
        AnalyzedSwapTable statement = e.analyze("alter cluster swap table t1 to t2");
        assertThat(statement.dropSource()).isLiteral(false);
    }

    @Test
    public void testSwapTableFailsIfOneTableIsMissing() {
        assertThatThrownBy(() -> {
            e.analyze("alter cluster swap table t1 to t4");

        })
                .hasMessage("Relation 't4' unknown");
    }

    @Test
    public void testSwapTableStatementFailsWithInvalidOptions() {
        assertThatThrownBy(() -> {
            e.analyze("alter cluster swap table t1 to t2 with (foo = 'bar')");

        })
                .hasMessage("Invalid options for ALTER CLUSTER SWAP TABLE: foo");
    }

    @Test
    public void testSwapTableDoesNotWorkOnSystemTables() {
        assertThatThrownBy(() -> {
            e.analyze("alter cluster swap table sys.cluster to t2");

        })
                .hasMessage("The relation \"sys.cluster\" doesn't support or allow ALTER operations");
    }
}
