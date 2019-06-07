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

package io.crate.execution.engine.window;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.TableDefinitions;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static io.crate.sql.tree.FrameBound.Type.CURRENT_ROW;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class WindowDefinitionTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService, 1, RandomizedTest.getRandom())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
    }

    @Test
    public void testStartUnboundedFollowingIsIllegal() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage(
            "Only unbounded preceding -> current row and current row -> unbounded following frame definitions are supported");
        e.plan(
            "select sum(col1) over(RANGE BETWEEN UNBOUNDED FOLLOWING and CURRENT ROW) FROM " +
            "unnest([1, 2, 1, 1, 1, 4])");

    }

    @Test
    public void testEndUnboundedPrecedingIsIllegal() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage(
            "Only unbounded preceding -> current row and current row -> unbounded following frame definitions are supported");
        e.plan(
            "select sum(col1) over(RANGE BETWEEN CURRENT ROW and UNBOUNDED PRECEDING) FROM " +
            "unnest([1, 2, 1, 1, 1, 4])");
    }

    @Test
    public void testFrameEndDefaultsToCurrentRowIfNotSpecified() {
        AnalyzedStatement analyze = e.analyze(
            "select sum(col1) over(RANGE UNBOUNDED PRECEDING) FROM " +
            "unnest([1, 2, 1, 1, 1, 4])");
        assertThat(analyze, is(instanceOf(QueriedTable.class)));
        QueriedTable queriedTable = (QueriedTable) analyze;
        List<Symbol> outputs = queriedTable.outputs();
        assertThat(outputs.size(), is(1));
        WindowFunction windowFunction = (WindowFunction) outputs.get(0);
        assertThat(windowFunction.windowDefinition().windowFrameDefinition().end().type(), is(CURRENT_ROW));
    }

    @Test
    public void testRowsFrameDefinitionIsNotSupported() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage(
            "Only unbounded preceding -> current row and current row -> unbounded following frame definitions are supported");
        e.plan(
            "select sum(col1) over(ROWS 2 PRECEDING) FROM " +
            "unnest([1, 2, 1, 1, 1, 4])");
    }

}
