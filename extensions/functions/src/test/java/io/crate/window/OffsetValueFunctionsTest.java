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

package io.crate.window;

import io.crate.execution.engine.window.AbstractWindowFunctionTest;
import io.crate.metadata.ColumnIdent;
import io.crate.module.ExtraFunctionsModule;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;

public class OffsetValueFunctionsTest extends AbstractWindowFunctionTest {

    public OffsetValueFunctionsTest() {
        super(new ExtraFunctionsModule());
    }

    @Test
    public void testLagWithSingleArgumentAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x) over()",
            contains(new Object[]{null, 1, null, 2}),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{null},
            new Object[]{2},
            new Object[]{3}
        );
    }

    @Test
    public void testLagWithOffsetAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x, 2) over()",
            contains(new Object[]{null, null, 1, 2}),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        );
    }

    @Test
    public void testLagWithNullOffset() throws Throwable {
        assertEvaluate(
            "lag(x, null) over()",
            contains(new Object[]{null, null, null}),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{null},
            new Object[]{2}
        );
    }

    @Test
    public void testLagZeroOffsetAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x, 0) over()",
            contains(new Object[]{1, 2}),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{2}
        );
    }

    @Test
    public void testLagNegativeOffsetAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x, -1) over()",
            contains(new Object[]{2, null}),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{2}
        );
    }

    @Test
    public void testLagWithDefaultValueAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x, 1, -1) over()",
            contains(new Object[]{-1, 1, 2, 3}),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        );
    }

    @Test
    public void testLagWithExpressionAsArgumentAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(coalesce(x, 1), 1, -1) over()",
            contains(new Object[]{-1, 1, 1}),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{null},
            new Object[]{2}
        );
    }

    @Test
    public void testLagWithOrderBy() throws Throwable {
        assertEvaluate(
            "lag(x) over(order by y)",
            contains(new Object[]{null, 1, 3, 1}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[]{1, 1},
            new Object[]{1, 3},
            new Object[]{3, 2},
            new Object[]{2, 5}
        );
    }

    @Test
    public void testLagWithOrderByReferenceUsedInFunction() throws Throwable {
        assertEvaluate(
            "lag(x) over(order by y, x)",
            contains(new Object[]{null, 1, 1, 3}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[]{1, 1},
            new Object[]{1, 2},
            new Object[]{3, 2},
            new Object[]{2, 3}
        );
    }

    @Test
    public void testLagOverPartitionedWindow() throws Throwable {
        assertEvaluate("lag(x) over(partition by x > 2)",
            contains(new Object[]{null, 1, 2, null, 3, 4}),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5});
    }

    @Test
    public void testLagOperatesOnPartitionAndIgnoresFrameRange() throws Throwable {
        assertEvaluate(
            "lag(x) over(RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING)",
            contains(new Object[]{null, 1, 2, 3}),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        );
    }

    @Test
    public void testLeadOverPartitionedWindow() throws Throwable {
        assertEvaluate(
            "lead(x) over(partition by x > 2)",
            contains(new Object[]{2, 2, null, 4, 5, null}),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5});
    }

    @Test
    public void testLeadWithOrderBy() throws Throwable {
        assertEvaluate("lead(x) over(order by y)",
                       contains(new Object[]{3, 1, 2, null}),
                       List.of(new ColumnIdent("x"), new ColumnIdent("y")),
                       new Object[]{1, 1},
                       new Object[]{1, 3},
                       new Object[]{3, 2},
                       new Object[]{2, 5}
        );
    }

    @Test
    public void testLeadOverCurrentRowUnboundedFollowingWithDefaultValue() throws Throwable {
        assertEvaluate(
            "lead(x, 2, 42) over(RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING)",
            contains(new Object[]{2, 3, 4, 42, 42}),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        );
    }

    private static final List<ColumnIdent> INTERVAL_COLS = List.of(new ColumnIdent("x"), new ColumnIdent("y"));

    private static final Object[][] INTERVAL_DATA = new Object[][]{
        new Object[]{7, 1574812800000L},
        new Object[]{3, 1574812810000L},
        new Object[]{5, 1574812820000L},
        new Object[]{7, 1574812830000L},
        new Object[]{8, 1574812840000L},
        new Object[]{2, 1574812850000L},
        new Object[]{3, 1574812860000L},
        new Object[]{2, 1574812870000L},
        new Object[]{2, 1574812880000L},
        new Object[]{3, 1574812890000L},
        new Object[]{7, 1574812900000L},
        new Object[]{6, 1574812910000L}
    };

    @Test
    public void test_offset_preceding_supports_interval_in_range_mode() throws Throwable {
        assertEvaluate(
            "avg(x) over(order by y range between '30 seconds'::interval preceding and current row)",
            contains(new Object[]{
                7.0,
                5.0,
                5.0,
                5.5,
                5.75,
                5.5,
                5.0,
                3.75,
                2.25,
                2.5,
                3.5,
                4.5}),
            INTERVAL_COLS,
            INTERVAL_DATA
        );
    }

    @Test
    public void test_offset_following_supports_interval_in_range_mode_simple() throws Throwable {
        assertEvaluate(
            "avg(x) over(order by y range between current row and '30 seconds'::interval following)",
            contains(new Object[]{
                5.5,
                5.75,
                5.5,
                5.0,
                3.75,
                2.25,
                2.5,
                3.5,
                4.5,
                5.333333333333333,
                6.5,
                6.0}),
            INTERVAL_COLS,
            INTERVAL_DATA
        );
    }

}
