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

import static io.crate.testing.Asserts.assertThat;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.carrotsearch.hppc.RamUsageEstimator;

import io.crate.data.RowN;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.window.AbstractWindowFunctionTest;
import io.crate.metadata.ColumnIdent;
import io.crate.testing.PlainRamAccounting;

public class OffsetValueFunctionsTest extends AbstractWindowFunctionTest {

    private static long SHALLOW_ROW_N_SIZE = RamUsageEstimator.shallowSizeOfInstance(RowN.class);


    @Test
    public void testLagWithSingleArgumentAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x) over()",
            new Object[]{null, 1, null, 2},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{null},
            new Object[]{2},
            new Object[]{3}
        );
    }

    @Test
    public void testLagWithIgnoreNullsWithSingleArgumentAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x) ignore nulls over()",
            new Object[]{null, 1, 1, 2},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{null},
            new Object[]{2},
            new Object[]{3}
        );
    }

    @Test
    public void testLagWithRespectNullsWithSingleArgumentAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x) respect nulls over()",
            new Object[]{null, 1, null, 2},
            List.of(ColumnIdent.of("x")),
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
            new Object[]{null, null, 1, 2},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        );
    }

    @Test
    public void testLagWithIgnoreNullsWithOffsetAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x, 2) ignore nulls over()",
            new Object[]{null, null, 1, 1},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{null},
            new Object[]{4}
        );
    }

    @Test
    public void testLagWithNullOffset() throws Throwable {
        assertEvaluate(
            "lag(x, null) over()",
            new Object[]{null, null, null},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{null},
            new Object[]{2}
        );
    }

    @Test
    public void testLagWithIgnoreNullsWithNullOffset() throws Throwable {
        assertEvaluate(
            "lag(x, null) ignore nulls over()",
            new Object[]{null, null, null},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{null},
            new Object[]{2}
        );
    }

    @Test
    public void testLagZeroOffsetAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x, 0) over()",
            new Object[]{1, 2},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2}
        );
    }

    @Test
    public void testLagWithIgnoreNullsAndZeroOffsetThrows() {
        Assertions.assertThatThrownBy(() -> assertEvaluate(
                "lag(x,0,123) ignore nulls over()",
                null,
                List.of(ColumnIdent.of("x")),
                new Object[][]{{1}, {2}, {null}, {3}, {null}, {null}, {4}, {5}, {null}, {6}, {null}, {7}}))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("offset 0 is not a valid argument if ignore nulls flag is set");
    }

    @Test
    public void testLagNegativeOffsetAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x, -1) over()",
            new Object[]{2, null},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2}
        );
    }

    @Test
    public void testLagIgnoreNullsNegativeOffsetAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x, -1) ignore nulls over()",
            new Object[]{2, 3, 3 ,null},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{null},
            new Object[]{3}
        );
    }

    @Test
    public void testLagWithDefaultValueAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x, 1, -1) over()",
            new Object[]{-1, 1, 2, 3},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        );
    }

    @Test
    public void testLagWithIgnoreNullsWithDefaultValueAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(x, 1, -1) ignore nulls over()",
            new Object[]{-1, 1, 1, 3},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{null},
            new Object[]{3},
            new Object[]{4}
        );
    }

    @Test
    public void testLagWithExpressionAsArgumentAndEmptyOver() throws Throwable {
        assertEvaluate(
            "lag(coalesce(x, 1), 1, -1) over()",
            new Object[]{-1, 1, 1},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{null},
            new Object[]{2}
        );
    }

    @Test
    public void testLagWithOrderBy() throws Throwable {
        assertEvaluate(
            "lag(x) over(order by y)",
            new Object[]{null, 1, 3, 1},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
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
            new Object[]{null, 1, 1, 3},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[]{1, 1},
            new Object[]{1, 2},
            new Object[]{3, 2},
            new Object[]{2, 3}
        );
    }

    @Test
    public void testLeadWithOnlyNulls() throws Throwable {
        assertEvaluate(
            "lead(x,1) ignore nulls over()",
            new Object[]{null, null, null, null},
            List.of(ColumnIdent.of("x")),
            new Object[]{null},
            new Object[]{null},
            new Object[]{null},
            new Object[]{null}
        );
    }

    @Test
    public void testLeadIgnoreNullsWithOrderBy() throws Throwable {
        assertEvaluate(
            "lead(x) ignore nulls over(order by x asc)",
            new Object[]{2, 3, 4, 5, null},
            List.of(ColumnIdent.of("x")),
            new Object[]{5},
            new Object[]{4},
            new Object[]{3},
            new Object[]{2},
            new Object[]{1}
        );
        assertEvaluate(
            "lead(x) ignore nulls over(order by y asc, z asc)",
            new Object[]{2, 3, 4, 5, null},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y"), ColumnIdent.of("z")),
            new Object[]{3,2,"c"},
            new Object[]{5,2,"e"},
            new Object[]{4,2,"d"},
            new Object[]{2,1,"b"},
            new Object[]{1,1,"a"}
        );
    }

    @Test
    public void testLagOverPartitionedWindow() throws Throwable {
        assertEvaluate("lag(x) over(partition by x > 2)",
            new Object[]{null, 1, 2, null, 3, 4},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5});
    }

    @Test
    public void testLagWithIgnoreNullsOverPartitionedWindow() throws Throwable {
        assertEvaluate("lag(x) over(partition by x > 2 order by x)",
                       new Object[]{null, 1, 2, null, 3, 4, null, null},
                       List.of(ColumnIdent.of("x")),
                       new Object[]{1},
                       new Object[]{2},
                       new Object[]{2},
                       new Object[]{null},
                       new Object[]{3},
                       new Object[]{4},
                       new Object[]{null},
                       new Object[]{5});
        // 3 partitions, 1) x > 2, 2) x <= 2, 3) nulls
    }

    @Test
    public void testLagOperatesOnPartitionAndIgnoresFrameRange() throws Throwable {
        assertEvaluate(
            "lag(x) over(RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING)",
            new Object[]{null, 1, 2, 3},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        );
    }

    @Test
    public void testLeadWithIgnoreNullsIgnoresFrameRange() throws Throwable {
        assertEvaluate(
            "lead(x,-3,123) ignore nulls over(RANGE BETWEEN CURRENT ROW and CURRENT ROW)",
            new Object[]{123, 123, 123, 123, 1, 1, 1, 2, 3, 3, 4, 4},
            List.of(ColumnIdent.of("x")),
            new Object[][]{{1}, {2}, {null}, {3}, {null}, {null}, {4}, {5}, {null}, {6}, {null}, {7}}
        );
    }

    @Test
    public void testLeadOverPartitionedWindow() throws Throwable {
        assertEvaluate(
            "lead(x) over(partition by x > 2)",
            new Object[]{2, 2, null, 4, 5, null},
            List.of(ColumnIdent.of("x")),
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
                       new Object[]{3, 1, 2, null},
                       List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
                       new Object[]{1, 1},
                       new Object[]{1, 3},
                       new Object[]{3, 2},
                       new Object[]{2, 5}
        );
    }

    @Test
    public void testLagIgnoringNullsWithNullsAtLeftCorner() throws Throwable {
        assertEvaluate(
            "lag(x) ignore nulls over()",
            new Object[]{null, null, null, 2},
            List.of(ColumnIdent.of("x")),
            new Object[]{null},
            new Object[]{null},
            new Object[]{2},
            new Object[]{3}
        );
    }

    @Test
    public void testLagIgnoringNullsWithNullsAtCenter() throws Throwable {
        assertEvaluate(
            "lag(x) ignore nulls over()",
            new Object[]{null, 1, 1, 1},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{null},
            new Object[]{null},
            new Object[]{2}
        );
    }

    @Test
    public void testLagIgnoringNullsWithNullsAtRightCorner() throws Throwable {
        assertEvaluate(
            "lag(x) ignore nulls over()",
            new Object[]{null, 1, 2, 2},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{null},
            new Object[]{null}
        );
    }

    @Test
    public void testLagIgnoringNullsWithOnlyNulls() throws Throwable {
        assertEvaluate(
            "lag(x,1) ignore nulls over()",
            new Object[]{null, null, null, null},
            List.of(ColumnIdent.of("x")),
            new Object[]{null},
            new Object[]{null},
            new Object[]{null},
            new Object[]{null}
        );
    }

    @Test
    public void testLagIgnoringNullsWithMultipleGroupsOfNulls() throws Throwable {
        assertEvaluate(
            "lag(x,3,123) ignore nulls over()",
            new Object[]{123, 123, 123, 123, 1, 1, 1, 2, 3, 3, 4, 4},
            List.of(ColumnIdent.of("x")),
            new Object[][]{{1}, {2}, {null}, {3}, {null}, {null}, {4}, {5}, {null}, {6}, {null}, {7}}
        );
    }

    @Test
    public void testLeadIgnoringNullsWithNullsAtLeftCorner() throws Throwable {
        assertEvaluate(
            "lead(x) ignore nulls over()",
            new Object[]{2, 2, 3, null},
            List.of(ColumnIdent.of("x")),
            new Object[]{null},
            new Object[]{null},
            new Object[]{2},
            new Object[]{3}
        );
    }

    @Test
    public void testLeadIgnoringNullsWithNullsAtCenter() throws Throwable {
        assertEvaluate(
            "lead(x) ignore nulls over()",
            new Object[]{2, 2, 2, null},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{null},
            new Object[]{null},
            new Object[]{2}
        );
    }

    @Test
    public void testLeadIgnoringNullsWithNullsAtRightCorner() throws Throwable {
        assertEvaluate(
            "lead(x) ignore nulls over()",
            new Object[]{2, null, null, null},
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{null},
            new Object[]{null}
        );
    }

    @Test
    public void testLeadIgnoringNullsWithOnlyNulls() throws Throwable {
        assertEvaluate(
            "lead(x,1,123) ignore nulls over()",
            new Object[]{123, 123, 123, 123},
            List.of(ColumnIdent.of("x")),
            new Object[]{null},
            new Object[]{null},
            new Object[]{null},
            new Object[]{null}
        );
    }

    @Test
    public void testLeadIgnoringNullsWithMultipleGroupsOfNulls() throws Throwable {
        assertEvaluate(
            "lead(x,3,123) ignore nulls over()",
            new Object[]{4, 5, 5, 6, 6, 6, 7, 123, 123, 123, 123, 123},
            List.of(ColumnIdent.of("x")),
            new Object[][]{{1}, {2}, {null}, {3}, {null}, {null}, {4}, {5}, {null}, {6}, {null}, {7}}
        );
    }

    @Test
    public void testLagIgnoringNullsWithOffsetLargerThanWindow() throws Throwable {
        assertEvaluate(
            "lag(x,40,123) ignore nulls over()",
            new Object[]{123, 123, 123, 123, 123, 123, 123, 123, 123, 123, 123, 123, 123},
            List.of(ColumnIdent.of("x")),
            new Object[][]{{null}, {1}, {2}, {null}, {3}, {null}, {null}, {4}, {5}, {null}, {6}, {null}, {7}}
        );
    }

    @Test
    public void testLeadWithNegativeOffsetIgnoringNullsWithMultipleGroupsOfNulls() throws Throwable {
        assertEvaluate(
            "lead(x,-3,123) ignore nulls over()",
            new Object[]{123, 123, 123, 123, 1, 1, 1, 2, 3, 3, 4, 4},
            List.of(ColumnIdent.of("x")),
            new Object[][]{{1}, {2}, {null}, {3}, {null}, {null}, {4}, {5}, {null}, {6}, {null}, {7}}
        );
    }

    @Test
    public void testLagWithNegativeOffsetIgnoringNullsWithMultipleGroupsOfNulls() throws Throwable {
        assertEvaluate(
            "lag(x,-2,123) ignore nulls over()",
            new Object[]{3, 4, 4, 5, 5, 5, 6, 7, 7, 123, 123, 123},
            List.of(ColumnIdent.of("x")),
            new Object[][]{{1}, {2}, {null}, {3}, {null}, {null}, {4}, {5}, {null}, {6}, {null}, {7}}
        );
    }

    private static final List<ColumnIdent> INTERVAL_COLS = List.of(ColumnIdent.of("x"), ColumnIdent.of("y"));

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
            new Object[]{
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
                4.5},
            INTERVAL_COLS,
            INTERVAL_DATA
        );
    }

    @Test
    public void test_offset_following_supports_interval_in_range_mode_simple() throws Throwable {
        assertEvaluate(
            "avg(x) over(order by y range between current row and '30 seconds'::interval following)",
            new Object[]{
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
                6.0},
            INTERVAL_COLS,
            INTERVAL_DATA
        );
    }

    @Test
    public void test_row_cache_memory_is_accounted() throws Throwable {
        RamAccounting accounting = new PlainRamAccounting();
        assertEvaluate(
            "lag(x) over(order by x)",
            new Object[]{null, 1},
            List.of(ColumnIdent.of("x")),
            accounting::addBytes,
            new Object[]{1},
            new Object[]{2}
        );
        assertThat(accounting.totalBytes()).isEqualTo(217L);
    }

}
