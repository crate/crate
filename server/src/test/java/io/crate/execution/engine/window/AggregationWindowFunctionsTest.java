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

package io.crate.execution.engine.window;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;

import java.util.List;

import org.junit.Test;

import io.crate.metadata.ColumnIdent;

public class AggregationWindowFunctionsTest extends AbstractWindowFunctionTest {

    private static final Object[][] INPUT_ROWS = {
        new Object[]{1, 1},
        new Object[]{2, 2},
        new Object[]{2, 2},
        new Object[]{3, 3},
        new Object[]{4, 4},
        new Object[]{5, 5},
        new Object[]{null, null}
    };

    @Test
    public void testSumOverUnboundedPrecedingToUnboundedFollowingFrames() throws Throwable {
        Object[] expected = new Object[]{5L, 5L, 5L, 12L, 12L, 12L, null};
        assertEvaluate("sum(x) OVER(" +
                            "PARTITION BY x>2 ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING" +
                       ")",
            expected,
            List.of(ColumnIdent.of("x")),
            INPUT_ROWS
        );
    }

    @Test
    public void testCountOverUnboundedFollowingFrames() throws Throwable {
        Object[] expected = new Object[]{3L, 2L, 2L, 3L, 2L, 1L, 0L};
        assertEvaluate("count(x) OVER(" +
                            "PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
                       ")",
            expected,
            List.of(ColumnIdent.of("x")),
            INPUT_ROWS
        );
    }

    @Test
    public void testAvgOverUnboundedFollowingFrames() throws Throwable {
        Object[] expected = new Object[]{1.6666666666666667, 2.0, 2.0, 4.0, 4.5, 5.0, null};
        assertEvaluate(
            "avg(x) OVER(" +
            "   PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
            ")",
            expected,
            List.of(ColumnIdent.of("x")),
            INPUT_ROWS);
    }

    @Test
    public void testSumOverUnboundedFollowingFrames() throws Throwable {
        Object[] expected = new Object[]{5L, 4L, 4L, 12L, 9L, 5L, null};
        assertEvaluate(
            "sum(x) OVER(" +
            "   PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
            ")",
            expected,
            List.of(ColumnIdent.of("x")),
            INPUT_ROWS);
    }

    @Test
    public void testVarianceOverUnboundedFollowingFrames() throws Throwable {
        Object[] expected = new Object[]{0.22222222222222202, 0.0, 0.0, 0.6666666666666666, 0.25, 0.0, null};
        assertEvaluate(
            "variance(x) OVER(" +
            "   PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
            ")",
            expected,
            List.of(ColumnIdent.of("x")),
            INPUT_ROWS);
    }

    @Test
    public void testStdDevOverUnboundedFollowingFrames() throws Throwable {
        Object[] expected = new Object[]{0.47140452079103146, 0.0, 0.0, 0.816496580927726, 0.5, 0.0, null};
        assertEvaluate(
            "stddev(x) OVER(" +
            "   PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
            ")",
            expected,
            List.of(ColumnIdent.of("x")),
            INPUT_ROWS);
    }

    @Test
    public void testStringAggOverUnboundedFollowingFrames() throws Throwable {
        Object[] expected = new Object[]{"a,b,b", "b,b", "b,b", "c,d,e", "d,e", "e", null};
        assertEvaluate(
            "string_agg(z, ',') OVER(" +
            "   PARTITION BY z>'b' ORDER BY z RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
            ")",
            expected,
            List.of(ColumnIdent.of("z")),
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{"d"},
            new Object[]{"e"},
            new Object[]{null});
    }

    @Test
    public void testCollectSetOverUnboundedFollowingFrames() throws Throwable {
        Object[] expected = new Object[]{
            List.of(1, 2),
            List.of(2),
            List.of(2),
            List.of(2),
            List.of(2),
            List.of(3, 4, 5),
            List.of(4, 5),
            List.of(5),
            List.of()
        };
        assertEvaluate(
            "collect_set(x) OVER(" +
            "   PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
            ")",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5},
            new Object[]{null});
    }

    @Test
    public void testCollectSetOverRowsUnboundedPrecedingCurrentRowFrame() throws Throwable {
        Object[] expected = new Object[]{
            List.of(1),
            List.of(1, 2),
            List.of(1, 2),
            List.of(1, 2),
            List.of(3),
            List.of(3, 4),
            List.of(3, 4, 5),
            List.of()
        };
        assertEvaluate("collect_set(x) OVER(" +
                            "PARTITION BY x>2 ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW" +
                       ")",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{1, 1},
            new Object[]{2, 2},
            new Object[]{2, 2},
            new Object[]{2, 2},
            new Object[]{3, 3},
            new Object[]{4, 4},
            new Object[]{5, 5},
            new Object[]{null, null});
    }

    @Test
    public void test_agg_over_range_offset_preceding() throws Throwable {
        Object[] expected = new Object[]{
            2.5,
            6.5,
            11.5,
            15.0,
            18.5,
            22.0,
            26.0,
            22.0
        };
        assertEvaluate("sum(d) OVER(" +
                            "ORDER BY d RANGE BETWEEN 3 PRECEDING and CURRENT ROW" +
                       ")",
            expected,
            List.of(ColumnIdent.of("d")),
            new Object[]{2.5, 2.5},
            new Object[]{4.0, 4.0},
            new Object[]{5.0, 5.0},
            new Object[]{6.0, 6.0},
            new Object[]{7.5, 7.5},
            new Object[]{8.5, 8.5},
            new Object[]{10.0, 10.0},
            new Object[]{12.0, 12.0});
    }

    @Test
    public void test_agg_over_rows_offset_preceding() throws Throwable {
        Object[] expected = new Object[]{
            2.5,
            6.5,
            11.5,
            17.5,
            22.5,
            27.0,
            32.0,
            38.0
        };

        assertEvaluate("sum(d) OVER(" +
                            "ORDER BY d ROWS BETWEEN 3 PRECEDING and CURRENT ROW" +
                       ")",
            expected,
            List.of(ColumnIdent.of("d")),
            new Object[]{2.5, 2.5},
            new Object[]{4.0, 4.0},
            new Object[]{5.0, 5.0},
            new Object[]{6.0, 6.0},
            new Object[]{7.5, 7.5},
            new Object[]{8.5, 8.5},
            new Object[]{10.0, 10.0},
            new Object[]{12.0, 12.0});
    }

    @Test
    public void test_agg_over_range_following() throws Throwable {
        Object[] expected = new Object[]{
            11.5,
            15.0,
            18.5,
            22.0,
            26.0,
            18.5,
            22.0,
            12.0
        };

        assertEvaluate("sum(d) OVER(" +
                               "ORDER BY d RANGE BETWEEN CURRENT ROW and 3 FOLLOWING" +
                       ")",
                       expected,
                       List.of(ColumnIdent.of("d")),
                       new Object[]{2.5, 2.5},
                       new Object[]{4.0, 4.0},
                       new Object[]{5.0, 5.0},
                       new Object[]{6.0, 6.0},
                       new Object[]{7.5, 7.5},
                       new Object[]{8.5, 8.5},
                       new Object[]{10.0, 10.0},
                       new Object[]{12.0, 12.0});
    }

    @Test
    public void test_agg_over_rows_offset_following() throws Throwable {
        Object[] expected = new Object[]{
            17.5,
            22.5,
            27.0,
            32.0,
            38.0,
            30.5,
            22.0,
            12.0
        };

        assertEvaluate("sum(d) OVER(" +
                            "ORDER BY d ROWS BETWEEN CURRENT ROW and 3 FOLLOWING" +
                       ")",
                       expected,
                       List.of(ColumnIdent.of("d")),
                       new Object[]{2.5, 2.5},
                       new Object[]{4.0, 4.0},
                       new Object[]{5.0, 5.0},
                       new Object[]{6.0, 6.0},
                       new Object[]{7.5, 7.5},
                       new Object[]{8.5, 8.5},
                       new Object[]{10.0, 10.0},
                       new Object[]{12.0, 12.0});
    }

    @Test
    public void test_sum_with_range_offset_preceding_following_including_partitions() throws Throwable {
        Object[][] rows = new Object[][] {
            $("Male", 1000),
            $("Male", 2000),
            $("Female", 3000),
            $("Female", 4000),
            $("Male", 5000),
            $("Female", 6000),
            $("Male", 7000),
            $("Female", 8000),
            $("Male", 9000),
            $("Male", 9500),
        };
        Object[] expected = new Object[] {
            7000.0d,
            7000.0d,
            6000.0d,
            8000.0d,
            3000.0d,
            3000.0d,
            5000.0d,
            7000.0d,
            18500.0d,
            18500.0d,
        };
        assertEvaluate(
            "sum(d) over (partition by z order by d range between 1000 preceding and 1000 following)",
            expected,
            List.of(ColumnIdent.of("z"), ColumnIdent.of("d")),
            rows
        );
    }
}
