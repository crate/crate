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

import io.crate.metadata.ColumnIdent;
import org.junit.Test;

import java.util.Collections;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.analyze.WindowDefinition.CURRENT_ROW_UNBOUNDED_FOLLOWING;
import static io.crate.analyze.WindowDefinition.UNBOUNDED_PRECEDING_UNBOUNDED_FOLLOWING;
import static org.hamcrest.Matchers.contains;

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
    public void testSumOverUnboundedPrecedingToUnboundedFollowingFrames() throws Exception {
        Object[] expected = new Object[]{5L, 5L, 5L, 12L, 12L, 12L, null};
        assertEvaluate("sum(x) OVER(" +
                       "PARTITION BY x>2 ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING" +
                       ")",
                       contains(expected),
                       Collections.singletonMap(new ColumnIdent("x"), 0),
                       UNBOUNDED_PRECEDING_UNBOUNDED_FOLLOWING,
                       INPUT_ROWS);
    }

    @Test
    public void testCountOverUnboundedFollowingFrames() throws Exception {
        Object[] expected = new Object[]{3L, 2L, 2L, 3L, 2L, 1L, 0L};
        assertEvaluate("count(x) OVER(" +
                            "PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
                       ")",
                       contains(expected),
                       Collections.singletonMap(new ColumnIdent("x"), 0),
                       CURRENT_ROW_UNBOUNDED_FOLLOWING,
                       INPUT_ROWS);
    }

    @Test
    public void testAvgOverUnboundedFollowingFrames() throws Exception {
        Object[] expected = new Object[]{1.6666666666666667, 2.0, 2.0, 4.0, 4.5, 5.0, null};
        assertEvaluate("avg(x) OVER(" +
                            "PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
                       ")",
                       contains(expected),
                       Collections.singletonMap(new ColumnIdent("x"), 0),
                       CURRENT_ROW_UNBOUNDED_FOLLOWING,
                       INPUT_ROWS);
    }

    @Test
    public void testSumOverUnboundedFollowingFrames() throws Exception {
        Object[] expected = new Object[]{5L, 4L, 4L, 12L, 9L, 5L, null};
        assertEvaluate("sum(x) OVER(" +
                            "PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
                       ")",
                       contains(expected),
                       Collections.singletonMap(new ColumnIdent("x"), 0),
                       CURRENT_ROW_UNBOUNDED_FOLLOWING,
                       INPUT_ROWS);
    }

    @Test
    public void testVarianceOverUnboundedFollowingFrames() throws Exception {
        Object[] expected = new Object[]{0.22222222222222202, 0.0, 0.0, 0.6666666666666666, 0.25, 0.0, null};
        assertEvaluate("variance(x) OVER(" +
                            "PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
                       ")",
                       contains(expected),
                       Collections.singletonMap(new ColumnIdent("x"), 0),
                       CURRENT_ROW_UNBOUNDED_FOLLOWING,
                       INPUT_ROWS);
    }

    @Test
    public void testStdDevOverUnboundedFollowingFrames() throws Exception {
        Object[] expected = new Object[]{0.47140452079103146, 0.0, 0.0, 0.816496580927726, 0.5, 0.0, null};
        assertEvaluate("stddev(x) OVER(" +
                            "PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
                       ")",
                       contains(expected),
                       Collections.singletonMap(new ColumnIdent("x"), 0),
                       CURRENT_ROW_UNBOUNDED_FOLLOWING,
                       INPUT_ROWS);
    }

    @Test
    public void testStringAggOverUnboundedFollowingFrames() throws Exception {
        Object[] expected = new Object[]{"a,b,b", "b,b", "b,b", "c,d,e", "d,e", "e", null};
        assertEvaluate("string_agg(z, ',') OVER(" +
                            "PARTITION BY z>'b' ORDER BY z RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
                       ")",
                       contains(expected),
                       Collections.singletonMap(new ColumnIdent("z"), 0),
                       CURRENT_ROW_UNBOUNDED_FOLLOWING,
                       new Object[]{"a", 1},
                       new Object[]{"b", 2},
                       new Object[]{"b", 2},
                       new Object[]{"c", 3},
                       new Object[]{"d", 4},
                       new Object[]{"e", 5},
                       new Object[]{null, null});
    }

    @Test
    public void testCollectSetOverUnboundedFollowingFrames() throws Exception {
        Object[] expected = new Object[]{
            $(1, 2),
            $(2),
            $(2),
            $(2),
            $(2),
            $(3, 4, 5),
            $(4, 5),
            $(5),
            $()
        };
        assertEvaluate("collect_set(x) OVER(" +
                            "PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
                       ")",
                       contains(expected),
                       Collections.singletonMap(new ColumnIdent("x"), 0),
                       CURRENT_ROW_UNBOUNDED_FOLLOWING,
                       new Object[]{1, 1},
                       new Object[]{2, 2},
                       new Object[]{2, 2},
                       new Object[]{2, 2},
                       new Object[]{2, 2},
                       new Object[]{3, 3},
                       new Object[]{4, 4},
                       new Object[]{5, 5},
                       new Object[]{null, null});
    }
}
