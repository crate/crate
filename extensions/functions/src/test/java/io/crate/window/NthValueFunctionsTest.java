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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.execution.engine.window.AbstractWindowFunctionTest;
import io.crate.metadata.ColumnIdent;


public class NthValueFunctionsTest extends AbstractWindowFunctionTest {

    @Test
    public void testLastValueWithEmptyOver() throws Throwable {
        assertEvaluate(
            "last_value(x) over()",
            new Object[] {4, 4, 4, 4},
            List.of(ColumnIdent.of("x")),
            new Object[] {1},
            new Object[] {2},
            new Object[] {3},
            new Object[] {4}
        );
    }

    @Test
    public void testLastValueWithOrderByClause() throws Throwable {
        assertEvaluate(
            "last_value(x) over(order by y)",
            new Object[] {1, 3, 3, 2},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testLastValueUseSymbolMultipleTimes() throws Throwable {
        assertEvaluate(
            "last_value(x) over(order by y, x)",
            new Object[] {1, 1, 3, 2},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testLastWithIgnoreNullsValueUseSymbolMultipleTimes() throws Throwable {
        assertEvaluate(
            "last_value(x) ignore nulls over(order by y, x)",
            new Object[] {1, 1, 3, 2, 2},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3},
            new Object[] {null, 4}
        );
    }

    @Test
    public void testFirstValueWithEmptyOver() throws Throwable {
        assertEvaluate(
            "first_value(x) over()",
            new Object[] {1, 1, 1, 1},
            List.of(ColumnIdent.of("x")),
            new Object[] {1},
            new Object[] {2},
            new Object[] {3},
            new Object[] {4}
        );
    }

    @Test
    public void testFirstValueWithOrderByClause() throws Throwable {
        assertEvaluate("first_value(x) over(order by y)",
            new Object[] {1, 1, 1, 1},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testNthValueWithEmptyOver() throws Throwable {
        assertEvaluate("nth_value(x, 3) over()",
            new Object[] {3, 3, 3, 3},
            List.of(ColumnIdent.of("x")),
            new Object[] {1},
            new Object[] {2},
            new Object[] {3},
            new Object[] {4}
        );
    }

    @Test
    public void testNthValueWithOrderByClause() throws Throwable {
        assertEvaluate(
            "nth_value(x, 3) over(order by y)",
            new Object[] {null, 3, 3, 3},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testNthValueWithNullPositionReturnsNull() throws Throwable {
        assertEvaluate("nth_value(x,null) over(order by y)",
            new Object[] {null, null, null, null},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testFirstValueWithNullPositionReturnsNull() {
        assertThatThrownBy(
            () -> assertEvaluate("first_value(x,null) over(order by y)",
                                 new Object[] {null, null, null, null},
                                 List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
                                 new Object[] {1, 1},
                                 new Object[] {1, 2},
                                 new Object[] {3, 2},
                                 new Object[] {2, 3}))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Unknown function: first_value(doc.t1.x, NULL), no overload found for matching argument types: " +
                        "(integer, undefined). Possible candidates: first_value(E):E");
    }

    @Test
    public void testLastValueWithNullPositionReturnsNull() {
        assertThatThrownBy(
            () -> assertEvaluate("last_value(x,null) over(order by y)",
                                 new Object[] {null, null, null, null},
                                 List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
                                 new Object[] {1, 1},
                                 new Object[] {1, 2},
                                 new Object[] {3, 2},
                                 new Object[] {2, 3}))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Unknown function: last_value(doc.t1.x, NULL), no overload found for matching argument types: " +
                        "(integer, undefined). Possible candidates: last_value(E):E");
    }

    @Test
    public void testNthValueIgnoreNullsWithNullPositionReturnsNull() throws Throwable {
        assertEvaluate("nth_value(x,null) ignore nulls over(order by y)",
                       new Object[] {null, null, null, null},
                       List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
                       new Object[] {1, 1},
                       new Object[] {null, 2},
                       new Object[] {3, 2},
                       new Object[] {2, 3}
        );
    }

    @Test
    public void testRowNthValueOverPartitionedWindow() throws Throwable {
        Object[] expected = new Object[]{2, 2, 2, 4, 4, 4, null};
        assertEvaluate(
            "nth_value(x, 2) over(partition by x > 2)",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{1, 1},
            new Object[]{2, 2},
            new Object[]{2, 2},
            new Object[]{3, 3},
            new Object[]{4, 4},
            new Object[]{5, 5},
            new Object[]{null, null});
    }

    @Test
    public void testRowNthValueIgnoreNullsOverPartitionedWindow() throws Throwable {
        Object[] expected = new Object[]{2, 2, 2, 4, 4, 4, null};
        assertEvaluate(
            "nth_value(x, 2) ignore nulls over(partition by x > 2)",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{1, 1},
            new Object[]{2, 2},
            new Object[]{2, 2},
            new Object[]{3, 3},
            new Object[]{4, 4},
            new Object[]{5, 5},
            new Object[]{null, null});
    }

    @Test
    public void testNthValueOverPartitionedOrderedWindow() throws Throwable {
        Object[] expected = new Object[]{null, 2, 2, null, 4, 4, null};
        assertEvaluate(
            "nth_value(x, 2) over(partition by x > 2 order by x)",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5},
            new Object[]{null});
    }

    @Test
    public void testNthValueIgnoreNullsOverPartitionedOrderedWindow() throws Throwable {
        Object[] expected = new Object[]{null, 2, 2, null, 4, 4, null};
        assertEvaluate(
            "nth_value(x, 2) ignore nulls over(partition by x > 2 order by x)",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5},
            new Object[]{null});
    }

    @Test
    public void testNthValueOverUnboundedFollowingWindow() throws Throwable {
        Object[] expected = new Object[]{2, 2, 2, 4, 5, null, null};
        assertEvaluate(
            "nth_value(x, 2) OVER(PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING)",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5},
            new Object[]{null});
    }

    @Test
    public void testNthValueWithIgnoreNullsOverUnboundedFollowingWindow() throws Throwable {
        Object[] expected = new Object[]{2, 2, 2, 4, 5, null, null};
        assertEvaluate(
            "nth_value(x, 2) ignore nulls OVER(PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING)",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5},
            new Object[]{null});
    }

    @Test
    public void testNthValueOverRangeModeOneRowFrames() throws Throwable {
        Object[] expected = new Object[]{1, 1};
        assertEvaluate(
            "nth_value(x, 1) OVER(ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING and CURRENT ROW)",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2});
    }

    @Test
    public void testNthValueWithIgnoreNullsOverRangeModeOneRowFrames() throws Throwable {
        Object[] expected = new Object[]{null, 1, 1};
        assertEvaluate(
            "nth_value(x, 1) ignore Nulls OVER(ORDER BY x nulls first RANGE BETWEEN UNBOUNDED PRECEDING and CURRENT ROW)",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{null},
            new Object[]{1},
            new Object[]{2});
    }

    @Test
    public void testNthValueOverRowsModeOneRowFrames() throws Throwable {
        Object[] expected = new Object[]{1, 1};
        assertEvaluate(
            "nth_value(x, 1) OVER(ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW)",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{1},
            new Object[]{2});
    }

    @Test
    public void testNthValueWithIgnoreNullsOverRowsModeOneRowFrames() throws Throwable {
        Object[] expected = new Object[]{null, 1, 1};
        assertEvaluate(
            "nth_value(x, 1) ignore nulls OVER(ORDER BY x nulls first ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW)",
            expected,
            List.of(ColumnIdent.of("x")),
            new Object[]{null},
            new Object[]{1},
            new Object[]{2});
    }

    @Test
    public void testLastValueIgnoringNullsEmptyOver() throws Throwable {
        assertEvaluate(
            "last_value(x) ignore nulls over()",
            new Object[] {null, null},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {null,1},
            new Object[] {null,2}
        );
        assertEvaluate(
            "last_value(x) ignore nulls over()",
            new Object[] {2, 2, 2, 2},
            List.of(ColumnIdent.of("x")),
            new Object[] {1,1},
            new Object[] {null,2},
            new Object[] {null,3},
            new Object[] {2,4}
        );
    }

    @Test
    public void testFirstValueIgnoringNullsEmptyOver() throws Throwable {
        assertEvaluate(
            "first_value(x) ignore nulls over()",
            new Object[] {null, null},
            List.of(ColumnIdent.of("x")),
            new Object[] {null},
            new Object[] {null}
        );
        assertEvaluate(
            "first_value(x) ignore nulls over()",
            new Object[] {1, 1, 1, 1},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {1,1},
            new Object[] {null,2},
            new Object[] {2,3},
            new Object[] {null,4}
        );
    }

    @Test
    public void testNthValueIgnoringNullsEmptyOver() throws Throwable {
        assertEvaluate(
            "nth_value(x,3) ignore nulls over()",
            new Object[] {null, null},
            List.of(ColumnIdent.of("x")),
            new Object[] {null},
            new Object[] {null}
        );
        assertEvaluate(
            "nth_value(x,1) ignore nulls over()",
            new Object[] {1, 1},
            List.of(ColumnIdent.of("x")),
            new Object[] {null},
            new Object[] {1}
        );
        assertEvaluate(
            "nth_value(x,2) ignore nulls over()",
            new Object[] {2, 2, 2, 2},
            List.of(ColumnIdent.of("x")),
            new Object[] {1},
            new Object[] {null},
            new Object[] {2},
            new Object[] {null}
        );
    }

    @Test
    public void testNthValueIgnoringNullsAndWindowingClause() throws Throwable {
        assertEvaluate(
            "nth_value(x,1) ignore nulls over(order by y range between current row and unbounded following)",
            new Object[] {1, 2, 2, 3},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {1,1},
            new Object[] {null,2},
            new Object[] {2,3},
            new Object[] {3,4}
        );
        assertEvaluate(
            "nth_value(x,2) ignore nulls over(order by y range between current row and unbounded following)",
            new Object[] {2, 3, 3, null},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {1,1},
            new Object[] {null,2},
            new Object[] {2,3},
            new Object[] {3,4}
        );
        assertEvaluate(
            "nth_value(x,1) ignore nulls over(order by y range between current row and current row)",
            new Object[] {1, null, 2, null},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {1,1},
            new Object[] {null,2},
            new Object[] {2,3},
            new Object[] {null,4}
        );
    }

    @Test
    public void testFirstValueIgnoringNullsAndWindowingClause() throws Throwable {
        assertEvaluate(
            "first_value(x) ignore nulls over(order by y range between current row and unbounded following)",
            new Object[] {1, 2, 2, 3},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {1,1},
            new Object[] {null,2},
            new Object[] {2,3},
            new Object[] {3,4}
        );
        assertEvaluate(
            "first_value(x) ignore nulls over(order by y range between unbounded preceding and unbounded following)",
            new Object[] {2, 2, 2, 2},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {null,1},
            new Object[] {null,2},
            new Object[] {2,3},
            new Object[] {null,4}
        );
    }

    @Test
    public void testLastValueIgnoringNullsAndWindowingClause() throws Throwable {
        assertEvaluate(
            "last_value(x) ignore nulls over(order by y range between unbounded preceding and current row)",
            new Object[] {null, 1, 1, 1, 3},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {null,1},
            new Object[] {1,2},
            new Object[] {null,3},
            new Object[] {null,4},
            new Object[] {3,5}
        );
        assertEvaluate(
            "last_value(x) ignore nulls over(order by y range between unbounded preceding and unbounded following)",
            new Object[] {2, 2, 2, 2},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {null,1},
            new Object[] {null,2},
            new Object[] {2,3},
            new Object[] {null,4}
        );
    }

    @Test
    public void testFirstValueRespectNulls() throws Throwable {
        assertEvaluate(
            "first_value(x) respect nulls over()",
            new Object[] {null, null, null, null},
            List.of(ColumnIdent.of("x"), ColumnIdent.of("y")),
            new Object[] {null,1},
            new Object[] {1,2},
            new Object[] {null,3},
            new Object[] {null,4}
        );
    }
}
