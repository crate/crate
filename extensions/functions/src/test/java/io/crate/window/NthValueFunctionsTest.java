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


public class NthValueFunctionsTest extends AbstractWindowFunctionTest {

    public NthValueFunctionsTest() {
        super(new ExtraFunctionsModule());
    }

    @Test
    public void testLastValueWithEmptyOver() throws Throwable {
        assertEvaluate(
            "last_value(x) over()",
            contains(new Object[] {4, 4, 4, 4}),
            List.of(new ColumnIdent("x")),
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
            contains(new Object[] {1, 3, 3, 2}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
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
            contains(new Object[] {1, 1, 3, 2}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testFirstValueWithEmptyOver() throws Throwable {
        assertEvaluate(
            "first_value(x) over()",
            contains(new Object[] {1, 1, 1, 1}),
            List.of(new ColumnIdent("x")),
            new Object[] {1},
            new Object[] {2},
            new Object[] {3},
            new Object[] {4}
        );
    }

    @Test
    public void testFirstValueWithOrderByClause() throws Throwable {
        assertEvaluate("first_value(x) over(order by y)",
            contains(new Object[] {1, 1, 1, 1}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

        @Test
    public void testNthValueWithEmptyOver() throws Throwable {
        assertEvaluate("nth_value(x, 3) over()",
            contains(new Object[] {3, 3, 3, 3}),
            List.of(new ColumnIdent("x")),
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
            contains(new Object[] {null, 3, 3, 3}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testNthValueWithNullPositionReturnsNull() throws Throwable {
        assertEvaluate("nth_value(x,null) over(order by y)",
            contains(new Object[] {null, null, null, null}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testRowNthValueOverPartitionedWindow() throws Throwable {
        Object[] expected = new Object[]{2, 2, 2, 4, 4, 4, null};
        assertEvaluate(
            "nth_value(x, 2) over(partition by x > 2)",
            contains(expected),
            List.of(new ColumnIdent("x")),
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
            contains(expected),
            List.of(new ColumnIdent("x")),
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
            contains(expected),
            List.of(new ColumnIdent("x")),
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
            contains(expected),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{2});
    }

    @Test
    public void testNthValueOverRowsModeOneRowFrames() throws Throwable {
        Object[] expected = new Object[]{1, 1};
        assertEvaluate(
            "nth_value(x, 1) OVER(ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW)",
            contains(expected),
            List.of(new ColumnIdent("x")),
            new Object[]{1},
            new Object[]{2});
    }
}
