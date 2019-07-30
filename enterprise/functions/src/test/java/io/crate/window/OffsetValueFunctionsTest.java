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

package io.crate.window;

import io.crate.execution.engine.window.AbstractWindowFunctionTest;
import io.crate.metadata.ColumnIdent;
import io.crate.module.EnterpriseFunctionsModule;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;

public class OffsetValueFunctionsTest extends AbstractWindowFunctionTest {

    public OffsetValueFunctionsTest() {
        super(new EnterpriseFunctionsModule());
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
}
