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

import java.util.Collections;
import java.util.Map;

import static io.crate.analyze.WindowDefinition.RANGE_CURRENT_ROW_UNBOUNDED_FOLLOWING;
import static org.hamcrest.Matchers.contains;

public class OffsetValueFunctionsTest extends AbstractWindowFunctionTest {

    public OffsetValueFunctionsTest() {
        super(new EnterpriseFunctionsModule());
    }

    @Test
    public void testLagWithSingleArgumentAndEmptyOver() throws Exception {
        assertEvaluate("lag(x) over()",
            contains(new Object[]{null, 1, null, 2}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            new Object[]{1},
            new Object[]{null},
            new Object[]{2},
            new Object[]{3}
        );
    }

    @Test
    public void testLagWithOffsetAndEmptyOver() throws Exception {
        assertEvaluate("lag(x, 2) over()",
            contains(new Object[]{null, null, 1, 2}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        );
    }

    @Test
    public void testLagWithNullOffset() throws Exception {
        assertEvaluate("lag(x, null) over()",
            contains(new Object[]{null, null, null}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            new Object[]{1},
            new Object[]{null},
            new Object[]{2}
        );
    }

    @Test
    public void testLagZeroOffsetAndEmptyOver() throws Exception {
        assertEvaluate("lag(x, 0) over()",
            contains(new Object[]{1, 2}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            new Object[]{1},
            new Object[]{2}
        );
    }

    @Test
    public void testLagNegativeOffsetAndEmptyOver() throws Exception {
        assertEvaluate("lag(x, -1) over()",
            contains(new Object[]{2, null}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            new Object[]{1},
            new Object[]{2}
        );
    }

    @Test
    public void testLagWithDefaultValueAndEmptyOver() throws Exception {
        assertEvaluate("lag(x, 1, -1) over()",
            contains(new Object[]{-1, 1, 2, 3}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        );
    }

    @Test
    public void testLagWithExpressionAsArgumentAndEmptyOver() throws Exception {
        assertEvaluate("lag(coalesce(x, 1), 1, -1) over()",
            contains(new Object[]{-1, 1, 1}),
            Map.of(new ColumnIdent("x"), 0),
            new Object[]{1},
            new Object[]{null},
            new Object[]{2}
        );
    }

    @Test
    public void testLagWithOrderBy() throws Exception {
        assertEvaluate("lag(x) over(order by y)",
            contains(new Object[]{null, 1, 3, 1}),
            Map.of(new ColumnIdent("x"), 0, new ColumnIdent("y"), 1),
            new Object[]{1, 1},
            new Object[]{1, 3},
            new Object[]{3, 2},
            new Object[]{2, 5}
        );
    }

    @Test
    public void testLagWithOrderByReferenceUsedInFunction() throws Exception {
        assertEvaluate("lag(x) over(order by y, x)",
            contains(new Object[]{null, 1, 1, 3}),
            Map.of(new ColumnIdent("x"), 0, new ColumnIdent("y"), 1),
            new Object[]{1, 1},
            new Object[]{1, 2},
            new Object[]{3, 2},
            new Object[]{2, 3}
        );
    }

    @Test
    public void testLagOverPartitionedWindow() throws Exception {
        assertEvaluate("lag(x) over(partition by x > 2)",
            contains(new Object[]{null, 1, 2, null, 3, 4}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5});
    }

    @Test
    public void testLagOperatesOnPartitionAndIgnoresFrameRange() throws Exception {
        assertEvaluate("lag(x) over(RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING)",
            contains(new Object[]{null, 1, 2, 3}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            RANGE_CURRENT_ROW_UNBOUNDED_FOLLOWING,
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        );
    }

    @Test
    public void testLeadOverPartitionedWindow() throws Exception {
        assertEvaluate("lead(x) over(partition by x > 2)",
            contains(new Object[]{2, 2, null, 4, 5, null}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5});
    }

    @Test
    public void testLeadOverCurrentRowUnboundedFollowingWithDefaultValue() throws Exception {
        assertEvaluate("lead(x, 2, 42) over(RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING)",
            contains(new Object[]{2, 3, 4, 42, 42}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            RANGE_CURRENT_ROW_UNBOUNDED_FOLLOWING,
            new Object[]{1},
            new Object[]{2},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        );
    }
}
