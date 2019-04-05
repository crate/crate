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
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.contains;


public class NthValueFunctionsTest extends AbstractWindowFunctionTest {

    public NthValueFunctionsTest() {
        super(new EnterpriseFunctionsModule());
    }

    @Test
    public void testLastValueWithEmptyOver() throws Exception {
        assertEvaluate("last_value(x) over()",
            contains(new Object[] {4, 4, 4, 4}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            new int[] {0},
            new Object[] {1},
            new Object[] {2},
            new Object[] {3},
            new Object[] {4}
        );
    }

    @Test
    public void testLastValueWithOrderByClause() throws Exception {
        Map<ColumnIdent, Integer> mapping = new HashMap<>();
        mapping.put(new ColumnIdent("x"), 0);
        mapping.put(new ColumnIdent("y"), 1);

        assertEvaluate("last_value(x) over(order by y)",
            contains(new Object[] {1, 3, 3, 2}),
            mapping,
            new int[] {1},
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testLastValueUseSymbolMultipleTimes() throws Exception {
        Map<ColumnIdent, Integer> mapping = new HashMap<>();
        mapping.put(new ColumnIdent("x"), 0);
        mapping.put(new ColumnIdent("y"), 1);

        assertEvaluate("last_value(x) over(order by y, x)",
            contains(new Object[] {1, 1, 3, 2}),
            mapping,
            new int[] {1, 0},
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testFirstValueWithEmptyOver() throws Exception {
        assertEvaluate("first_value(x) over()",
            contains(new Object[] {1, 1, 1, 1}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            new int[] {0},
            new Object[] {1},
            new Object[] {2},
            new Object[] {3},
            new Object[] {4}
        );
    }

    @Test
    public void testFirstValueWithOrderByClause() throws Exception {
        Map<ColumnIdent, Integer> mapping = new HashMap<>();
        mapping.put(new ColumnIdent("x"), 0);
        mapping.put(new ColumnIdent("y"), 1);

        assertEvaluate("first_value(x) over(order by y)",
            contains(new Object[] {1, 1, 1, 1}),
            mapping,
            new int[] {1},
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

        @Test
    public void testNthValueWithEmptyOver() throws Exception {
        assertEvaluate("nth_value(x, 3) over()",
            contains(new Object[] {3, 3, 3, 3}),
            Collections.singletonMap(new ColumnIdent("x"), 0),
            new int[] {},
            new Object[] {1},
            new Object[] {2},
            new Object[] {3},
            new Object[] {4}
        );
    }

    @Test
    public void testNthValueWithOrderByClause() throws Exception {
        Map<ColumnIdent, Integer> mapping = new HashMap<>();
        mapping.put(new ColumnIdent("x"), 0);
        mapping.put(new ColumnIdent("y"), 1);

        assertEvaluate("nth_value(x,3) over(order by y)",
            contains(new Object[] {null, 3, 3, 3}),
            mapping,
            new int[] {1},
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testNthValueWithNullPositionReturnsNull() throws Exception {
        Map<ColumnIdent, Integer> mapping = new HashMap<>();
        mapping.put(new ColumnIdent("x"), 0);
        mapping.put(new ColumnIdent("y"), 1);

        assertEvaluate("nth_value(x,null) over(order by y)",
            contains(new Object[] {null, null, null, null}),
            mapping,
            new int[] {1},
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {3, 2},
            new Object[] {2, 3}
        );
    }

    @Test
    public void testRowNthValueOverPartitionedWindow() {
        Object[] expected = new Object[]{2, 2, 2, 4, 4, 4, null};
        assertEvaluate("nth_value(x, 2) over(partition by x>2)",
                       contains(expected),
                       Collections.singletonMap(new ColumnIdent("x"), 0),
                       null,
                       new Object[]{1, 1},
                       new Object[]{2, 2},
                       new Object[]{2, 2},
                       new Object[]{3, 3},
                       new Object[]{4, 4},
                       new Object[]{5, 5},
                       new Object[]{null, null});
    }

    @Test
    public void testNthValueOverPartitionedOrderedWindow() {
        Object[] expected = new Object[]{null, 2, 2, null, 4, 4, null};
        assertEvaluate("nth_value(x, 2) over(partition by x>2 order by x)",
                       contains(expected),
                       Collections.singletonMap(new ColumnIdent("x"), 0),
                       new int[]{0},
                       new Object[]{1, 1},
                       new Object[]{2, 2},
                       new Object[]{2, 2},
                       new Object[]{3, 3},
                       new Object[]{4, 4},
                       new Object[]{5, 5},
                       new Object[]{null, null});
    }
}
