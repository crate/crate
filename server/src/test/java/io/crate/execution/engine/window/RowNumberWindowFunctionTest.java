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

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;

public class RowNumberWindowFunctionTest extends AbstractWindowFunctionTest {

    @Test
    public void testRowNumberFunction() throws Throwable {
        Object[] expected = new Object[] {1, 2, 3, 4};

        assertEvaluate("row_number() over(order by x)",
                       expected,
                       List.of(ColumnIdent.of("x")),
                       new Object[] {4},
                       new Object[] {3},
                       new Object[] {2},
                       new Object[] {1}
        );
    }

    @Test
    public void testRowNumberOverPartitionedWindow() throws Throwable {
        Object[] expected = new Object[]{1, 2, 3, 1, 2, 3, 1};
        assertEvaluate("row_number() over(partition by x>2)",
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
    public void testRowNumberOverPartitionedOrderedWindow() throws Throwable {
        Object[] expected = new Object[]{1, 2, 3, 1, 2, 3, 1};
        assertEvaluate("row_number() over(partition by x>2 order by x)",
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
    public void testRowNumberOverUnboundedFollowingFrames() throws Throwable {
        Object[] expected = new Object[]{1, 2, 3, 1, 2, 3, 1};
        assertEvaluate("row_number() OVER(" +
                            "PARTITION BY x>2 ORDER BY x RANGE BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING" +
                       ")",
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
    public void testIgnoreNullsFlagThrows() {
        Assertions.assertThatThrownBy(() -> assertEvaluate(
                "row_number() ignore nulls over()",
                null,
                List.of(ColumnIdent.of("x")),
                new Object[] {1}
            ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("row_number cannot accept RESPECT or IGNORE NULLS flag.");
    }
}
