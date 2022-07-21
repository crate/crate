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

import static org.hamcrest.Matchers.contains;

import java.util.List;

import org.junit.Test;

import io.crate.execution.engine.window.AbstractWindowFunctionTest;
import io.crate.metadata.ColumnIdent;
import io.crate.module.ExtraFunctionsModule;
import io.crate.testing.Asserts;


public class RankFunctionsTest extends AbstractWindowFunctionTest {

    public RankFunctionsTest() {
        super(new ExtraFunctionsModule());
    }

    @Test
    public void testRankWithEmptyOver() throws Throwable {
        assertEvaluate(
            "rank() over()",
            contains(new Object[] {1, 1, 1, 1, 1}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {2, 1},
            new Object[] {1, 1},
            new Object[] {1, 0},
            new Object[] {2, 1}
        );
    }


    @Test
    public void testRankWithOrderByClause() throws Throwable {
        assertEvaluate(
            "rank() over(order by x)",
            contains(new Object[] {1, 1, 1, 4, 4}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {2, 1},
            new Object[] {1, 1},
            new Object[] {1, 0},
            new Object[] {2, 1}
        );
    }

    @Test
    public void testRankUseSymbolMultipleTimes() throws Throwable {
        assertEvaluate(
            "rank() over(order by y, x)",
            contains(new Object[] {1, 2, 2, 4, 4}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {2, 1},
            new Object[] {1, 1},
            new Object[] {1, 0},
            new Object[] {2, 1}
        );
    }

    @Test
    public void testRankOverPartitionedWindow() throws Throwable {
        Object[] expected = new Object[]{1, 1, 1, 1, 1, 1};
        assertEvaluate(
            "rank() over(partition by y > 0)",
            contains(expected),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {2, 1},
            new Object[] {3, 1},
            new Object[] {1, 0},
            new Object[] {2, 0},
            new Object[] {3, 0});
    }

    @Test
    public void testRankOverPartitionedOrderedWindow() throws Throwable {
        Object[] expected = new Object[]{1, 2, 3, 1, 2, 3};
        assertEvaluate(
            "rank() over(partition by y > 0 order by x)",
            contains(expected),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {2, 1},
            new Object[] {3, 1},
            new Object[] {1, 0},
            new Object[] {2, 0},
            new Object[] {3, 0});
    }

    @Test
    public void testDenseRankWithEmptyOver() throws Throwable {
        assertEvaluate(
            "dense_rank() over()",
            contains(new Object[] {1, 1, 1, 1, 1}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {2, 1},
            new Object[] {1, 1},
            new Object[] {1, 0},
            new Object[] {2, 1}
        );
    }


    @Test
    public void testDenseRankWithOrderByClause() throws Throwable {
        assertEvaluate(
            "dense_rank() over(order by x)",
            contains(new Object[] {1, 1, 1, 2, 2}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {2, 1},
            new Object[] {1, 1},
            new Object[] {1, 0},
            new Object[] {2, 1}
        );
    }

    @Test
    public void testDenseRankUseSymbolMultipleTimes() throws Throwable {
        assertEvaluate(
            "dense_rank() over(order by y, x)",
            contains(new Object[] {1, 2, 2, 3, 3}),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {2, 1},
            new Object[] {1, 1},
            new Object[] {1, 0},
            new Object[] {2, 1}
        );
    }

    @Test
    public void testDenseRankOverPartitionedWindow() throws Throwable {
        Object[] expected = new Object[]{1, 1, 1, 1, 1, 1};
        assertEvaluate(
            "dense_rank() over(partition by y > 0)",
            contains(expected),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {2, 1},
            new Object[] {3, 1},
            new Object[] {1, 0},
            new Object[] {2, 0},
            new Object[] {3, 0});
    }

    @Test
    public void testDenseRankOverPartitionedOrderedWindow() throws Throwable {
        Object[] expected = new Object[]{1, 2, 3, 1, 2, 3};
        assertEvaluate(
            "dense_rank() over(partition by y > 0 order by x)",
            contains(expected),
            List.of(new ColumnIdent("x"), new ColumnIdent("y")),
            new Object[] {1, 1},
            new Object[] {2, 1},
            new Object[] {3, 1},
            new Object[] {1, 0},
            new Object[] {2, 0},
            new Object[] {3, 0});
    }

    @Test
    public void testIgnoreNullsFlagThrows() {
        Asserts.assertThrowsMatches(
            () -> assertEvaluate(
                "rank() ignore nulls over()",
                null,
                List.of(new ColumnIdent("x")),
                new Object[] {1}
            ),
            IllegalArgumentException.class,
            "rank cannot accept RESPECT or IGNORE NULLS flag."
        );
    }
}
