/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.join;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class NestedLoopTest extends RandomizedTest {

    @Test
    public void testNoRows() throws Exception {
        NestedLoop loop = new NestedLoop(new Object[0][], new Object[0][], 100);
        for (Object[] row : loop) {
            fail("NestedLoop did produce some rows though it shouldnt");
        }

        NestedLoop loopNoLeft = new NestedLoop(new Object[0][],
                new Object[][] {
                        new Object[]{1,2,3},
                        new Object[]{4,5,6}
                }, 100);
        for (Object[] row : loopNoLeft) {
            fail("NestedLoop did produce some rows though it shouldnt");
        }

        NestedLoop loopNoRight = new NestedLoop(
                new Object[][] {
                        new Object[]{1,2,3},
                        new Object[]{4,5,6}
                },
                new Object[0][],
                100);
        for (Object[] row : loopNoRight) {
            fail("NestedLoop did produce some rows though it shouldnt");
        }
    }

    @Test
    public void testLimit0() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(1, 2);

        assertNestedLoop(left, right, 0, 0);
    }

    @Test
    public void testNoLimitRightNoRows() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(0, 2);

        assertNestedLoop(left, right, -1, 0);
    }

    @Test
    public void testNoLimitLeftNoRows() throws Exception {
        Object[][] left = randomRows(0, 4);
        Object[][] right = randomRows(4, 2);

        assertNestedLoop(left, right, -1, 0);
    }

    @Test
    public void testLimit1() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(1, 2);

        assertNestedLoop(left, right, 1, 1);
    }

    @Test
    public void testEmptyRows() throws Exception {
        Object[][] left = new Object[][]{
                new Object[0],
                new Object[0],
                new Object[0]
        };
        Object[][] right = new Object[][]{
                new Object[0],
                new Object[0]
        };
        assertNestedLoop(left, right, 10, 6);
    }

    private void assertNestedLoop(Object[][] left, Object[][] right, int limit, int expectedRows) {
        NestedLoop loopLimit1 = new NestedLoop(left, right, limit);
        int i = 0;
        int leftIdx = 0;
        int rightIdx = 0;
        for (Object[] row : loopLimit1) {
            int rowIdx = 0;
            if (rightIdx == right.length) {
                rightIdx = 0;
                leftIdx++;
            }
            assertThat(row.length, is(left[leftIdx].length + right[rightIdx].length));


            for (int j = 0; j < left[leftIdx].length; j++) {
                assertThat(row[rowIdx], is(left[leftIdx][rowIdx]));
                rowIdx++;
            }
            for (int j = 0; j < right[rightIdx].length; j++) {
                assertThat(row[rowIdx], is(right[rightIdx][j]));
                rowIdx++;
            }

            i++;
            rightIdx++;
        }
        assertThat(i, is(expectedRows));
    }

    @Test
    public void testLimitBetween() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(3, 2);

        assertNestedLoop(left, right, 4, 4);
    }

    @Test
    public void testLimitSomeMore() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(3, 2);

        assertNestedLoop(left, right, 32, 30);
    }

    @Test
    public void testLimitNoLimit() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(3, 2);

        assertNestedLoop(left, right, -1, 30);
    }

    private Object[][] randomRows(int numRows, int rowLength) {
        Object[][] rows = new Object[numRows][];
        for (int i = 0; i < numRows; i++) {
            rows[i] = randomRow(rowLength);
        }
        return rows;
    }

    private Object[] randomRow(int length) {
        Object[] row = new Object[length];
        for (int i = 0; i < length; i++) {
            switch (randomByte() % 4) {
                case 0:
                    row[i] = randomInt();
                    break;
                case 1:
                    row[i] = randomAsciiOfLength(10);
                    break;
                case 2:
                    row[i] = null;
                    break;
                case 3:
                    row[i] = (randomBoolean() ? -1 : 1) * randomDouble();
                    break;
            }
        }
        return row;
    }
}
