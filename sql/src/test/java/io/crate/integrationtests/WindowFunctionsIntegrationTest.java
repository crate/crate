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

package io.crate.integrationtests;

import org.junit.Test;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;

public class WindowFunctionsIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testAvgOnEmptyOver() {
        execute("select avg(col1) OVER() from unnest([1, 2, null])");
        assertThat(printedTable(response.rows()), is("1.5\n1.5\n1.5\n"));
    }

    @Test
    public void testMultipleWindowFunctions() {
        execute("select col1, sum(col1) OVER(ORDER BY col1), sum(col2) OVER(ORDER BY col2) from unnest([1, 2, 2, 3, 4], [5, 6, 6, 7, 1])");
        assertThat(printedTable(response.rows()), is("4| 12| 1\n1| 1| 6\n2| 5| 18\n2| 5| 18\n3| 8| 25\n"));
    }

    @Test
    public void testOrderedWindow() {
        execute("select col1, avg(col1) OVER(ORDER BY col1 NULLS LAST) from unnest([2, 1, 1, 3, 3, null, 4]) order by 1 desc");
        assertThat(printedTable(response.rows()), is("NULL| 2.3333333333333335\n" +
                                                     "4| 2.3333333333333335\n" +
                                                     "3| 2.0\n" +
                                                     "3| 2.0\n" +
                                                     "2| 1.3333333333333333\n" +
                                                     "1| 1.0\n" +
                                                     "1| 1.0\n"));
    }

    @Test
    public void testOrderedWindowByMultipleColumns() {
        execute("select col1, sum(col1) OVER(ORDER BY col1, col2) from unnest([1, 2, 2, 2, 3, 2], [6, 7, 6, 9, -1, 6])");
        assertThat(printedTable(response.rows()), is("1| 1\n" +
                                                     "2| 5\n" +
                                                     "2| 5\n" +
                                                     "2| 7\n" +
                                                     "2| 9\n" +
                                                     "3| 12\n"));
    }

    @Test
    public void testOrderedWindowWithSingleRowWindows() {
        execute("select col1, sum(col1) OVER(ORDER BY col1) from unnest([1, 2, 3, 4])");
        assertThat(printedTable(response.rows()), is("1| 1\n" +
                                                     "2| 3\n" +
                                                     "3| 6\n" +
                                                     "4| 10\n"));
    }

    @Test
    public void testPartitionedWindow() {
        execute("select col1, col2, sum(col1) over(partition by col1) FROM " +
                "unnest([1, 2, 1, 1, 1, 4], [6, 6, 9, 6, 7, 8]) order by 1, 2, 3");
        assertThat(printedTable(response.rows()), is("1| 6| 4\n" +
                                                     "1| 6| 4\n" +
                                                     "1| 7| 4\n" +
                                                     "1| 9| 4\n" +
                                                     "2| 6| 2\n" +
                                                     "4| 8| 4\n"));
    }

    @Test
    public void testPartitionByMultipleColumns() {
        execute("select col1, col2, row_number() over(partition by col1, col2) FROM " +
                "unnest([1, 2, 1, 1, 1, 4], [6, 6, 9, 6, 7, 8]) order by 1, 2, 3");
        assertThat(printedTable(response.rows()), is("1| 6| 1\n" +
                                                     "1| 6| 2\n" +
                                                     "1| 7| 1\n" +
                                                     "1| 9| 1\n" +
                                                     "2| 6| 1\n" +
                                                     "4| 8| 1\n"));
    }

    @Test
    public void testPartitionedOrderedWindow() {
        execute("select col1, col2, sum(col1) over(partition by col1 order by col2) FROM " +
                "unnest([1, 2, 1, 1, 1, 4], [6, 6, 9, 6, 7, 8]) order by 1, 2, 3");
        assertThat(printedTable(response.rows()), is("1| 6| 2\n" +
                                                     "1| 6| 2\n" +
                                                     "1| 7| 3\n" +
                                                     "1| 9| 4\n" +
                                                     "2| 6| 2\n" +
                                                     "4| 8| 4\n"));
    }

    @Test
    public void testSelectStandaloneColumnsAndWindowFunction() {
        execute("select col1, avg(col1) OVER(), col2 from unnest([1, 2, null], [3, 4, 5])");
        assertThat(printedTable(response.rows()), is("1| 1.5| 3\n" +
                                                     "2| 1.5| 4\n" +
                                                     "NULL| 1.5| 5\n"));
    }

    @Test
    public void testRowNumberOnEmptyOver() {
        execute("select col1, row_number() OVER() from unnest(['a', 'c', 'd', 'b'])");
        assertThat(printedTable(response.rows()), is("a| 1\n" +
                                                     "c| 2\n" +
                                                     "d| 3\n" +
                                                     "b| 4\n"));
    }

    @Test
    public void testRowNumberWithOrderByClauseNoPeers() {
        execute("select col1, row_number() OVER(ORDER BY col1) from unnest(['a', 'c', 'd', 'b'])");
        assertThat(printedTable(response.rows()), is("a| 1\n" +
                                                     "b| 2\n" +
                                                     "c| 3\n" +
                                                     "d| 4\n"));
    }

    @Test
    public void testRowNumberWithOrderByClauseHavingPeers() {
        execute("select col1, row_number() OVER(ORDER BY col1) from unnest(['a', 'c', 'c', 'd', 'b'])");
        assertThat(printedTable(response.rows()), is("a| 1\n" +
                                                     "b| 2\n" +
                                                     "c| 3\n" +
                                                     "c| 4\n" +
                                                     "d| 5\n"));
    }
}
