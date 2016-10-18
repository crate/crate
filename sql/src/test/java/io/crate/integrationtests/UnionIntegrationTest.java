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

import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
@UseJdbc
public class UnionIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testUnionAll2SameTable() {
        createColorsAndSizes();
        execute("select * from sizes " +
                "union all " +
                "select * from sizes");
        assertThat(Arrays.asList(response.rows()),
                   containsInAnyOrder(
                       new Object[]{1, "small", "size2"},
                       new Object[]{2, "large", "size1"},
                       new Object[]{1, "small", "size2"},
                       new Object[]{2, "large", "size1"}));
    }

    @Test
    public void testUnionAll2Tables() {
        createColorsAndSizes();
        execute("select color from colors " +
                "union all " +
                "select size from sizes " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("blue\n" +
                                                                    "green\n" +
                                                                    "large\n" +
                                                                    "red\n" +
                                                                    "small\n"));
    }

    @Test
    public void testUnionAll3Tables() {
        createColorsAndSizes();
        execute("select size from sizes " +
                "union all " +
                "select color from colors " +
                "union all " +
                "select size from sizes " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("blue\n" +
                                                                    "green\n" +
                                                                    "large\n" +
                                                                    "large\n" +
                                                                    "red\n" +
                                                                    "small\n" +
                                                                    "small\n"));
    }

    @Test
    public void testUnionAllWithOrderBy() {
        createColorsAndSizes();
        execute("select color from colors " +
                "union all " +
                "select size from sizes " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("blue\n" +
                                                                    "green\n" +
                                                                    "large\n" +
                                                                    "red\n" +
                                                                    "small\n"));
    }

    @Test
    public void testUnionAllWithOrderByWithFunctions() {
        createColorsAndSizes();
        execute("select color, lower(txt) from colors " +
                "union all " +
                "select upper(size), txt from sizes " +
                "order by upper(lower(lower(txt))) || '_' || upper(color)");
        assertThat(TestingHelpers.printedTable(response.rows()), is("blue| color1\n" +
                                                                    "green| color2\n" +
                                                                    "red| color3\n" +
                                                                    "LARGE| size1\n" +
                                                                    "SMALL| size2\n"));
    }

    @Test
    public void testUnionAllWithPrimaryKeys() {
        createColorsAndSizes();
        execute("select color from colors where id = 1 " +
                "union all " +
                "select size from sizes where id = 1 " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("red\nsmall\n"));
    }

    @Test
    public void testUnionAllWithPartitionedTables() {
        createColorsAndSizesPartitioned();
        execute("select color from colors where id = 2 " +
                "union all " +
                "select size from sizes where size = 'small' " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("blue\n" +
                                                                    "small\n"));
    }

    @Test
    public void testUnionAllWithCount() {
        createColorsAndSizes();
        execute("select count(*) from colors " +
                "union all " +
                "select count(*) from sizes " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("2\n" +
                                                                    "3\n"));
    }

    @Test
    public void testUnionAllWithGlobalAggregation() {
        createColorsAndSizes();
        execute("select sum(id) from colors " +
                "union all " +
                "select sum(id) from sizes " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("3.0\n" +
                                                                    "6.0\n"));
    }

    @Test
    public void testUnionAllWithGroupBy() {
        createColorsAndSizes();
        execute("select count(id), color from colors group by color " +
                "union all " +
                "select count(id), size from sizes group by size " +
                "order by 2");
        assertThat(TestingHelpers.printedTable(response.rows()), is("1| blue\n" +
                                                                    "1| green\n" +
                                                                    "1| large\n" +
                                                                    "1| red\n" +
                                                                    "1| small\n"));
    }

    @Test
    public void testUnionAllWithSubSelect() {
        createColorsAndSizes();
        execute("select color from (select * from colors limit 2) a " +
                "union all " +
                "select size from (select * from sizes limit 1) b " +
                "order by 1 " +
                "limit 10 offset 2");
        assertThat(TestingHelpers.printedTable(response.rows()), is("large\n"));
    }

    @Test
    public void testUnionAllWithCrossJoin() {
        createColorsAndSizes();
        execute("select colors.color from sizes, colors " +
                "union all " +
                "select size from sizes " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("blue\n" +
                                                                    "blue\n" +
                                                                    "green\n" +
                                                                    "green\n" +
                                                                    "large\n" +
                                                                    "red\n" +
                                                                    "red\n" +
                                                                    "small\n"));
    }

    @Test
    public void testUnionAllWithInnerJoin() {
        createColorsAndSizes();
        execute("select colors.color from colors inner join sizes on colors.id = sizes.id " +
                "union all " +
                "select size from sizes " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("blue\n" +
                                                                    "large\n" +
                                                                    "red\n" +
                                                                    "small\n"));
    }

    @Test
    public void testUnionAllWithLeftOuterJoin() {
        createColorsAndSizes();
        execute("select colors.color from colors left join sizes on colors.color = sizes.size " +
                "union all " +
                "select size from sizes " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("blue\n" +
                                                                    "green\n" +
                                                                    "large\n" +
                                                                    "red\n" +
                                                                    "small\n"));
    }

    @Test
    public void testUnionAllWithRightOuterJoin() {
        createColorsAndSizes();
        execute("select colors.color from sizes right join colors on colors.color = sizes.size " +
                "union all " +
                "select size from sizes " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("blue\n" +
                                                                    "green\n" +
                                                                    "large\n" +
                                                                    "red\n" +
                                                                    "small\n"));
    }

    @Test
    public void testUnionWithSystemTables() {
        List<String> results = new ArrayList<>();
        execute("select name from sys.nodes");
        for (Object[] row : response.rows()) {
            results.add((String) row[0]);
        }
        execute("select name from sys.cluster");
        for (Object[] row : response.rows()) {
            results.add((String) row[0]);
        }
        Collections.sort(results);
        StringBuilder sb = new StringBuilder();
        for (String str : results) {
            sb.append(str).append('\n');
        }

        execute("select name from sys.nodes " +
                "union all " +
                "select name from sys.cluster " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is(sb.toString()));
    }

    private void createColorsAndSizes() {
        execute("create table colors (id integer primary key, color string, txt string)");
        execute("create table sizes (id integer primary key, size string, txt string)");
        ensureYellow();

        execute("insert into colors (id, color, txt) values (?, ?, ?)", new Object[][]{
            new Object[]{1, "red", "color3"},
            new Object[]{2, "blue", "color1"},
            new Object[]{3, "green", "color2"}
        });
        execute("insert into sizes (id, size, txt) values (?, ?, ?)", new Object[][]{
            new Object[]{1, "small", "size2"},
            new Object[]{2, "large", "size1"},
            });
        execute("refresh table colors, sizes");
    }

    private void createColorsAndSizesPartitioned() {
        execute("create table colors (id integer primary key, color string) partitioned by (id)");
        execute("create table sizes (id integer, size string) partitioned by (id)");
        ensureYellow();

        execute("insert into colors (id, color) values (?, ?)", new Object[][]{
            new Object[]{1, "red"},
            new Object[]{2, "blue"},
            new Object[]{3, "green"}
        });
        execute("insert into sizes (id, size) values (?, ?)", new Object[][]{
            new Object[]{1, "small"},
            new Object[]{2, "large"},
            });
        execute("refresh table colors, sizes");
    }
}
