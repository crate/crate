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

package io.crate.analyze;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static org.hamcrest.Matchers.is;

public class SQLPrinterTest extends CrateDummyClusterServiceUnitTest {

    private final String input;
    private final String expectedOutput;
    private SQLExecutor e;
    private SQLPrinter printer;

    public SQLPrinterTest(String input, String expectedOutput) {
        this.input = input;
        this.expectedOutput = expectedOutput;
    }

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int, \"user\" string)")
            .addTable("create table t2 (x int, \"name\" string)")
            .addTable("create table \"user\" (name string)")
            .addView(RelationName.fromIndexName("v1"), "select x, \"user\" from t1")
            .build();
        printer = new SQLPrinter(new SymbolPrinter(e.functions()));
    }

    @Test
    public void testPrintAndAnalyzeRoundTrip() {
        String actualOutputRound1 = printer.format(e.analyze(input));
        assertThat(actualOutputRound1, is(expectedOutput));
        // must be possible to analyze again without error
        String actualOutputRound2 = printer.format(e.analyze(actualOutputRound1));
        assertThat(actualOutputRound2, is(expectedOutput));
    }


    @ParametersFactory
    public static Iterable<Object[]> testParameters() {
        return Arrays.asList(
            // resolving star
            $("select * from t1",
                "SELECT doc.t1.\"user\", doc.t1.x FROM doc.t1"),

            // with WHERE
            $("select * from t1 where x > 1",
                "SELECT doc.t1.\"user\", doc.t1.x FROM doc.t1 WHERE (doc.t1.x > 1)"),

            // alias handling
            $("select x as y from t1",
                "SELECT doc.t1.x AS y FROM doc.t1"),

            // alias handling on function
            $("select x + x AS xx from t1",
                "SELECT (doc.t1.x + doc.t1.x) AS xx FROM doc.t1"),

            // with GROUP BY and HAVING
            $("select x, count(*) from t1 group by x having count(*) > 1",
                "SELECT doc.t1.x, count(*) FROM doc.t1 GROUP BY doc.t1.x HAVING (count(*) > 1)"),

            // with ORDER BY and LIMIT / OFFSET
            $("select x from t1 order by x limit 10 offset 5",
                "SELECT doc.t1.x FROM doc.t1 ORDER BY doc.t1.x ASC LIMIT 10 OFFSET 5"),

            // with scalar subquery
            $("select (select \"user\" from t1 limit 1), x from t1",
                "SELECT (SELECT doc.t1.\"user\" FROM doc.t1 LIMIT 1), doc.t1.x FROM doc.t1"),

            $("select 1", "SELECT 1 FROM empty_row()"),
            $("select * from unnest([1, 2])", "SELECT col1 FROM unnest([1, 2])"),
            $("select col1 as x from unnest([1, 2])", "SELECT col1 AS x FROM unnest([1, 2])"),
            $("select col1 as x from unnest([1, 2]) t", "SELECT col1 AS x FROM unnest([1, 2]) AS t"),

            // table name requires quotes
            $("select * from \"user\"", "SELECT doc.\"user\".name FROM doc.\"user\""),

            // UNION (simple)
            $("select * from t1 union all select * from t2",
                "SELECT doc.t1.\"user\", doc.t1.x FROM doc.t1 UNION ALL SELECT doc.t2.name, doc.t2.x FROM doc.t2"),
            // UNION (order by)
            $("select \"user\" from t1 union all select name from t2 order by \"user\"",
                "SELECT doc.t1.\"user\" FROM doc.t1 UNION ALL SELECT doc.t2.name FROM doc.t2 ORDER BY \"user\" ASC"),
            // UNION (order by / limit / offset)
            $("select * from t1 union all select * from t2 order by \"user\" limit 1 offset 5",
                "SELECT doc.t1.\"user\", doc.t1.x FROM doc.t1 UNION ALL SELECT doc.t2.name, doc.t2.x FROM doc.t2 ORDER BY \"user\" ASC LIMIT 1 OFFSET 5"),

            // VIEW (simple)
            $("select * from v1", "SELECT x, \"user\" FROM doc.v1"),
            // VIEW (order by)
            $("select * from v1 order by \"user\"", "SELECT x, \"user\" FROM doc.v1 ORDER BY \"user\" ASC"),
            // VIEW (order by / limit / offset)
            $("select * from v1 order by \"user\" limit 1 offset 5",
                "SELECT x, \"user\" FROM doc.v1 ORDER BY \"user\" ASC LIMIT 1 OFFSET 5"),
            // VIEW (group by / having)
            $("select x, count(*) from v1 group by x having count(*) > 1",
                "SELECT x, count(*) FROM doc.v1 GROUP BY x HAVING (count(*) > 1)"),

            // SUBQUERY (simple)
            $("select * from (select * from t1) a",
                "SELECT \"user\", x FROM (SELECT doc.t1.\"user\", doc.t1.x FROM doc.t1) a"),
            // SUBQUERY (order by)
            $("select * from (select * from t1) a order by \"user\"",
                "SELECT \"user\", x FROM (SELECT doc.t1.\"user\", doc.t1.x FROM doc.t1) a ORDER BY \"user\" ASC"),
            // SUBQUERY (order by / limit / offset)
            $("select * from (select * from t1) a order by \"user\" limit 1 offset 5",
                "SELECT \"user\", x FROM (SELECT doc.t1.\"user\", doc.t1.x FROM doc.t1) a ORDER BY \"user\" ASC LIMIT 1 OFFSET 5"),
            // SUBQUERY (nested)
            $("select * from (select * from (select * from t1) a) b ORDER BY \"user\" LIMIT 1 OFFSET 5",
                "SELECT \"user\", x FROM (SELECT \"user\", x FROM (SELECT doc.t1.\"user\", doc.t1.x FROM doc.t1) b) b ORDER BY \"user\" ASC LIMIT 1 OFFSET 5"),
            // SUBQUERY (group by / having)
            $("select x, cnt from (select x, count(*) as cnt from t1 group by x having count(*) > 1) a",
                "SELECT x, cnt FROM (SELECT doc.t1.x, count(*) AS cnt FROM doc.t1 GROUP BY doc.t1.x HAVING (count(*) > 1)) a")

        );
    }
}
