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

package io.crate.integrationtests;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Locale;

import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.testing.Asserts;

public class ArithmeticIntegrationTest extends IntegTestCase {

    @Test
    public void testMathFunctionNullArguments() throws Exception {
        testMathFunction("round(null)");
        testMathFunction("ceil(null)");
        testMathFunction("floor(null)");
        testMathFunction("abs(null)");
        testMathFunction("sqrt(null)");
        testMathFunction("log(null)");
        testMathFunction("log(null, 1)");
        testMathFunction("log(1, null)");
        testMathFunction("log(null, null)");
        testMathFunction("ln(null)");
    }

    public void testMathFunction(String function) {
        assertNull(execute("select " + function + " from sys.cluster").rows()[0][0]);
    }

    @Test
    public void testSelectWhereArithmeticScalar() throws Exception {
        execute("create table t (d double, i integer) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (d) values (?), (?), (?)", new Object[]{1.3d, 1.6d, 2.2d});
        execute("refresh table t");

        execute("select * from t where round(d) < 2");
        assertThat(response.rowCount(), is(1L));

        execute("select * from t where ceil(d) < 3");
        assertThat(response.rowCount(), is(2L));

        execute("select floor(d) from t where floor(d) = 2");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(2L));

        execute("insert into t (d, i) values (?, ?)", new Object[]{-0.2, 10});
        execute("refresh table t");

        execute("select abs(d) from t where abs(d) = 0.2");
        assertThat(response.rowCount(), is(1L));
        assertThat((Double) response.rows()[0][0], is(0.2));

        execute("select ln(i) from t where ln(i) = 2.302585092994046");
        assertThat(response.rowCount(), is(1L));
        assertThat((Double) response.rows()[0][0], is(2.302585092994046));

        execute("select log(i, 100) from t where log(i, 100) = 0.5");
        assertThat(response.rowCount(), is(1L));
        assertThat((Double) response.rows()[0][0], is(0.5));

        execute("select round(d), count(*) from t where abs(d) > 1 group by 1 order by 1");
        assertThat(response.rowCount(), is(2L));
        assertThat((Long) response.rows()[0][0], is(1L));
        assertThat((Long) response.rows()[0][1], is(1L));
        assertThat((Long) response.rows()[1][0], is(2L));
        assertThat((Long) response.rows()[1][1], is(2L));
    }

    @Test
    public void testSelectOrderByScalar() throws Exception {
        execute("create table t (d double, i integer, name string) clustered into 1 shards with (number_of_replicas=0)");
        execute("insert into t (d, name) values (?, ?)", new Object[][]{
            new Object[]{1.3d, "Arthur"},
            new Object[]{1.6d, null},
            new Object[]{2.2d, "Marvin"}
        });
        execute("refresh table t");

        execute("select * from t order by round(d) * 2 + 3");
        assertThat(response.rowCount(), is(3L));
        assertThat(response.rows()[0][0], is(1.3d));

        execute("select name from t order by substr(name, 1, 1) nulls first");
        assertThat(response.rowCount(), is(3L));
        assertThat(response.rows()[0][0], nullValue());
        assertThat(response.rows()[1][0], is("Arthur"));

        execute("select name from t order by substr(name, 1, 1) nulls last");
        assertThat(printedTable(response.rows()), is(
            "Arthur\n" +
            "Marvin\n" +
            "NULL\n"
        ));

        execute("select * from t order by ceil(d), d");
        assertThat(response.rowCount(), is(3L));
        assertThat(response.rows()[0][0], is(1.3d));

        execute("select * from t order by floor(d), d");
        assertThat(response.rowCount(), is(3L));
        assertThat(response.rows()[0][0], is(1.3d));

        execute("insert into t (d, i) values (?, ?), (?, ?)", new Object[]{-0.2, 10, 0.1, 5});
        execute("refresh table t");

        execute("select * from t order by abs(d)");
        assertThat(response.rowCount(), is(5L));
        assertThat(response.rows()[0][0], is(0.1));

        execute("select i from t order by ln(i)");
        assertThat(response.rowCount(), is(5L));
        assertThat(response.rows()[0][0], is(5));

        execute("select i from t order by log(i, 100)");
        assertThat(response.rowCount(), is(5L));
        assertThat(response.rows()[0][0], is(5));
    }

    @Test
    public void testSelectWhereArithmeticScalarTwoReferences() throws Exception {
        execute("create table t (d double, i integer) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (d, i) values (?, ?), (?, ?), (?, ?)", new Object[]{
            1.3d, 1,
            1.6d, 2,
            2.2d, 9});
        execute("refresh table t");

        execute("select i from t where round(d)::integer = i order by i");
        assertThat(response.rowCount(), is(2L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(2));
    }

    @Test
    public void testSelectWhereArithmeticScalarTwoReferenceArgs() throws Exception {
        execute("create table t (x long, base long) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (x, base) values (?, ?), (?, ?), (?, ?)", new Object[]{
            144L, 12L, // 2
            65536L, 2L, // 16
            9L, 3L // 2
        });
        execute("refresh table t");

        execute("select x, base, round(log(x, base)) from t where round(log(x, base)) = 2 order by x");
        assertThat(response.rowCount(), is(2L));
        assertThat((Long) response.rows()[0][0], is(9L));
        assertThat((Long) response.rows()[0][1], is(3L));
        assertThat((Long) response.rows()[0][2], is(2L));
        assertThat((Long) response.rows()[1][0], is(144L));
        assertThat((Long) response.rows()[1][1], is(12L));
        assertThat((Long) response.rows()[1][2], is(2L));
    }

    @Test
    public void testScalarInOrderByAndSelect() throws Exception {
        execute("create table t (i integer, l long, d double) clustered into 3 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (-1, 4, 90.5), (193384, 31234594433, 99.0)");
        execute("insert into t (i, l, d) values (1, 2, 99.0), (-1, 4, 99.0)");
        refresh();
        execute("select l, log(d,l) from t order by l, log(d,l) desc");
        assertThat(response.rowCount(), is(5L));
        assertThat(printedTable(response.rows()), is(
            "2| 6.6293566200796095\n" +
            "2| 6.499845887083206\n" +
            "4| 3.3146783100398047\n" +
            "4| 3.249922943541603\n" +
            "31234594433| 0.19015764044502392\n"));
    }

    @Test
    public void testNonIndexedColumnInRegexScalar() throws Exception {
        execute("create table regex_noindex (i integer, s string INDEX OFF) clustered into 3 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into regex_noindex (i, s) values (?, ?)", new Object[][]{
            new Object[]{1, "foo"},
            new Object[]{2, "bar"},
            new Object[]{3, "foobar"}
        });
        refresh();
        execute("select regexp_replace(s, 'foo', 'crate') from regex_noindex order by i");
        assertThat(response.rowCount(), is(3L));
        assertThat((String) response.rows()[0][0], is("crate"));
        assertThat((String) response.rows()[1][0], is("bar"));
        assertThat((String) response.rows()[2][0], is("cratebar"));

        execute("select regexp_matches(s, '^(bar).*') from regex_noindex order by i");
        assertThat((List<Object>) response.rows()[0][0], Matchers.contains("bar"));
    }

    @Test
    public void testFulltextColumnInRegexScalar() throws Exception {
        execute("create table regex_fulltext (i integer, s string INDEX USING FULLTEXT) clustered into 3 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into regex_fulltext (i, s) values (?, ?)", new Object[][]{
            new Object[]{1, "foo is first"},
            new Object[]{2, "bar is second"},
            new Object[]{3, "foobar is great"},
            new Object[]{4, "crate is greater"}
        });
        refresh();

        execute("select regexp_replace(s, 'is', 'was') from regex_fulltext order by i");
        assertThat(response.rowCount(), is(4L));
        assertThat(response.rows()[0][0], is("foo was first"));
        assertThat(response.rows()[1][0], is("bar was second"));
        assertThat(response.rows()[2][0], is("foobar was great"));
        assertThat(response.rows()[3][0], is("crate was greater"));

        execute("select regexp_matches(s, '(\\w+) is (\\w+)') from regex_fulltext order by i");
        List<Object> match1 = (List<Object>) response.rows()[0][0];
        assertThat(match1, Matchers.contains("foo", "first"));

        List<Object> match2 = (List<Object>) response.rows()[1][0];
        assertThat(match2, Matchers.contains("bar", "second"));

        List<Object> match3 = (List<Object>) response.rows()[2][0];
        assertThat(match3, Matchers.contains("foobar", "great"));

        List<Object> match4 = (List<Object>) response.rows()[3][0];
        assertThat(match4, Matchers.contains("crate", "greater"));
    }

    @Test
    public void testSelectRandomTwoTimes() throws Exception {
        execute("select random(), random() from sys.cluster limit 1");
        assertThat(response.rows()[0][0], is(not(response.rows()[0][1])));

        execute("create table t (name string) ");
        ensureYellow();
        execute("insert into t (name) values ('Marvin')");
        execute("refresh table t");

        execute("select random(), random() from t");
        assertThat(response.rows()[0][0], is(not(response.rows()[0][1])));
    }

    @Test
    public void testSelectArithmeticOperatorInWhereClause() throws Exception {
        execute("create table t (i integer, l long, d double) clustered into 3 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (2, 5, 90.5), (193384, 31234594433, 99.0), (10, 21, 99.0), (-1, 4, 99.0)");
        refresh();

        execute("select i from t where i%2 = 0 order by i");
        assertThat(response.rowCount(), is(3L));

        assertThat(printedTable(response.rows()), is("2\n10\n193384\n"));

        execute("select l from t where i * -1 > 0");
        assertThat(response.rowCount(), is(1L));
        assertThat(printedTable(response.rows()), is("4\n"));

        execute("select l from t where cast(i * 2  as long) = l");
        assertThat(response.rowCount(), is(1L));
        assertThat(printedTable(response.rows()), is("2\n"));

        execute("select i%3, sum(l) from t where i+1 > 2 group by i%3 order by sum(l)");
        assertThat(response.rowCount(), is(2L));
        assertThat(printedTable(response.rows()), is(
            "2| 5\n" +
            "1| 31234594454\n"));
    }

    @Test
    public void testSelectArithMetricOperatorInOrderBy() throws Exception {
        execute("create table t (i integer, l long, d double) clustered into 3 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (2, 5, 90.5), (193384, 31234594433, 99.0), (10, 21, 99.0), (-1, 4, 99.0)");
        refresh();

        execute("select i, i%3 from t order by i%3, l");
        assertThat(response.rowCount(), is(5L));
        assertThat(printedTable(response.rows()), is(
            "-1| -1\n" +
            "1| 1\n" +
            "10| 1\n" +
            "193384| 1\n" +
            "2| 2\n"));
    }

    @Test
    public void testSelectFailingArithmeticScalar() throws Exception {
        execute("create table t (i integer, l long, d double) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, l, d) values (1, 2, 90.5)");
        refresh();
        Asserts.assertSQLError(() -> execute("select log(d, l) from t where log(d, -1) >= 0"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("log(x, b): given arguments would result in: 'NaN'");
    }

    @Test
    public void testSelectGroupByFailingArithmeticScalar() throws Exception {
        execute("create table t (i integer, l long, d double) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (0, 4, 100)");
        execute("refresh table t");

        Asserts.assertSQLError(() -> execute("select log(d, l) from t where log(d, -1) >= 0 group by log(d, l)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("log(x, b): given arguments would result in: 'NaN'");
    }

    @Test
    public void testArithmeticScalarFunctionsOnAllTypes() {
        // this test validates that no exception is thrown
        execute("create table t (" +
                "   b byte, " +
                "   s short, " +
                "   i integer, " +
                "   l long, " +
                "   f float, " +
                "   d double, " +
                "   tz timestamp with time zone, " +
                "   t timestamp without time zone " +
                ") with (number_of_replicas=0)");
        execute("insert into t (b, s, i, l, f, d, tz, t) values (1, 2, 3, 4, 5.7, 6.3, '2014-07-30', '2018-07-30')");
        refresh();

        String[] functionCalls = new String[]{
            "abs(%s)",
            "ceil(%s)",
            "floor(%s)",
            "ln(%s)",
            "log(%s)",
            "log(%s, 2)",
            "random()",
            "round(%s)",
            "sqrt(%s)"
        };
        execute("select b + b, s + s, i + i, l + l, f + f, d + d, tz + tz, t + t from t");

        for (String functionCall : functionCalls) {
            String byteCall = String.format(Locale.ENGLISH, functionCall, "b");
            execute(String.format(Locale.ENGLISH, "select %s, b from t where %s < 2", byteCall, byteCall));

            String shortCall = String.format(Locale.ENGLISH, functionCall, "s");
            execute(String.format(Locale.ENGLISH, "select %s, s from t where %s < 2", shortCall, shortCall));

            String intCall = String.format(Locale.ENGLISH, functionCall, "i");
            execute(String.format(Locale.ENGLISH, "select %s, i from t where %s < 2", intCall, intCall));

            String longCall = String.format(Locale.ENGLISH, functionCall, "l");
            execute(String.format(Locale.ENGLISH, "select %s, l from t where %s < 2", longCall, longCall));

            String floatCall = String.format(Locale.ENGLISH, functionCall, "f");
            execute(String.format(Locale.ENGLISH, "select %s, f from t where %s < 2", floatCall, floatCall));

            String doubleCall = String.format(Locale.ENGLISH, functionCall, "d");
            execute(String.format(Locale.ENGLISH, "select %s, d from t where %s < 2", doubleCall, doubleCall));
        }
    }

    @Test
    public void testSelectOrderByScalarOnNullValue() throws Exception {
        execute("create table t (d long) clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (d) values (?)", new Object[][]{
            new Object[]{-7L},
            new Object[]{null},
            new Object[]{5L},
            new Object[]{null}

        });
        execute("refresh table t");
        execute("select (d - 10) from t order by (d - 10) nulls first limit 2");
        assertThat(response.rows()[0][0], is(nullValue()));
    }

    @Test
    public void test_floating_point_arithmetic() {
        execute("create table t1 (i int, bi bigint, d double, f float)");
        execute("insert into t1 (i, bi, d, f) values (1, 1, 1, 1)");
        refresh();
        execute("select " +
                "    i / 3.0," +
                "    bi / 3.0::float," +
                "    d * 5::int," +
                "    f * 5," +
                "    1::int / 3.0," +
                "    1 / 3.0 " +
                "from t1");
        assertThat(printedTable(response.rows()), is(
            "0.3333333333333333| 0.33333334| 5.0| 5.0| 0.3333333333333333| 0.3333333333333333\n"));
    }
}
