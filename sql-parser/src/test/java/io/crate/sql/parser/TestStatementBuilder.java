/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.sql.parser;

import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.*;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.antlr.runtime.tree.CommonTree;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.crate.sql.parser.TreeAssertions.assertFormattedSql;
import static io.crate.sql.parser.TreePrinter.treeToString;
import static com.google.common.base.Strings.repeat;
import static java.lang.String.format;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestStatementBuilder
{
    @Test
    public void testStatementBuilder()
            throws Exception
    {
        printStatement("select * from foo");
        printStatement("explain select * from foo");

        printStatement("select * from foo a (x, y, z)");

        printStatement("select *, 123, * from foo");

        printStatement("select show from foo");
        printStatement("select extract(day from x), extract(dow from x) from y");

        printStatement("select 1 + 13 || '15' from foo");

        printStatement("select x is distinct from y from foo where a is not distinct from b");

        printStatement("" +
                "select depname, empno, salary\n" +
                ", count(*) over ()\n" +
                ", avg(salary) over (partition by depname)\n" +
                ", rank() over (partition by depname order by salary desc)\n" +
                ", sum(salary) over (order by salary rows unbounded preceding)\n" +
                ", sum(salary) over (partition by depname order by salary rows between current row and 3 following)\n" +
                ", sum(salary) over (partition by depname range unbounded preceding)\n" +
                ", sum(salary) over (rows between 2 preceding and unbounded following)\n" +
                "from emp");

        printStatement("" +
                "with a (id) as (with x as (select 123 from z) select * from x) " +
                "   , b (id) as (select 999 from z) " +
                "select * from a join b using (id)");

        printStatement("with recursive t as (select * from x) select * from t");

        printStatement("select * from information_schema.tables");

        printStatement("show tables");
        printStatement("show tables from information_schema");
        printStatement("show tables like '%'");
        printStatement("show tables from information_schema like '%'");

        printStatement("show partitions from foo");
        printStatement("show partitions from foo where name = 'foo'");
        printStatement("show partitions from foo order by x");
        printStatement("show partitions from foo limit 10");
        printStatement("show partitions from foo limit 10 offset 20");
        printStatement("show partitions from foo offset 20");
        printStatement("show partitions from foo order by x desc limit 10");
        printStatement("show partitions from foo order by x desc limit 10 offset 20");
        printStatement("show partitions from foo order by x desc offset 20");

        printStatement("select * from a.b.c@d");

        printStatement("select \"TOTALPRICE\" \"my price\" from \"ORDERS\"");

        printStatement("select * from foo tablesample system (10+1)");
        printStatement("select * from foo tablesample system (10) join bar tablesample bernoulli (30) on a.id = b.id");
        printStatement("select * from foo tablesample bernoulli (10) stratify on (id)");
        printStatement("select * from foo tablesample system (50) stratify on (id, name)");

        printStatement("select * from foo limit 100 offset 20");
        printStatement("select * from foo offset 20");

        printStatement("create table foo as select * from abc");

        printStatement("insert into foo (id, name) values ('string', 1.2)");
        printStatement("insert into foo values ('string', NULL)");
        printStatement("insert into foo (id, name) values ('string', 1.2), (abs(-4), 4+?)");
        printStatement("insert into schemah.foo (foo.id, bar.name) values ('string', 1.2)");

        printStatement("delete from foo");
        printStatement("delete from schemah.foo where foo.a=foo.b and a is not null");

        printStatement("update foo set a=b");
        printStatement("update schemah.foo set foo.a='b', foo.b=foo.a");
        printStatement("update schemah.foo set foo.a=abs(-6.3334), x=true where x=false");

    }

    @Test
    public void testStatementBuilderTpch()
            throws Exception
    {
        printTpchQuery(1, 3);
        printTpchQuery(2, 33, "part type like", "region name");
        printTpchQuery(3, "market segment", "2013-03-05");
        printTpchQuery(4, "2013-03-05");
        printTpchQuery(5, "region name", "2013-03-05");
        printTpchQuery(6, "2013-03-05", 33, 44);
        printTpchQuery(7, "nation name 1", "nation name 2");
        printTpchQuery(8, "nation name", "region name", "part type");
        printTpchQuery(9, "part name like");
        printTpchQuery(10, "2013-03-05");
        printTpchQuery(11, "nation name", 33);
        printTpchQuery(12, "ship mode 1", "ship mode 2", "2013-03-05");
        printTpchQuery(13, "comment like 1", "comment like 2");
        printTpchQuery(14, "2013-03-05");
        // query 15: views not supported
        printTpchQuery(16, "part brand", "part type like", 3, 4, 5, 6, 7, 8, 9, 10);
        printTpchQuery(17, "part brand", "part container");
        printTpchQuery(18, 33);
        printTpchQuery(19, "part brand 1", "part brand 2", "part brand 3", 11, 22, 33);
        printTpchQuery(20, "part name like", "2013-03-05", "nation name");
        printTpchQuery(21, "nation name");
        printTpchQuery(22,
                "phone 1",
                "phone 2",
                "phone 3",
                "phone 4",
                "phone 5",
                "phone 6",
                "phone 7");
    }

    @Test
    public void testStatementSubscript() throws Exception {
        printStatement("select a['x'] from foo where a['x']['y']['z'] = 1");
        printStatement("select a['x'] from foo where a[1 + 2]['y'] = 1");
    }

    @Test
    public void testCaseSensitivity() throws Exception {
        Expression expression = SqlParser.createExpression("\"firstName\" = 'myName'");
        QualifiedNameReference nameRef = (QualifiedNameReference)((ComparisonExpression)expression).getLeft();
        StringLiteral myName = (StringLiteral)((ComparisonExpression)expression).getRight();
        assertThat(nameRef.getName().getSuffix(), is("firstName"));
        assertThat(myName.getValue(), is("myName"));

        expression = SqlParser.createExpression("FIRSTNAME = 'myName'");
        nameRef = (QualifiedNameReference)((ComparisonExpression)expression).getLeft();
        assertThat(nameRef.getName().getSuffix(), is("firstname"));

        expression = SqlParser.createExpression("ABS(1)");
        QualifiedName functionName = ((FunctionCall)expression).getName();
        assertThat(functionName.getSuffix(), is("abs"));
    }

    @Test
    public void testParameterNode() throws Exception {
        printStatement("select foo, $0 from foo where a = $1 or a = $2");

        final AtomicInteger counter = new AtomicInteger(0);

        Expression inExpression = SqlParser.createExpression("x in (?, ?, ?)");
        inExpression.accept(new DefaultTraversalVisitor<Object, Object>() {
            @Override
            public Object visitParameterExpression(ParameterExpression node, Object context) {
                assertEquals(counter.getAndIncrement(), node.position());
                return super.visitParameterExpression(node, context);
            }
        }, null);

        assertEquals(3, counter.get());
        counter.set(0);

        Expression andExpression = SqlParser.createExpression("a = ? and b = ? and c = $2");
        andExpression.accept(new DefaultTraversalVisitor<Object, Object>() {
            @Override
            public Object visitParameterExpression(ParameterExpression node, Object context) {
                assertEquals(counter.getAndIncrement(), node.position());
                return super.visitParameterExpression(node, context);
            }
        }, null);
        assertEquals(3, counter.get());
    }

    private static void printStatement(String sql)
    {
        println(sql.trim());
        println("");

        CommonTree tree = SqlParser.parseStatement(sql);
        println(treeToString(tree));
        println("");

        Statement statement = SqlParser.createStatement(tree);
        println(statement.toString());
        println("");

        // TODO: support formatting all statement types
        if (statement instanceof Query) {
            println(SqlFormatter.formatSql(statement));
            println("");
            assertFormattedSql(statement);
        }

        println(repeat("=", 60));
        println("");
    }

    private static void println(String s)
    {
        if (Boolean.parseBoolean(System.getProperty("printParse"))) {
            System.out.println(s);
        }
    }

    private static String getTpchQuery(int q)
            throws IOException
    {
        return readResource("tpch/queries/" + q + ".sql");
    }

    private static void printTpchQuery(int query, Object... values)
            throws IOException
    {
        String sql = getTpchQuery(query);

        for (int i = values.length - 1; i >= 0; i--) {
            sql = sql.replaceAll(format(":%s", i + 1), String.valueOf(values[i]));
        }

        assertFalse(sql.matches("(?s).*:[0-9].*"), "Not all bind parameters were replaced: " + sql);

        sql = fixTpchQuery(sql);
        printStatement(sql);
    }

    private static String readResource(String name)
            throws IOException
    {
        return Resources.toString(Resources.getResource(name), Charsets.UTF_8);
    }

    private static String fixTpchQuery(String s)
    {
        s = s.replaceFirst("(?m);$", "");
        s = s.replaceAll("(?m)^:[xo]$", "");
        s = s.replaceAll("(?m)^:n -1$", "");
        s = s.replaceAll("(?m)^:n ([0-9]+)$", "LIMIT $1");
        s = s.replace("day (3)", "day"); // for query 1
        return s;
    }
}
