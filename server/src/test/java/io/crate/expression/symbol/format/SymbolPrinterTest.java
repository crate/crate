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

package io.crate.expression.symbol.format;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.expression.scalar.FormatFunction;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.is;

public class SymbolPrinterTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;

    private static final String TABLE_NAME = "formatter";

    @Before
    public void prepare() throws Exception {
        String createTableStmt =
            "create table doc.formatter (" +
            "  foo text," +
            "  bar bigint," +
            "  \"CraZy\" ip," +
            "  \"1a\" int," +
            "  \"select\" char," +
            "  idx int," +
            "  s_arr array(text)" +
            ")";
        RelationName name = new RelationName(DocSchemaInfo.NAME, TABLE_NAME);
        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            name,
            createTableStmt,
            clusterService);
        Map<RelationName, AnalyzedRelation> sources = Map.of(name, new TableRelation(tableInfo));
        sqlExpressions = new SqlExpressions(sources);
    }

    private void assertPrint(Symbol s, String formatted) {
        assertThat(s.toString(Style.QUALIFIED), is(formatted));
    }

    private void assertPrintIsParseable(String sql) {
        Symbol symbol = sqlExpressions.asSymbol(sql);
        String formatted = symbol.toString(Style.UNQUALIFIED);
        Symbol formattedSymbol = sqlExpressions.asSymbol(formatted);
        assertThat(symbol, is(formattedSymbol));
    }

    private void assertPrintingRoundTrip(String sql, String expected) {
        Symbol sym = sqlExpressions.asSymbol(sql);
        assertPrint(sym, expected);
        assertPrint(sym, expected);
        assertPrintIsParseable(sql);
    }

    @Test
    public void testFormatFunctionFullQualifiedInputName() throws Exception {
        Symbol f = sqlExpressions.asSymbol("concat('foo', foo)");
        assertPrint(f, "concat('foo', doc.formatter.foo)");
    }

    @Test
    public void testFormatFunctionsWithoutBrackets() throws Exception {
        Symbol f = sqlExpressions.asSymbol("current_schema");
        assertPrint(f, "current_schema");
    }

    @Test
    public void testSubstrFunction() throws Exception {
        Symbol f = sqlExpressions.asSymbol("substr('foobar', 4)");
        assertPrint(f, "'bar'");
    }

    @Test
    public void testSubstrFunctionWithLength() throws Exception {
        Symbol f = sqlExpressions.asSymbol("substr('foobar', 4, 1)");
        assertPrint(f, "'b'");
    }

    @Test
    public void testWindowFunction() throws Exception {
        Symbol f = sqlExpressions.asSymbol("avg(idx) over (partition by idx order by foo)");
        assertPrint(f, "avg(doc.formatter.idx) OVER (PARTITION BY doc.formatter.idx ORDER BY doc.formatter.foo ASC)");
    }

    @Test
    public void testFormatAggregation() throws Exception {
        FunctionInfo functionInfo = new FunctionInfo(
            new FunctionIdent("agg", Collections.singletonList(DataTypes.INTEGER)),
            DataTypes.LONG,
            FunctionInfo.Type.AGGREGATE
        );
        Aggregation a = new Aggregation(
            functionInfo, DataTypes.LONG, Collections.singletonList(Literal.of(-127)));

        assertPrint(a, "agg(-127)");
    }

    @Test
    public void testReference() throws Exception {
        Reference r = new Reference(
            new ReferenceIdent(
            new RelationName("sys", "table"),
            new ColumnIdent("column", Arrays.asList("path", "nested"))),
            RowGranularity.DOC,
            DataTypes.STRING,
            null,
            null
        );
        assertPrint(r, "sys.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testDocReference() throws Exception {
        Reference r = new Reference(
            new ReferenceIdent(
            new RelationName("doc", "table"),
            new ColumnIdent("column", Arrays.asList("path", "nested"))),
            RowGranularity.DOC,
            DataTypes.STRING,
            null,
            null
        );
        assertPrint(r, "doc.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testDynamicReference() throws Exception {
        Reference r = new DynamicReference(
            new ReferenceIdent(new RelationName("schema", "table"), new ColumnIdent("column", Arrays.asList("path", "nested"))),
            RowGranularity.DOC);
        assertPrint(r, "schema.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testReferenceEscaped() throws Exception {
        Reference r = new Reference(
            new ReferenceIdent(new RelationName("doc", "table"),
            new ColumnIdent("colum\"n")),
            RowGranularity.DOC,
            DataTypes.STRING,
            null,
            null
        );
        assertPrint(r, "doc.\"table\".\"colum\"\"n\"");
    }

    @Test
    public void testLiteralEscaped() throws Exception {
        assertPrint(Literal.of("bla'bla"), "'bla''bla'");

    }

    @Test
    public void testObjectLiteral() throws Exception {
        Literal<Map<String, Object>> l = Literal.of(Map.ofEntries(
            Map.entry("field", "value"),
            Map.entry("array", List.of(1, 2, 3)),
            Map.entry("nestedMap", Map.of("inner", -0.00005d))
        ));
        assertPrint(l, "{\"array\"=[1, 2, 3], \"field\"='value', \"nestedMap\"={\"inner\"=-5.0E-5}}");
    }

    @Test
    public void testBooleanLiteral() throws Exception {
        Literal<Boolean> f = Literal.of(false);
        assertPrint(f, "false");
        Literal<Boolean> t = Literal.of(true);
        assertPrint(t, "true");
    }

    @Test
    public void visitStringLiteral() throws Exception {
        Literal<String> l = Literal.of("fooBar");
        assertPrint(l, "'fooBar'");
    }

    @Test
    public void visitDoubleLiteral() throws Exception {
        Literal<Double> d = Literal.of(-500.88765d);
        assertPrint(d, "-500.88765");
    }

    @Test
    public void visitFloatLiteral() throws Exception {
        Literal<Float> f = Literal.of(500.887f);
        assertPrint(f, "500.887");
    }

    @Test
    public void testExtract() {
        assertPrintIsParseable("extract(century from '1970-01-01')::bigint");
        assertPrintIsParseable("extract(day_of_week from current_timestamp)::bigint");
    }

    @Test
    public void testIsNull() throws Exception {
        assertPrintIsParseable("null IS NULL");
        assertPrintIsParseable("formatter.foo IS NULL");
        assertPrintIsParseable("'123' IS NULL");
    }

    @Test
    public void testQueries() throws Exception {
        assertPrintIsParseable("(1 + formatter.bar) * 4 = 14");
    }

    @Test
    public void testCast() {
        assertPrintIsParseable("CAST (formatter.bar AS timestamp with time zone)");
        assertPrintIsParseable("CAST (formatter.bar AS timestamp without time zone)");
        assertPrintIsParseable("CAST (TRUE AS string)");
        assertPrintIsParseable("CAST (1+2 AS string)");
    }

    @Test
    public void testNull() throws Exception {
        assertPrint(Literal.of(DataTypes.UNDEFINED, null), "NULL");
    }

    @Test
    public void testNullKey() throws Exception {
        assertPrint(Literal.of(new HashMap<String, Object>() {{
            put("null", null);
        }}), "{\"null\"=NULL}");
    }

    @Test
    public void testNativeArray() throws Exception {
        assertPrint(
            Literal.of(
                DataTypes.GEO_SHAPE,
                DataTypes.GEO_SHAPE.value(Map.of("type", "Point", "coordinates", new double[]{1.0d, 2.0d}))
            ),
            "{\"coordinates\"=[1.0, 2.0], \"type\"='Point'}"
        );
    }

    @Test
    public void testFormatQualified() {
        Symbol ref = sqlExpressions.asSymbol("formatter.\"CraZy\"");
        assertThat(ref.toString(Style.QUALIFIED), is("doc.formatter.\"CraZy\""));
        assertThat(ref.toString(Style.UNQUALIFIED), is("\"CraZy\""));

        ref = sqlExpressions.asSymbol("formatter.\"1a\"");
        assertThat(ref.toString(Style.QUALIFIED), is("doc.formatter.\"1a\""));
        assertThat(ref.toString(Style.UNQUALIFIED), is("\"1a\""));
    }

    @Test
    public void testMaxDepthEllipsis() throws Exception {
        Symbol nestedFn = sqlExpressions.asSymbol("abs(sqrt(ln(1+1+1+1+1+1+1+1)))");
        assertThat(nestedFn.toString(), is("1.442026886600883"));
    }

    @Test
    public void testStyles() throws Exception {
        Symbol nestedFn = sqlExpressions.asSymbol("abs(sqrt(ln(bar+cast(\"select\" as long)+1+1+1+1+1+1)))");
        assertThat(nestedFn.toString(Style.QUALIFIED),
            is("abs(sqrt(ln(cast((((((((doc.formatter.bar + cast(doc.formatter.\"select\" AS bigint)) + 1) + 1) + 1) + 1) + 1) + 1) AS double precision))))"));
        assertThat(nestedFn.toString(Style.UNQUALIFIED),
            is("abs(sqrt(ln(cast((((((((bar + cast(\"select\" AS bigint)) + 1) + 1) + 1) + 1) + 1) + 1) AS double precision))))"));
    }

    @Test
    public void testFormatOperatorWithStaticInstance() throws Exception {
        Symbol comparisonOperator = sqlExpressions.asSymbol("bar = 1 and foo = 2");
        String printed = comparisonOperator.toString(Style.QUALIFIED);
        assertThat(
            printed,
            is("((doc.formatter.bar = 1) AND (doc.formatter.foo = '2'))")
        );
    }

    @Test
    public void testPrintFetchRefs() throws Exception {
        Symbol field = sqlExpressions.asSymbol("bar");
        assertThat(field, isReference("bar"));
        Reference ref = (Reference) field;
        FetchReference fetchRef = new FetchReference(new InputColumn(0, field.valueType()), ref);
        assertPrint(fetchRef, "FETCH(INPUT(0), doc.formatter.bar)");
    }

    @Test
    public void testPrintInputColumn() throws Exception {
        Symbol inputCol = new InputColumn(42);
        assertPrint(inputCol, "INPUT(42)");
    }

    @Test
    public void testPrintLikeOperator() throws Exception {
        Symbol likeQuery = sqlExpressions.asSymbol("foo like '%bla%'");
        assertPrint(likeQuery, "(doc.formatter.foo LIKE '%bla%')");
        assertPrint(likeQuery, "(doc.formatter.foo LIKE '%bla%')");
        assertPrintIsParseable("(foo LIKE 'a')");
    }

    @Test
    public void testPrintILikeOperator() throws Exception {
        Symbol likeQuery = sqlExpressions.asSymbol("foo ilike '%bla%'");
        assertPrint(likeQuery, "(doc.formatter.foo ILIKE '%bla%')");
        assertPrint(likeQuery, "(doc.formatter.foo ILIKE '%bla%')");
        assertPrintIsParseable("(foo ILIKE 'a')");
    }

    @Test
    public void testPrintAnyEqOperator() throws Exception {
        assertPrintingRoundTrip("foo = ANY (['a', 'b', 'c'])", "(doc.formatter.foo = ANY(['a', 'b', 'c']))");
        assertPrintingRoundTrip("foo = ANY(s_arr)", "(doc.formatter.foo = ANY(doc.formatter.s_arr))");
    }

    @Test
    public void testAnyNeqOperator() throws Exception {
        assertPrintingRoundTrip("not foo != ANY (['a', 'b', 'c'])", "(NOT (doc.formatter.foo <> ANY(['a', 'b', 'c'])))");
        assertPrintingRoundTrip("not foo != ANY(s_arr)", "(NOT (doc.formatter.foo <> ANY(doc.formatter.s_arr)))");
    }

    @Test
    public void testNot() throws Exception {
        assertPrintingRoundTrip("not foo = 'bar'", "(NOT (doc.formatter.foo = 'bar'))");
    }

    @Test
    public void testAnyLikeOperator() throws Exception {
        assertPrintingRoundTrip("foo LIKE ANY (s_arr)", "(doc.formatter.foo LIKE ANY(doc.formatter.s_arr))");
        assertPrintingRoundTrip("foo NOT LIKE ANY (['a', 'b', 'c'])", "(doc.formatter.foo NOT LIKE ANY(['a', 'b', 'c']))");
    }
}
