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

package io.crate.expression.symbol.format;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.VoidReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;

public class SymbolPrinterTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;

    private static final String TABLE_NAME = "formatter";

    @Before
    public void prepare() {
        String createTableStmt =
            "create table doc.formatter (" +
            "  foo text," +
            "  bar bigint," +
            "  \"CraZy\" ip," +
            "  \"1a\" int," +
            "  \"select\" byte," +
            "  idx int," +
            "  s_arr array(text)," +
            "  a array(object as (b object as (c int)))," +
            "  \"OBJ\" object as (intarray int[])" +
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
        assertThat(s.toString(Style.QUALIFIED)).isEqualTo(formatted);
    }

    private void assertPrintIsParseable(String sql) {
        Symbol symbol = sqlExpressions.asSymbol(sql);
        String formatted = symbol.toString(Style.UNQUALIFIED);
        Symbol formattedSymbol = sqlExpressions.asSymbol(formatted);
        assertThat(symbol).isEqualTo(formattedSymbol);
    }

    private void assertPrintingRoundTrip(String sql, String expected) {
        Symbol sym = sqlExpressions.asSymbol(sql);
        assertPrint(sym, expected);
        assertPrint(sym, expected);
        assertPrintIsParseable(sql);
    }

    @Test
    public void testFormatFunctionFullQualifiedInputName() {
        Symbol f = sqlExpressions.asSymbol("concat('foo', foo)");
        assertPrint(f, "concat('foo', doc.formatter.foo)");
    }

    @Test
    public void testFormatFunctionsWithoutBrackets() {
        Symbol f = sqlExpressions.asSymbol("current_schema");
        assertPrint(f, "current_schema");
    }

    @Test
    public void testSubstrFunction() {
        Symbol f = sqlExpressions.asSymbol("substr('foobar', 4)");
        assertPrint(f, "'bar'");
    }

    @Test
    public void testSubstrFunctionWithLength() {
        Symbol f = sqlExpressions.asSymbol("substr('foobar', 4, 1)");
        assertPrint(f, "'b'");
    }

    @Test
    public void testWindowFunction() {
        Symbol f = sqlExpressions.asSymbol("avg(idx) over (partition by idx order by foo)");
        assertPrint(f, "avg(doc.formatter.idx) OVER (PARTITION BY doc.formatter.idx ORDER BY doc.formatter.foo ASC)");
    }

    @Test
    public void testFormatAggregation() {
        Signature signature = Signature.aggregate(
            "agg",
            DataTypes.INTEGER.getTypeSignature(),
            DataTypes.LONG.getTypeSignature()
        );
        Aggregation a = new Aggregation(
            signature,
            DataTypes.LONG,
            Collections.singletonList(Literal.of(-127))
        );

        assertPrint(a, "agg(-127)");
    }

    @Test
    public void testReference() {
        SimpleReference r = new SimpleReference(
            new ReferenceIdent(
                new RelationName("sys", "table"),
                new ColumnIdent("column", Arrays.asList("path", "nested"))),
            RowGranularity.DOC,
            DataTypes.STRING,
            1,
            null
        );
        assertPrint(r, "sys.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testDocReference() {
        SimpleReference r = new SimpleReference(
            new ReferenceIdent(
                new RelationName("doc", "table"),
                new ColumnIdent("column", Arrays.asList("path", "nested"))),
            RowGranularity.DOC,
            DataTypes.STRING,
            0,
            null
        );
        assertPrint(r, "doc.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testDynamicReference() {
        Reference r = new DynamicReference(
            new ReferenceIdent(new RelationName("schema", "table"),
                               new ColumnIdent("column", Arrays.asList("path", "nested"))),
            RowGranularity.DOC,
            0);
        assertPrint(r, "schema.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testVoidReference() {
        Reference r = new VoidReference(
            new ReferenceIdent(new RelationName("schema", "table"),
                               new ColumnIdent("column", Arrays.asList("path", "nested"))),
            RowGranularity.DOC,
            0);
        assertPrint(r, "schema.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testReferenceEscaped() {
        SimpleReference r = new SimpleReference(
            new ReferenceIdent(new RelationName("doc", "table"),
                               new ColumnIdent("colum\"n")),
            RowGranularity.DOC,
            DataTypes.STRING,
            0,
            null
        );
        assertPrint(r, "doc.\"table\".\"colum\"\"n\"");
    }

    @Test
    public void testLiteralEscaped() {
        assertPrint(Literal.of("bla'bla"), "'bla''bla'");

    }

    @Test
    public void testObjectLiteral() {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put("field", "value");
        map.put("array", List.of(1, 2, 3));
        map.put("nestedMap", Map.of("inner", -0.00005d));
        Literal<Map<String, Object>> l = Literal.of(map);
        assertPrint(l, "{\"field\"='value', \"array\"=[1, 2, 3], \"nestedMap\"={\"inner\"=-5.0E-5}}");
    }

    @Test
    public void testBooleanLiteral() {
        Literal<Boolean> f = Literal.of(false);
        assertPrint(f, "false");
        Literal<Boolean> t = Literal.of(true);
        assertPrint(t, "true");
    }

    @Test
    public void visitStringLiteral() {
        Literal<String> l = Literal.of("fooBar");
        assertPrint(l, "'fooBar'");
    }

    @Test
    public void visitDoubleLiteral() {
        Literal<Double> d = Literal.of(-500.88765d);
        assertPrint(d, "-500.88765");
    }

    @Test
    public void visitFloatLiteral() {
        Literal<Float> f = Literal.of(500.887f);
        assertPrint(f, "500.887");
    }

    @Test
    public void testExtract() {
        assertPrintIsParseable("extract(century from '1970-01-01')::bigint");
        assertPrintIsParseable("extract(day_of_week from current_timestamp)::bigint");
    }

    @Test
    public void testIsNull() {
        assertPrintIsParseable("null IS NULL");
        assertPrintIsParseable("formatter.foo IS NULL");
        assertPrintIsParseable("'123' IS NULL");
    }

    @Test
    public void testQueries() {
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
    public void testNull() {
        assertPrint(Literal.of(DataTypes.UNDEFINED, null), "NULL");
    }

    @Test
    public void testNullKey() {
        assertPrint(Literal.of(new HashMap<>() {{
                put("null", null);
            }}
        ), "{\"null\"=NULL}");
    }

    @Test
    public void testNativeArray() {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put("type", "Point");
        map.put("coordinates", new double[] {1.0d, 2.0d});
        assertPrint(
            Literal.of(
                DataTypes.GEO_SHAPE,
                DataTypes.GEO_SHAPE.sanitizeValue(map)
            ),
            "{\"type\"='Point', \"coordinates\"=[1.0, 2.0]}"
        );
    }

    @Test
    public void testFormatQualified() {
        Symbol ref = sqlExpressions.asSymbol("formatter.\"CraZy\"");
        assertThat(ref.toString(Style.QUALIFIED)).isEqualTo("doc.formatter.\"CraZy\"");
        assertThat(ref.toString(Style.UNQUALIFIED)).isEqualTo("\"CraZy\"");

        ref = sqlExpressions.asSymbol("formatter.\"1a\"");
        assertThat(ref.toString(Style.QUALIFIED)).isEqualTo("doc.formatter.\"1a\"");
        assertThat(ref.toString(Style.UNQUALIFIED)).isEqualTo("\"1a\"");
    }

    @Test
    public void testMaxDepthEllipsis() {
        Symbol nestedFn = sqlExpressions.asSymbol("abs(sqrt(ln(1+1+1+1+1+1+1+1)))");
        assertThat(nestedFn).hasToString("1.442026886600883");
    }

    @Test
    public void testStyles() {
        Symbol nestedFn = sqlExpressions.asSymbol("abs(sqrt(ln(bar+cast(\"select\" as long)+1+1+1+1+1+1)))");
        assertThat(nestedFn.toString(Style.QUALIFIED)).isEqualTo(
                   "abs(sqrt(ln((((((((doc.formatter.bar + cast(doc.formatter.\"select\" AS bigint)) + 1::bigint) + 1::bigint) + 1::bigint) + 1::bigint) + 1::bigint) + 1::bigint))))");
        assertThat(nestedFn.toString(Style.UNQUALIFIED)).isEqualTo(
                   "abs(sqrt(ln((((((((bar + cast(\"select\" AS bigint)) + 1::bigint) + 1::bigint) + 1::bigint) + 1::bigint) + 1::bigint) + 1::bigint))))");
    }

    @Test
    public void testFormatOperatorWithStaticInstance() {
        Symbol comparisonOperator = sqlExpressions.asSymbol("bar = 1 and foo = '2'");
        String printed = comparisonOperator.toString(Style.QUALIFIED);
        assertThat(printed).isEqualTo(
            "((doc.formatter.bar = 1::bigint) AND (doc.formatter.foo = '2'))");
    }

    @Test
    public void testPrintFetchRefs() {
        Symbol field = sqlExpressions.asSymbol("bar");
        assertThat(field).isReference().hasName("bar");
        Reference ref = (Reference) field;
        FetchReference fetchRef = new FetchReference(new InputColumn(0, field.valueType()), ref);
        assertPrint(fetchRef, "FETCH(INPUT(0), doc.formatter.bar)");
    }

    @Test
    public void testPrintInputColumn() {
        Symbol inputCol = new InputColumn(42);
        assertPrint(inputCol, "INPUT(42)");
    }

    @Test
    public void testPrintLikeOperator() {
        Symbol likeQuery = sqlExpressions.asSymbol("foo like '%bla%'");
        assertPrint(likeQuery, "(doc.formatter.foo LIKE '%bla%')");
        assertPrint(likeQuery, "(doc.formatter.foo LIKE '%bla%')");
        assertPrintIsParseable("(foo LIKE 'a')");
    }

    @Test
    public void testPrintILikeOperator() {
        Symbol likeQuery = sqlExpressions.asSymbol("foo ilike '%bla%'");
        assertPrint(likeQuery, "(doc.formatter.foo ILIKE '%bla%')");
        assertPrint(likeQuery, "(doc.formatter.foo ILIKE '%bla%')");
        assertPrintIsParseable("(foo ILIKE 'a')");
    }

    @Test
    public void testPrintAnyEqOperator() {
        assertPrintingRoundTrip("foo = ANY (['a', 'b', 'c'])", "(doc.formatter.foo = ANY(['a', 'b', 'c']))");
        assertPrintingRoundTrip("foo = ANY(s_arr)", "(doc.formatter.foo = ANY(doc.formatter.s_arr))");
    }

    @Test
    public void testAnyNeqOperator() {
        assertPrintingRoundTrip("not foo != ANY (['a', 'b', 'c'])",
                                "(NOT (doc.formatter.foo <> ANY(['a', 'b', 'c'])))");
        assertPrintingRoundTrip("not foo != ANY(s_arr)", "(NOT (doc.formatter.foo <> ANY(doc.formatter.s_arr)))");
    }

    @Test
    public void testNot() {
        assertPrintingRoundTrip("not foo = 'bar'", "(NOT (doc.formatter.foo = 'bar'))");
    }

    @Test
    public void test_negate_is_pretty_printed() {
        assertPrintingRoundTrip("- bar", "- doc.formatter.bar");
    }

    @Test
    public void testAnyLikeOperator() {
        assertPrintingRoundTrip("foo LIKE ANY (s_arr)", "(doc.formatter.foo LIKE ANY(doc.formatter.s_arr))");
        assertPrintingRoundTrip("foo NOT LIKE ANY (['a', 'b', 'c'])",
                                "(doc.formatter.foo NOT LIKE ANY(['a', 'b', 'c']))");
    }

    // tracks a bug: https://github.com/crate/crate/issues/13504
    @Test
    public void test_printing_nested_objects() {
        Symbol symbol = sqlExpressions.asSymbol("a[1]['b']['c']");
        assertThat(symbol).isFunction("subscript");
        assertPrint(symbol, "a[1]['b']['c']");
    }

    /**
     * Tracks a bug:
     *      cr> CREATE TABLE t ("OBJ" OBJECT AS (intarray int[]), firstElement AS "OBJ"['intarray'][1]);
     *      ColumnUnknownException[Column obj['intarray'] unknown]
     */
    @Test
    public void test_subscript_function_quotes_root_column_if_required() {
        Symbol symbol = sqlExpressions.asSymbol("\"OBJ\"[1]['intarray']");
        assertThat(symbol).isFunction("subscript");
        assertPrint(symbol, "\"OBJ\"[1]['intarray']");
    }
}
