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

package io.crate.analyze.symbol.format;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.symbol.*;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.InOperator;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class SymbolPrinterTest extends CrateUnitTest {

    private SqlExpressions sqlExpressions;
    private SymbolPrinter printer;

    private static final String TABLE_NAME = "formatter";

    @Before
    public void prepare() throws Exception {
        DocTableInfo tableInfo = TestingTableInfo.builder(new TableIdent(DocSchemaInfo.NAME, TABLE_NAME), null)
            .add("foo", DataTypes.STRING)
            .add("bar", DataTypes.LONG)
            .add("CraZy", DataTypes.IP)
            .add("select", DataTypes.BYTE)
            .add("idx", DataTypes.INTEGER)
            .add("s_arr", new ArrayType(DataTypes.STRING))
            .build();
        Map<QualifiedName, AnalyzedRelation> sources = ImmutableMap.<QualifiedName, AnalyzedRelation>builder()
            .put(QualifiedName.of(TABLE_NAME), new TableRelation(tableInfo))
            .build();
        sqlExpressions = new SqlExpressions(sources);
        printer = new SymbolPrinter(sqlExpressions.functions());
    }

    private void assertPrint(Symbol s, String formatted) {
        assertThat(printer.printFullQualified(s), is(formatted));
    }

    private void assertPrintStatic(Symbol s, String formatted) {
        assertThat(SymbolPrinter.INSTANCE.printFullQualified(s), is(formatted));
    }

    private void assertPrintIsParseable(String sql) {
        assertPrintIsParseable(sql, SymbolPrinter.Style.SIMPLE);
    }

    private void assertPrintIsParseable(String sql, SymbolPrinter.Style style) {
        Symbol symbol = sqlExpressions.asSymbol(sql);
        String formatted = printer.print(symbol, style);
        Symbol formattedSymbol = sqlExpressions.asSymbol(formatted);
        assertThat(symbol, is(formattedSymbol));
    }

    private void assertPrintingRoundTrip(String sql, String expected) {
        Symbol sym = sqlExpressions.asSymbol(sql);
        assertPrint(sym, expected);
        assertPrintStatic(sym, expected);
        assertPrintIsParseable(sql);
    }


    @Test
    public void testFormatFunction() throws Exception {
        Function f = new Function(new FunctionInfo(
            new FunctionIdent("foo", Arrays.<DataType>asList(DataTypes.STRING, DataTypes.DOUBLE)), DataTypes.DOUBLE),
            Arrays.<Symbol>asList(Literal.of("bar"), Literal.of(3.4)));
        assertPrint(f, "foo('bar', 3.4)");
    }

    @Test
    public void testFormatFunctionWithoutArgs() throws Exception {
        Function f = new Function(new FunctionInfo(
            new FunctionIdent("baz", ImmutableList.<DataType>of()), DataTypes.DOUBLE),
            ImmutableList.<Symbol>of());
        assertPrint(f, "baz()");
    }

    @Test
    public void testSubstrFunction() throws Exception {
        Function f = new Function(new FunctionInfo(
            new FunctionIdent("substr", Arrays.<DataType>asList(DataTypes.STRING, DataTypes.LONG)), DataTypes.STRING),
            Arrays.<Symbol>asList(Literal.of("foobar"), Literal.of(4)));
        assertPrint(f, "substr('foobar', 4)");
    }

    @Test
    public void testSubstrFunctionWithLength() throws Exception {
        Function f = new Function(new FunctionInfo(
            new FunctionIdent("substr", Arrays.<DataType>asList(DataTypes.STRING, DataTypes.LONG, DataTypes.LONG)), DataTypes.STRING),
            Arrays.<Symbol>asList(Literal.of("foobar"), Literal.of(4), Literal.of(1)));
        assertPrint(f, "substr('foobar', 4, 1)");
    }

    @Test
    public void testFormatAggregation() throws Exception {
        Aggregation a = Aggregation.partialAggregation(new FunctionInfo(
            new FunctionIdent("agg", Collections.<DataType>singletonList(DataTypes.INTEGER)), DataTypes.LONG, FunctionInfo.Type.AGGREGATE
        ), DataTypes.LONG, Collections.<Symbol>singletonList(Literal.of(-127)));

        assertPrint(a, "agg(-127)");
    }

    @Test
    public void testReference() throws Exception {
        Reference r = new Reference(new ReferenceIdent(
            new TableIdent("sys", "table"),
            new ColumnIdent("column", Arrays.asList("path", "nested"))),
            RowGranularity.DOC, DataTypes.STRING);
        assertPrint(r, "sys.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testDocReference() throws Exception {
        Reference r = new Reference(new ReferenceIdent(
            new TableIdent("doc", "table"),
            new ColumnIdent("column", Arrays.asList("path", "nested"))),
            RowGranularity.DOC, DataTypes.STRING);
        assertPrint(r, "doc.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testDynamicReference() throws Exception {
        Reference r = new DynamicReference(
            new ReferenceIdent(new TableIdent("schema", "table"), new ColumnIdent("column", Arrays.asList("path", "nested"))),
            RowGranularity.DOC);
        assertPrint(r, "schema.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testReferenceEscaped() throws Exception {
        Reference r = new Reference(new ReferenceIdent(
            new TableIdent("doc", "table"),
            new ColumnIdent("colum\"n")),
            RowGranularity.DOC, DataTypes.STRING);
        assertPrint(r, "doc.\"table\".\"colum\"\"n\"");
    }

    @Test
    public void testLiteralEscaped() throws Exception {
        assertPrint(Literal.of("bla'bla"), "'bla''bla'");

    }

    @Test
    public void testObjectLiteral() throws Exception {
        Literal<Map<String, Object>> l = Literal.of(new HashMap<String, Object>() {{
            put("field", "value");
            put("array", new Integer[]{1, 2, 3});
            put("nestedMap", new HashMap<String, Object>() {{
                put("inner", -0.00005d);
            }});
        }});
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
        Literal<BytesRef> l = Literal.of("fooBar");
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
    public void testExtract() throws Exception {
        assertPrintIsParseable("extract(century from '1970-01-01')");
        assertPrintIsParseable("extract(day_of_week from 0)");
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
    public void testCast() throws Exception {
        assertPrintIsParseable("CAST (formatter.bar AS TIMESTAMP)");
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
            Literal.of(DataTypes.GEO_SHAPE, ImmutableMap.of("type", "Point", "coordinates", new double[]{1.0d, 2.0d})),
            "{\"coordinates\"=[1.0, 2.0], \"type\"='Point'}");
    }

    @Test
    public void testFormatQualified() throws Exception {
        Symbol ref = sqlExpressions.asSymbol("doc.formatter.\"CraZy\"");
        assertThat(printer.print(ref, 10, true, false), is("doc.formatter.\"CraZy\""));
        assertThat(printer.print(ref, 10, false, false), is("\"CraZy\""));
    }

    @Test
    public void testMaxDepthEllipsis() throws Exception {
        Symbol nestedFn = sqlExpressions.asSymbol("abs(sqrt(ln(1+1+1+1+1+1+1+1)))");
        assertThat(printer.print(nestedFn, 5, true, false), is("abs(sqrt(ln(((... + ...) + 1))))"));
    }

    @Test
    public void testMaxDepthFail() throws Exception {
        expectedException.expect(MaxDepthReachedException.class);
        expectedException.expectMessage("max depth of 5 reached while traversing symbol");

        Symbol nestedFn = sqlExpressions.asSymbol("abs(sqrt(ln(1+1+1+1+1+1+1+1)))");
        printer.print(nestedFn, 5, true, true);
    }

    @Test
    public void testStyles() throws Exception {
        Symbol nestedFn = sqlExpressions.asSymbol("abs(sqrt(ln(bar+\"select\"+1+1+1+1+1+1)))");
        assertThat(printer.print(nestedFn, SymbolPrinter.Style.FULL_QUALIFIED), is("abs(sqrt(ln((((((((doc.formatter.bar + doc.formatter.\"select\") + 1) + 1) + 1) + 1) + 1) + 1))))"));
        assertThat(printer.print(nestedFn, SymbolPrinter.Style.SIMPLE), is("abs(sqrt(ln((((((((bar + \"select\") + 1) + 1) + 1) + 1) + 1) + 1))))"));
        assertThat(printer.print(nestedFn, SymbolPrinter.Style.PARSEABLE), is("abs(sqrt(ln((((((((doc.formatter.bar + doc.formatter.\"select\") + 1) + 1) + 1) + 1) + 1) + 1))))"));
        assertThat(printer.print(nestedFn, SymbolPrinter.Style.PARSEABLE_NOT_QUALIFIED), is("abs(sqrt(ln((((((((bar + \"select\") + 1) + 1) + 1) + 1) + 1) + 1))))"));
    }

    @Test
    public void testFormatOperatorWithStaticInstance() throws Exception {
        Symbol comparisonOperator = sqlExpressions.asSymbol("bar = 1 and foo = 2");
        String printed = SymbolPrinter.INSTANCE.printFullQualified(comparisonOperator);
        assertThat(
            printed,
            is("((doc.formatter.bar = 1) AND (doc.formatter.foo = '2'))")
        );
    }

    @Test
    public void testPrintFetchRefs() throws Exception {
        Field field = (Field) sqlExpressions.asSymbol("bar");
        Reference reference = ((AbstractTableRelation) field.relation()).resolveField(field);
        Symbol fetchRef = new FetchReference(sqlExpressions.asSymbol("1"), reference);
        assertPrint(fetchRef, "FETCH(1, doc.formatter.bar)");
    }

    @Test
    public void testPrintInputColumn() throws Exception {
        Symbol inputCol = new InputColumn(42);
        assertPrint(inputCol, "INPUT(42)");
    }

    @Test
    public void testPrintRelationColumn() throws Exception {
        Symbol relationColumn = new RelationColumn(new QualifiedName(TABLE_NAME), 42, DataTypes.STRING);
        assertPrint(relationColumn, "RELCOL(formatter, 42)");
    }

    @Test
    public void testPrintInOperator() throws Exception {
        Symbol inQuery = sqlExpressions.asSymbol("bar in (1)");
        assertPrint(inQuery, "(doc.formatter.bar = ANY(_array(1)))"); // internal in is rewritten to ANY
        FunctionImplementation impl = sqlExpressions.functions().getSafe(new FunctionIdent(InOperator.NAME, Arrays.<DataType>asList(DataTypes.LONG, new SetType(DataTypes.LONG))));
        Function fn = new Function(impl.info(), Arrays.asList(sqlExpressions.asSymbol("bar"), Literal.of(new SetType(DataTypes.LONG), ImmutableSet.of(1L, 2L))));
        assertPrint(fn, "(doc.formatter.bar IN (1, 2))");
        inQuery = sqlExpressions.asSymbol("bar in (1, abs(-10), 9)");
        assertPrint(inQuery, "(doc.formatter.bar = ANY(_array(1, abs(-10), 9)))");
        assertPrintStatic(inQuery, "(doc.formatter.bar = ANY(_array(1, abs(-10), 9)))");
    }

    @Test
    public void testPrintLikeOperator() throws Exception {
        Symbol likeQuery = sqlExpressions.asSymbol("foo like '%bla%'");
        assertPrint(likeQuery, "(doc.formatter.foo LIKE '%bla%')");
        assertPrintStatic(likeQuery, "(doc.formatter.foo LIKE '%bla%')");
        assertPrintIsParseable("(foo LIKE 'a')");
    }

    @Test
    public void testPrintAnyEqOperator() throws Exception {
        assertPrintingRoundTrip("foo = ANY (['a', 'b', 'c'])", "(doc.formatter.foo = ANY(_array('a', 'b', 'c')))");
        assertPrintingRoundTrip("foo = ANY(s_arr)", "(doc.formatter.foo = ANY(doc.formatter.s_arr))");
    }

    @Test
    public void testAnyNeqOperator() throws Exception {
        assertPrintingRoundTrip("not foo != ANY (['a', 'b', 'c'])", "(NOT (doc.formatter.foo <> ANY(_array('a', 'b', 'c'))))");
        assertPrintingRoundTrip("not foo != ANY(s_arr)", "(NOT (doc.formatter.foo <> ANY(doc.formatter.s_arr)))");
    }

    @Test
    public void testNot() throws Exception {
        assertPrintingRoundTrip("not foo = 'bar'", "(NOT (doc.formatter.foo = 'bar'))");
    }

    @Test
    public void testAnyLikeOperator() throws Exception {
        assertPrintingRoundTrip("foo LIKE ANY (s_arr)", "(doc.formatter.foo LIKE ANY(doc.formatter.s_arr))");
        assertPrintingRoundTrip("foo NOT LIKE ANY (['a', 'b', 'c'])", "(doc.formatter.foo NOT LIKE ANY(_array('a', 'b', 'c')))");
    }
}
