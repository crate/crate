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

package io.crate.analyze.symbol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class SymbolFormatterTest extends CrateUnitTest {

    SqlExpressions sqlExpressions;
    SymbolFormatter formatter;

    public static final String TABLE_NAME = "formatter";

    @Before
    public void prepare() throws Exception {
        DocTableInfo tableInfo = TestingTableInfo.builder(new TableIdent(DocSchemaInfo.NAME, TABLE_NAME), null)
                .add("foo", DataTypes.STRING)
                .add("bar", DataTypes.LONG)
                .add("CraZy", DataTypes.IP)
                .add("select", DataTypes.BYTE)
                .build();
        Map<QualifiedName, AnalyzedRelation> sources = ImmutableMap.<QualifiedName, AnalyzedRelation>builder()
                .put(QualifiedName.of(TABLE_NAME), new TableRelation(tableInfo))
                .build();
        sqlExpressions = new SqlExpressions(sources);
        formatter = new SymbolFormatter(sqlExpressions.analysisMD().functions());
    }

    private void assertFormat(Symbol s, String formatted) {
        assertThat(formatter.formatFullQualified(s), is(formatted));
    }

    private void assertFormatIsParseable(String sql) {
        Symbol symbol = sqlExpressions.asSymbol(sql);
        String formatted = formatter.formatSimple(symbol);
        Symbol formattedSymbol = sqlExpressions.asSymbol(formatted);
        assertThat(symbol, is(formattedSymbol));
    }

    @Test
    public void testFormatFunction() throws Exception {
        Function f = new Function(new FunctionInfo(
                new FunctionIdent("foo", Arrays.<DataType>asList(DataTypes.STRING, DataTypes.DOUBLE)), DataTypes.DOUBLE),
                Arrays.<Symbol>asList(Literal.newLiteral("bar"), Literal.newLiteral(3.4)));
        assertFormat(f, "foo('bar', 3.4)");
    }

    @Test
    public void testFormatFunctionWithoutArgs() throws Exception {
        Function f = new Function(new FunctionInfo(
                new FunctionIdent("baz", ImmutableList.<DataType>of()), DataTypes.DOUBLE),
                ImmutableList.<Symbol>of());
        assertFormat(f, "baz()");
    }

    @Test
    public void testSubstrFunction() throws Exception {
        Function f = new Function(new FunctionInfo(
                new FunctionIdent("substr", Arrays.<DataType>asList(DataTypes.STRING, DataTypes.LONG)), DataTypes.STRING),
                Arrays.<Symbol>asList(Literal.newLiteral("foobar"), Literal.newLiteral(4)));
        assertFormat(f, "substr('foobar', 4)");
    }

    @Test
    public void testSubstrFunctionWithLength() throws Exception {
        Function f = new Function(new FunctionInfo(
                new FunctionIdent("substr", Arrays.<DataType>asList(DataTypes.STRING, DataTypes.LONG, DataTypes.LONG)), DataTypes.STRING),
                Arrays.<Symbol>asList(Literal.newLiteral("foobar"), Literal.newLiteral(4), Literal.newLiteral(1)));
        assertFormat(f, "substr('foobar', 4, 1)");
    }

    @Test
    public void testFormatAggregation() throws Exception {
        Aggregation a = Aggregation.partialAggregation(new FunctionInfo(
                new FunctionIdent("agg", Collections.<DataType>singletonList(DataTypes.INTEGER)), DataTypes.LONG, FunctionInfo.Type.AGGREGATE
        ), DataTypes.LONG, Collections.<Symbol>singletonList(Literal.newLiteral(-127)));

        assertFormat(a, "agg(-127)");
    }

    @Test
    public void testReference() throws Exception {
        Reference r = new Reference(new ReferenceInfo(new ReferenceIdent(
                new TableIdent("sys", "table"),
                new ColumnIdent("column", Arrays.asList("path", "nested"))),
                RowGranularity.DOC, DataTypes.STRING));
        assertFormat(r, "sys.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testDocReference() throws Exception {
        Reference r = new Reference(new ReferenceInfo(new ReferenceIdent(
                new TableIdent("doc", "table"),
                new ColumnIdent("column", Arrays.asList("path", "nested"))),
                RowGranularity.DOC, DataTypes.STRING));
        assertFormat(r, "doc.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testDynamicReference() throws Exception {
        Reference r = new DynamicReference(new ReferenceInfo(new ReferenceIdent(
                new TableIdent("schema", "table"),
                new ColumnIdent("column", Arrays.asList("path", "nested"))),
                RowGranularity.DOC, DataTypes.STRING));
        assertFormat(r, "schema.\"table\".\"column\"['path']['nested']");
    }

    @Test
    public void testReferenceEscaped() throws Exception {
        Reference r = new Reference(new ReferenceInfo(new ReferenceIdent(
                new TableIdent("doc", "table"),
                new ColumnIdent("colum\"n")),
                RowGranularity.DOC, DataTypes.STRING));
        assertFormat(r, "doc.\"table\".\"colum\"\"n\"");
    }

    @Test
    public void testLiteralEscaped() throws Exception {
        assertFormat(Literal.newLiteral("bla'bla"), "'bla''bla'");

    }

    @Test
    public void testObjectLiteral() throws Exception {
        Literal<Map<String, Object>> l = Literal.newLiteral(new HashMap<String, Object>(){{
            put("field", "value");
            put("array", new Integer[]{1,2,3});
            put("nestedMap", new HashMap<String, Object>(){{
                put("inner", -0.00005d);
            }});
        }});
        assertFormat(l, "{\"array\"=[1, 2, 3], \"field\"='value', \"nestedMap\"={\"inner\"=-5.0E-5}}");
    }

    @Test
    public void testBooleanLiteral() throws Exception {
        Literal<Boolean> f = Literal.newLiteral(false);
        assertFormat(f, "false");
        Literal<Boolean> t = Literal.newLiteral(true);
        assertFormat(t, "true");
    }

    @Test
    public void visitStringLiteral() throws Exception {
        Literal<BytesRef> l = Literal.newLiteral("fooBar");
        assertFormat(l, "'fooBar'");
    }

    @Test
    public void visitDoubleLiteral() throws Exception {
        Literal<Double> d = Literal.newLiteral(-500.88765d);
        assertFormat(d, "-500.88765");
    }

    @Test
    public void visitFloatLiteral() throws Exception {
        Literal<Float> f = Literal.newLiteral(500.887f);
        assertFormat(f, "500.887");
    }

    @Test
    public void testProcess() throws Exception {
        Function f = new Function(new FunctionInfo(
                new FunctionIdent("foo", Arrays.<DataType>asList(DataTypes.STRING, DataTypes.UNDEFINED)), DataTypes.DOUBLE),
                Arrays.<Symbol>asList(Literal.newLiteral("bar"), Literal.newLiteral(3.4)));
        assertThat(SymbolFormatter.formatTmpl("This Symbol is formatted %s", f), is("This Symbol is formatted foo('bar', 3.4)"));
    }

    @Test
    public void testExtract() throws Exception {
        assertFormatIsParseable("extract(century from '1970-01-01')");
        assertFormatIsParseable("extract(day_of_week from 0)");
    }

    @Test
    public void testIsNull() throws Exception {
        assertFormatIsParseable("null IS NULL");
        assertFormatIsParseable("formatter.foo IS NULL");
        assertFormatIsParseable("'123' IS NULL");
    }

    @Test
    public void testQueries() throws Exception {
        assertFormatIsParseable("(1 + formatter.bar) * 4 = 14");
    }

    @Test
    public void testCast() throws Exception {
        assertFormatIsParseable("CAST (formatter.bar AS TIMESTAMP)");
        assertFormatIsParseable("CAST (TRUE AS string)");
        assertFormatIsParseable("CAST (1+2 AS string)");
    }

    @Test
    public void testNull() throws Exception {
        assertFormat(Literal.newLiteral(DataTypes.UNDEFINED, null) , "NULL");
    }

    @Test
    public void testNullKey() throws Exception {
        assertFormat(Literal.newLiteral(new HashMap<String, Object>(){{ put("null", null);}}), "{\"null\"=NULL}");
    }

    @Test
    public void testNativeArray() throws Exception {
        assertFormat(
                Literal.newLiteral(DataTypes.GEO_SHAPE, ImmutableMap.of("type", "Point", "coordinates", new double[]{1.0d, 2.0d})),
                "{\"coordinates\"=[1.0, 2.0], \"type\"='Point'}");
    }

    @Test
    public void testFormatQualified() throws Exception {
        Symbol ref = sqlExpressions.asSymbol("doc.formatter.\"CraZy\"");
        assertThat(formatter.format(ref, 10, true, false), is("doc.formatter.\"CraZy\""));
        assertThat(formatter.format(ref, 10, false, false), is("\"CraZy\""));
    }

    @Test
    public void testMaxDepthEllipsis() throws Exception {
        Symbol nestedFn = sqlExpressions.asSymbol("abs(sqrt(ln(1+1+1+1+1+1+1+1)))");
        assertThat(formatter.format(nestedFn, 5, true, false), is("abs(sqrt(ln(((... + ...) + 1))))"));
    }

    @Test
    public void testMaxDepthFail() throws Exception {
        expectedException.expect(SymbolFormatter.MaxDepthReachedException.class);
        expectedException.expectMessage("max depth of 5 reached while traversing symbol");

        Symbol nestedFn = sqlExpressions.asSymbol("abs(sqrt(ln(1+1+1+1+1+1+1+1)))");
        formatter.format(nestedFn, 5, true, true);
    }

    @Test
    public void testStyles() throws Exception {
        Symbol nestedFn = sqlExpressions.asSymbol("abs(sqrt(ln(bar+\"select\"+1+1+1+1+1+1)))");
        assertThat(formatter.format(nestedFn, SymbolFormatter.Style.FULL_QUALIFIED), is("abs(sqrt(ln((((((((doc.formatter.bar + doc.formatter.\"select\") + 1) + 1) + 1) + 1) + 1) + 1))))"));
        assertThat(formatter.format(nestedFn, SymbolFormatter.Style.SIMPLE), is("abs(sqrt(ln((((((((bar + \"select\") + 1) + 1) + 1) + 1) + 1) + 1))))"));
        assertThat(formatter.format(nestedFn, SymbolFormatter.Style.PARSEABLE), is("abs(sqrt(ln((((((((doc.formatter.bar + doc.formatter.\"select\") + 1) + 1) + 1) + 1) + 1) + 1))))"));
        assertThat(formatter.format(nestedFn, SymbolFormatter.Style.PARSEABLE_NOT_QUALIFIED), is("abs(sqrt(ln((((((((bar + \"select\") + 1) + 1) + 1) + 1) + 1) + 1))))"));


    }
}
