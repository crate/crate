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

package io.crate.planner.symbol;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.*;
import io.crate.planner.RowGranularity;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class SymbolFormatterTest extends CrateUnitTest {

    private void assertFormat(Symbol s, String formatted) {
        assertThat(SymbolFormatter.format(s), is(formatted));
    }

    @Test
    public void testFormatFunction() throws Exception {
        Function f = new Function(new FunctionInfo(
                new FunctionIdent("foo", Arrays.<DataType>asList(DataTypes.STRING, DataTypes.DOUBLE)), DataTypes.DOUBLE),
                Arrays.<Symbol>asList(Literal.newLiteral("bar"), Literal.newLiteral(3.4)));
        assertFormat(f, "foo(string, double)");
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
        assertFormat(f, "substr(string, long)");
    }

    @Test
    public void testSubstrFunctionWithLength() throws Exception {
        Function f = new Function(new FunctionInfo(
                new FunctionIdent("substr", Arrays.<DataType>asList(DataTypes.STRING, DataTypes.LONG, DataTypes.LONG)), DataTypes.STRING),
                Arrays.<Symbol>asList(Literal.newLiteral("foobar"), Literal.newLiteral(4), Literal.newLiteral(1)));
        assertFormat(f, "substr(string, long, long)");
    }

    @Test
    public void testFormatAggregation() throws Exception {
        Aggregation a = new Aggregation(new FunctionInfo(
                new FunctionIdent("agg", Arrays.<DataType>asList(DataTypes.INTEGER)), DataTypes.LONG, FunctionInfo.Type.AGGREGATE
        ), Arrays.<Symbol>asList(Literal.newLiteral(-127)), Aggregation.Step.ITER, Aggregation.Step.PARTIAL);

        assertFormat(a, "agg(integer)");
    }

    @Test
    public void testReference() throws Exception {
        Reference r = new Reference(new ReferenceInfo(new ReferenceIdent(
                new TableIdent("sys", "table"),
                new ColumnIdent("column", Arrays.asList("path", "nested"))),
                RowGranularity.DOC, DataTypes.STRING));
        assertFormat(r, "sys.table.column['path']['nested']");
    }

    @Test
    public void testDocReference() throws Exception {
        Reference r = new Reference(new ReferenceInfo(new ReferenceIdent(
                new TableIdent("doc", "table"),
                new ColumnIdent("column", Arrays.asList("path", "nested"))),
                RowGranularity.DOC, DataTypes.STRING));
        assertFormat(r, "table.column['path']['nested']");
    }

    @Test
    public void testDynamicReference() throws Exception {
        Reference r = new DynamicReference(new ReferenceInfo(new ReferenceIdent(
                new TableIdent("schema", "table"),
                new ColumnIdent("column", Arrays.asList("path", "nested"))),
                RowGranularity.DOC, DataTypes.STRING));
        assertFormat(r, "schema.table.column['path']['nested']");
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
        assertFormat(l, "{'array': [1, 2, 3], 'field': 'value', 'nestedMap': {'inner': -5.0E-5}}");
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
        assertThat(SymbolFormatter.format("This Symbol is formatted %s", f), is("This Symbol is formatted foo(string, null)"));
    }

    @Test
    public void testNull() throws Exception {
        assertFormat(Literal.newLiteral(DataTypes.UNDEFINED, null) , "NULL");
    }

    @Test
    public void testNullKey() throws Exception {
        assertFormat(Literal.newLiteral(new HashMap<String, Object>(){{ put("null", null);}}), "{'null': NULL}");
    }
}
