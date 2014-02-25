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
import org.cratedb.DataType;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SymbolFormatterTest {


    private void assertFormat(Symbol s, String formatted) {
        assertThat(SymbolFormatter.format(s), is(formatted));
    }

    @Test
    public void testFormatFunction() throws Exception {
        Function f = new Function(new FunctionInfo(
                new FunctionIdent("foo", Arrays.asList(DataType.STRING, DataType.DOUBLE)), DataType.DOUBLE, false),
                Arrays.<Symbol>asList(new StringLiteral("bar"), new DoubleLiteral(3.4)));
        assertFormat(f, "double foo(string, double)");
    }

    @Test
    public void testFormatFunctionWithoutArgs() throws Exception {
        Function f = new Function(new FunctionInfo(
                new FunctionIdent("baz", ImmutableList.<DataType>of()), DataType.DOUBLE, false),
                ImmutableList.<Symbol>of());
        assertFormat(f, "double baz()");
    }

    @Test
    public void testFormatAggregation() throws Exception {
        Aggregation a = new Aggregation(new FunctionInfo(
                new FunctionIdent("agg", Arrays.asList(DataType.INTEGER)), DataType.LONG, true
        ), Arrays.<Symbol>asList(new IntegerLiteral(-127)), Aggregation.Step.ITER, Aggregation.Step.PARTIAL);

        assertFormat(a, "long agg(integer)");
    }

    @Test
    public void testReference() throws Exception {
        Reference r = new Reference(new ReferenceInfo(new ReferenceIdent(
                new TableIdent("sys", "table"),
                new ColumnIdent("column", Arrays.asList("path", "nested"))),
                RowGranularity.DOC, DataType.STRING));
        assertFormat(r, "sys.table.column['path']['nested']");
    }

    @Test
    public void testDocReference() throws Exception {
        Reference r = new Reference(new ReferenceInfo(new ReferenceIdent(
                new TableIdent("doc", "table"),
                new ColumnIdent("column", Arrays.asList("path", "nested"))),
                RowGranularity.DOC, DataType.STRING));
        assertFormat(r, "table.column['path']['nested']");
    }

    @Test
    public void testDynamicReference() throws Exception {
        Reference r = new DynamicReference(new ReferenceInfo(new ReferenceIdent(
                new TableIdent("schema", "table"),
                new ColumnIdent("column", Arrays.asList("path", "nested"))),
                RowGranularity.DOC, DataType.STRING));
        assertFormat(r, "schema.table.column['path']['nested']");
    }

    @Test
    public void testObjectLiteral() throws Exception {
        ObjectLiteral l = new ObjectLiteral(new HashMap<String, Object>(){{
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
        BooleanLiteral f = BooleanLiteral.FALSE;
        assertFormat(f, "false");
        BooleanLiteral t = BooleanLiteral.TRUE;
        assertFormat(t, "true");
    }

    @Test
    public void visitStringLiteral() throws Exception {
        StringLiteral l = new StringLiteral("fooBar");
        assertFormat(l, "'fooBar'");
    }

    @Test
    public void visitDoubleLiteral() throws Exception {
        DoubleLiteral d = new DoubleLiteral(-500.88765d);
        assertFormat(d, "-500.88765");
    }

    @Test
    public void visitFloatLiteral() throws Exception {
        FloatLiteral f = new FloatLiteral(500.887f);
        assertFormat(f, "500.887");
    }

    @Test
    public void testProcess() throws Exception {
        Function f = new Function(new FunctionInfo(
                new FunctionIdent("foo", Arrays.asList(DataType.STRING, DataType.NULL)), DataType.DOUBLE, false),
                Arrays.<Symbol>asList(new StringLiteral("bar"), new DoubleLiteral(3.4)));
        assertThat(SymbolFormatter.format("This Symbol is formatted %s", f), is("This Symbol is formatted double foo(string, null)"));
    }

    @Test
    public void testNull() throws Exception {
        assertFormat(new ObjectLiteral(null), "NULL");
    }

    @Test
    public void testNullKey() throws Exception {
        assertFormat(new ObjectLiteral(new HashMap<String, Object>(){{ put("null", null);}}), "{'null': NULL}");
    }
}
