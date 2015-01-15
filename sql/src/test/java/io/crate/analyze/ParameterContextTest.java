/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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


package io.crate.analyze;

import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;


public class ParameterContextTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final static Object[] EMPTY_ARGS = new Object[0];
    private final static Object[][] EMPTY_BULK_ARGS = new Object[0][];

    @Test
    public void testEmpty() throws Exception {
        ParameterContext ctx = new ParameterContext(EMPTY_ARGS, EMPTY_BULK_ARGS);
        assertFalse(ctx.hasBulkParams());
        assertThat(ctx.parameters(), is(new Object[0]));
    }

    @Test
    public void testArgs() throws Exception {
        Object[] args = new Object[] { true, 1, null, "string" };
        ParameterContext ctx = new ParameterContext(args, EMPTY_BULK_ARGS);
        assertFalse(ctx.hasBulkParams());
        assertThat(ctx.parameters(), is(args));
    }

    @Test
    public void testBulkArgs() throws Exception {
        Object[][] bulkArgs = new Object[][] {
                new Object[]{ true, 1, "foo", null, new String[]{null} },
                new Object[]{ false, 2, "bar", new Object[0], new String[]{"foo", "bar"} }
        };
        ParameterContext ctx = new ParameterContext(EMPTY_ARGS, bulkArgs);
        assertTrue(ctx.hasBulkParams());
        ctx.setBulkIdx(0);
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(0), true);
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(1), 1);
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(2), "foo");
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(3), null, new ArrayType(DataTypes.UNDEFINED));
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(4), new BytesRef[]{null}, new ArrayType(DataTypes.STRING));
        ctx.setBulkIdx(1);
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(0), false);
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(1), 2);
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(2), "bar");
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(3), new Object[0], new ArrayType(DataTypes.UNDEFINED));
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(4), new BytesRef[]{ new BytesRef("foo"), new BytesRef("bar") }, new ArrayType(DataTypes.STRING));
    }

    @Test
    public void testBulkArgsMixedNumberOfArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("mixed number of arguments inside bulk arguments");
        Object[][] bulkArgs = new Object[][] {
                new Object[]{ "foo" },
                new Object[]{ false, 1 }
        };
        new ParameterContext(EMPTY_ARGS, bulkArgs);
    }

    @Test
    public void testBulkArgsMixedSimpleTypes() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("argument 1 of bulk arguments contains mixed data types");
        Object[][] bulkArgs = new Object[][] {
                new Object[]{ true },
                new Object[]{ false },
                new Object[]{ "maybe" }
        };
        new ParameterContext(EMPTY_ARGS, bulkArgs);
    }

    @Test
    public void testBulkArgsMixedArrayTypes() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("argument 1 of bulk arguments contains mixed data types");
        Object[][] bulkArgs = new Object[][] {
                new Object[]{ new String[]{ "foo" } },
                new Object[]{ new Integer[]{ 1 } }
        };
        new ParameterContext(EMPTY_ARGS, bulkArgs);
    }


    @Test
    public void testBulkArgsNested() throws Exception {
        HashMap<String, Object[]> obj1 = new HashMap();
        obj1.put("a", new String[]{null});
        obj1.put("b", new Integer[]{1});

        HashMap<String, Object[]> obj2 = new HashMap();
        obj2.put("a", new String[]{"foo"});
        obj2.put("b", new Float[]{0.5f});

        Object[][] bulkArgs = new Object[][] {
                new Object[]{obj1},
                new Object[]{obj2},
        };
        ParameterContext ctx = new ParameterContext(EMPTY_ARGS, bulkArgs);
        ctx.setBulkIdx(0);
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(0), obj1, DataTypes.OBJECT);
        ctx.setBulkIdx(1);
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(0), obj2, DataTypes.OBJECT);
    }

    @Test
    public void testBulkNestedNested() throws Exception {
        Object[][] bulkArgs = new Object[][] {
                new Object[] { new String[][] { new String[]{ null } } },
                new Object[] { new String[][] { new String[]{ "foo" } } },
        };
        ParameterContext ctx = new ParameterContext(EMPTY_ARGS, bulkArgs);
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(0), bulkArgs[0][0], new ArrayType(new ArrayType(DataTypes.STRING)));
    }

    @Test
    public void testBulkNestedNestedEmpty() throws Exception {
        Object[][] bulkArgs = new Object[][] {
                new Object[] { new String[][] { new String[0] } },
                new Object[] { new String[][] { new String[0] } },
        };
        ParameterContext ctx = new ParameterContext(EMPTY_ARGS, bulkArgs);
        TestingHelpers.assertLiteralSymbol(ctx.getAsSymbol(0), bulkArgs[0][0], new ArrayType(new ArrayType(DataTypes.UNDEFINED)));
    }
}
