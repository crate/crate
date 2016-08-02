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

import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.HashMap;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.is;


public class ParameterContextTest extends CrateUnitTest {

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
        assertThat(ctx.getAsSymbol(0), isLiteral(true));
        assertThat(ctx.getAsSymbol(1), isLiteral(1));
        assertThat(ctx.getAsSymbol(2), isLiteral("foo"));
        assertThat(ctx.getAsSymbol(3), isLiteral(null, DataTypes.UNDEFINED));
        assertThat(ctx.getAsSymbol(4), isLiteral(new BytesRef[]{null}, new ArrayType(DataTypes.UNDEFINED)));
        ctx.setBulkIdx(1);
        assertThat(ctx.getAsSymbol(0), isLiteral(false));
        assertThat(ctx.getAsSymbol(1), isLiteral(2));
        assertThat(ctx.getAsSymbol(2), isLiteral("bar"));
        assertThat(ctx.getAsSymbol(3), isLiteral(new Object[0], new ArrayType(DataTypes.UNDEFINED)));
        assertThat(ctx.getAsSymbol(4), isLiteral(new BytesRef[]{ new BytesRef("foo"), new BytesRef("bar") }, new ArrayType(DataTypes.STRING)));
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
    public void testBulkArgsNested() throws Exception {
        HashMap<String, Object[]> obj1 = new HashMap<>();
        obj1.put("a", new String[]{null});
        obj1.put("b", new Integer[]{1});

        HashMap<String, Object[]> obj2 = new HashMap<>();
        obj2.put("a", new String[]{"foo"});
        obj2.put("b", new Float[]{0.5f});

        Object[][] bulkArgs = new Object[][] {
                new Object[]{obj1},
                new Object[]{obj2},
        };
        ParameterContext ctx = new ParameterContext(EMPTY_ARGS, bulkArgs);
        ctx.setBulkIdx(0);
        assertThat(ctx.getAsSymbol(0), isLiteral(obj1, DataTypes.OBJECT));
        ctx.setBulkIdx(1);
        assertThat(ctx.getAsSymbol(0), isLiteral(obj2, DataTypes.OBJECT));
    }

    @Test
    public void testBulkNestedNested() throws Exception {
        Object[][] bulkArgs = new Object[][] {
                new Object[] { new String[][] { new String[]{ null } } },
                new Object[] { new String[][] { new String[]{ "foo" } } },
        };
        ParameterContext ctx = new ParameterContext(EMPTY_ARGS, bulkArgs);
        assertThat(ctx.getAsSymbol(0), isLiteral(bulkArgs[0][0], new ArrayType(new ArrayType(DataTypes.UNDEFINED))));
    }

    @Test
    public void testBulkNestedNestedEmpty() throws Exception {
        Object[][] bulkArgs = new Object[][] {
                new Object[] { new String[][] { new String[0] } },
                new Object[] { new String[][] { new String[0] } },
        };
        ParameterContext ctx = new ParameterContext(EMPTY_ARGS, bulkArgs);
        assertThat(ctx.getAsSymbol(0), isLiteral(bulkArgs[0][0], new ArrayType(new ArrayType(DataTypes.UNDEFINED))));
    }
}
