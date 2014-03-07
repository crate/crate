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

import io.crate.DataType;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

public class ParameterTest {

    @Test
    public void testResolveBoolean() throws Exception {
        assertThat(new Parameter(true).toLiteral(), Matchers.<Literal>is(BooleanLiteral.TRUE));
        assertThat(new Parameter(false).toLiteral(), Matchers.<Literal>is(BooleanLiteral.FALSE));
        assertThat(new Parameter(null).toLiteral(DataType.BOOLEAN), Matchers.instanceOf(Null.class));
    }

    @Test
    public void testResolveObject() throws Exception {
        assertThat(new Parameter(new HashMap<String, Object>()).toLiteral(), Matchers.<Literal>instanceOf(ObjectLiteral.class));
        assertThat(new Parameter(new HashMap<String, Object>()).toLiteral(DataType.OBJECT), Matchers.<Literal>instanceOf(ObjectLiteral.class));
    }

    @Test
    public void testResolveNumberTypes() throws Exception {
        assertThat(new Parameter(1L).toLiteral(), Matchers.<Literal>instanceOf(LongLiteral.class));
        assertThat(new Parameter(1L).toLiteral(DataType.BYTE), Matchers.<Literal>instanceOf(ByteLiteral.class));
        assertThat(new Parameter(1L).toLiteral(DataType.SHORT), Matchers.<Literal>instanceOf(ShortLiteral.class));
        assertThat(new Parameter(1L).toLiteral(DataType.INTEGER), Matchers.<Literal>instanceOf(IntegerLiteral.class));
        assertThat(new Parameter(1L).toLiteral(DataType.LONG), Matchers.<Literal>instanceOf(LongLiteral.class));
        assertThat(new Parameter(1L).toLiteral(DataType.FLOAT), Matchers.<Literal>instanceOf(FloatLiteral.class));
        assertThat(new Parameter(1L).toLiteral(DataType.DOUBLE), Matchers.<Literal>instanceOf(DoubleLiteral.class));
    }

    @Test
    public void testResolveTimestamp() throws Exception {
        assertThat(new Parameter(0L).toLiteral(DataType.TIMESTAMP), Matchers.<Literal>is(new TimestampLiteral(0L)));
        assertThat(new Parameter("1970-01-01T00:00:00").toLiteral(DataType.TIMESTAMP), Matchers.<Literal>is(new TimestampLiteral(0L)));
    }

    @Test( expected = IllegalArgumentException.class )
    public void testResolveInvalidTimestamp() throws Exception {
        new Parameter("a").toLiteral(DataType.TIMESTAMP);
    }

    @Test
    public void testResolveFromString() throws Exception {
        assertThat(new Parameter("123").toLiteral(), Matchers.<Literal>is(new StringLiteral("123")));

        assertThat(new Parameter("123").toLiteral(DataType.INTEGER), Matchers.<Literal>is(new IntegerLiteral(123)));
        assertThat(new Parameter("123").toLiteral(DataType.FLOAT), Matchers.<Literal>is(new FloatLiteral(123)));
        assertThat(new Parameter("true").toLiteral(DataType.BOOLEAN), Matchers.<Literal>is(BooleanLiteral.TRUE));
    }

    @Test
    public void testResolveArrayTypesFromEmptyArray() throws Exception {
        assertThat(((ArrayLiteral)new Parameter(new Map[0]).toLiteral()).itemType(), Matchers.is(DataType.NULL));
        assertThat(((ArrayLiteral)new Parameter(new Map[0]).toLiteral(DataType.OBJECT_ARRAY)).itemType(), Matchers.is(DataType.OBJECT));
        assertThat(((ArrayLiteral)new Parameter(new Map[0]).toLiteral(DataType.FLOAT_ARRAY)).itemType(), Matchers.is(DataType.FLOAT));
    }

    @Test
    public void testResolveArrayType() throws Exception {
        assertThat(((ArrayLiteral)new Parameter(new Boolean[]{true, true, false}).toLiteral()).itemType(), Matchers.is(DataType.BOOLEAN));
    }

    @Test( expected = IllegalArgumentException.class)
    public void testResolveArrayTypeToInvalidType() throws Exception {
        new Parameter(new Boolean[]{true, true, false}).toLiteral(DataType.INTEGER_ARRAY);
    }

    @Test( expected = IllegalArgumentException.class)
    public void testResolveArrayTypeToInvalidPrimitiveType() throws Exception {
        new Parameter(new Boolean[]{true, true, false}).toLiteral(DataType.INTEGER);
    }
}
