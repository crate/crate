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

package io.crate.operation.scalar.cast;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Literal;
import io.crate.metadata.FunctionIdent;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class ToIpFunctionTest extends AbstractScalarFunctionsTest {

    private final String functionName = CastFunctionResolver.FunctionNames.TO_IP;

    private BytesRef evaluate(Object value, DataType type) {
        ToPrimitiveFunction fn = getFunction(functionName, type);
        return (BytesRef) fn.evaluate(Literal.newLiteral(type, value));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertThat(normalize(functionName, "127.0.0.1", DataTypes.STRING), isLiteral(new BytesRef("127.0.0.1"), DataTypes.IP));
        assertThat(normalize(functionName, new BytesRef("127.0.0.1"), DataTypes.STRING), isLiteral(new BytesRef("127.0.0.1"), DataTypes.IP));
        assertThat(normalize(functionName, 2130706433L, DataTypes.LONG), isLiteral(new BytesRef("127.0.0.1"), DataTypes.IP));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertThat(evaluate("127.0.0.1", DataTypes.STRING), is(new BytesRef("127.0.0.1")));
        assertThat(evaluate(new BytesRef("127.0.0.1"), DataTypes.STRING), is(new BytesRef("127.0.0.1")));
        assertThat(evaluate(2130706433L, DataTypes.LONG), is(new BytesRef("127.0.0.1")));
        assertThat(evaluate(null, DataTypes.STRING), nullValue());
        assertThat(evaluate(null, DataTypes.LONG), nullValue());
    }

    @Test
    public void testEvaluateInvalidStringValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Failed to validate ip [not.a.valid.ip], not a valid ipv4 address");
        assertThat(evaluate("not.a.valid.ip", DataTypes.STRING), is(nullValue()));
    }

    @Test
    public void testEvaluateInvalidBytesRefValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Failed to validate ip [257.0.1.1], not a valid ipv4 address");
        assertThat(evaluate(new BytesRef("257.0.1.1"), DataTypes.STRING), is(nullValue()));
    }

    @Test
    public void testEvaluateInvalidLongValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Failed to convert long value: -1 to ipv4 address");
        assertThat(evaluate(-1L, DataTypes.LONG), is(nullValue()));
    }

    @Test
    public void testNormalizeInvalidStringValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("cannot cast '257.0.0.0' to ip");
        normalize(functionName, "257.0.0.0", DataTypes.STRING);
    }

    @Test
    public void testNormalizeInvalidLongValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("cannot cast -1 to ip");
        normalize(functionName, -1L, DataTypes.LONG);
    }

    @Test
    public void testNormalizeInvalidBytesRefValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("cannot cast '257.0.0.0' to ip");
        normalize(functionName, new BytesRef("257.0.0.0"), DataTypes.STRING);
    }

    @Test
    public void testInvalidObjectType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("type 'object' not supported for conversion");
        functions.get(new FunctionIdent(functionName, ImmutableList.<DataType>of(DataTypes.OBJECT)));
    }

}
