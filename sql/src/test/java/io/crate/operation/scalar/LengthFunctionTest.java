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

package io.crate.operation.scalar;

import io.crate.analyze.symbol.Literal;
import io.crate.operation.Input;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

public class LengthFunctionTest extends AbstractScalarFunctionsTest {

    private static final String BIT_LENGTH_FUNCTION_NAME = LengthFunction.BitLengthFunction.NAME;
    private static final String CHAR_LENGTH_FUNCTION_NAME = LengthFunction.CharLengthFunction.NAME;
    private static final String OCTET_LENGTH_FUNCTION_NAME = LengthFunction.OctetLengthFunction.NAME;

    private Integer evaluate(String functionName, Object value, DataType dataType) {
        return ((LengthFunction) getFunction(functionName, dataType))
                .evaluate((Input) Literal.newLiteral(dataType, value));
    }

    @Test
    public void testOctetLengthEvaluateOnString() throws Exception {
        assertThat(evaluate(OCTET_LENGTH_FUNCTION_NAME, new BytesRef("©rate"), DataTypes.STRING), is(6));
        assertThat(evaluate(OCTET_LENGTH_FUNCTION_NAME, new BytesRef("crate"), DataTypes.STRING), is(5));
        assertThat(evaluate(OCTET_LENGTH_FUNCTION_NAME, new BytesRef(""), DataTypes.STRING), is(0));
    }

    @Test
    public void testBitLengthEvaluateOnString() throws Exception {
        assertThat(evaluate(BIT_LENGTH_FUNCTION_NAME, new BytesRef("©rate"), DataTypes.STRING), is(48));
        assertThat(evaluate(BIT_LENGTH_FUNCTION_NAME, new BytesRef("crate"), DataTypes.STRING), is(40));
        assertThat(evaluate(BIT_LENGTH_FUNCTION_NAME, new BytesRef(""), DataTypes.STRING), is(0));
    }

    @Test
    public void testCharLengthEvaluateOnString() throws Exception {
        assertThat(evaluate(CHAR_LENGTH_FUNCTION_NAME, new BytesRef("©rate"), DataTypes.STRING), is(5));
        assertThat(evaluate(CHAR_LENGTH_FUNCTION_NAME, new BytesRef("crate"), DataTypes.STRING), is(5));
        assertThat(evaluate(CHAR_LENGTH_FUNCTION_NAME, new BytesRef(""), DataTypes.STRING), is(0));
    }

    @Test
    public void testOctetLengthEvaluateOnNull() throws Exception {
        assertThat(evaluate(BIT_LENGTH_FUNCTION_NAME, null, DataTypes.UNDEFINED), nullValue());
        assertThat(evaluate(BIT_LENGTH_FUNCTION_NAME, null, DataTypes.STRING), nullValue());
    }

    @Test
    public void testBitLengthEvaluateOnNull() throws Exception {
        assertThat(evaluate(BIT_LENGTH_FUNCTION_NAME, null, DataTypes.UNDEFINED), nullValue());
        assertThat(evaluate(BIT_LENGTH_FUNCTION_NAME, null, DataTypes.STRING), nullValue());
    }

    @Test
    public void testCharLengthEvaluateOnNull() throws Exception {
        assertThat(evaluate(CHAR_LENGTH_FUNCTION_NAME, null, DataTypes.UNDEFINED), nullValue());
        assertThat(evaluate(CHAR_LENGTH_FUNCTION_NAME, null, DataTypes.STRING), nullValue());
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertThat(normalize(BIT_LENGTH_FUNCTION_NAME, new BytesRef("©"), DataTypes.STRING), isLiteral(16));
        assertThat(normalize(OCTET_LENGTH_FUNCTION_NAME, new BytesRef("©"), DataTypes.STRING), isLiteral(2));
        assertThat(normalize(CHAR_LENGTH_FUNCTION_NAME, new BytesRef("©"), DataTypes.STRING), isLiteral(1));
        assertThat(normalize(BIT_LENGTH_FUNCTION_NAME, null, DataTypes.UNDEFINED), isLiteral(null, DataTypes.INTEGER));
    }
}
