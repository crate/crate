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

package io.crate.operation.scalar.arithmetic;

import io.crate.analyze.symbol.Literal;
import io.crate.metadata.FunctionIdent;
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.isFunction;
import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TrigonometricFunctionTest extends AbstractScalarFunctionsTest {

    private TrigonometricFunction getFunction(String name, DataType type) {
        return (TrigonometricFunction) functions.get(new FunctionIdent(name, Arrays.asList(type)));
    }

    private Number evaluate(Number number, String name, DataType type) {
        return getFunction(name, type).evaluate((Input) Literal.newLiteral(type, number));
    }

    private Number evaluateSin(Number number, DataType type) {
        return evaluate(number, TrigonometricFunction.SinFunction.NAME, type);
    }

    @Test
    public void testEvaluateSin() throws Exception {
        assertThat((Double) evaluateSin(1.0, DataTypes.DOUBLE), is(0.8414709848078965));
        assertThat((Double) evaluateSin(2.0F, DataTypes.FLOAT), is(0.9092974268256817));
        assertThat((Double) evaluateSin(3L, DataTypes.LONG), is(0.1411200080598672));
        assertThat((Double) evaluateSin(4, DataTypes.INTEGER), is(-0.7568024953079282));
        assertThat((Double) evaluateSin((short) 5, DataTypes.SHORT), is(-0.9589242746631385));
    }

    @Test
    public void testEvaluateSinOnNull() throws Exception {
        for (DataType type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertThat(evaluateSin(null, type), nullValue());
        }
    }

    @Test
    public void testNormalizeSin() throws Exception {
        assertNormalize("sin(1.0)", isLiteral(0.8414709848078965, DataTypes.DOUBLE));
        assertNormalize("sin(cast (2.0 as float))", isLiteral(0.9092974268256817, DataTypes.DOUBLE));
        assertNormalize("sin(cast (3 as long))", isLiteral(0.1411200080598672, DataTypes.DOUBLE));
        assertNormalize("sin(cast (4 as integer))", isLiteral(-0.7568024953079282, DataTypes.DOUBLE));
        assertNormalize("sin(cast (5 as short))", isLiteral(-0.9589242746631385, DataTypes.DOUBLE));
    }

    @Test
    public void testNormalizeSinOnNull() throws Exception {
        assertNormalize("sin(cast(null as double))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("sin(cast(null as float))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("sin(cast(null as long))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("sin(cast(null as integer))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("sin(cast(null as short))", isLiteral(null, DataTypes.DOUBLE));
    }

    @Test
    public void testNormalizeSinReference() throws Exception {
        assertNormalize("sin(age)", isFunction(TrigonometricFunction.SinFunction.NAME));
    }
}
