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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

    private Number evaluateAsin(Number number, DataType type) {
        return evaluate(number, TrigonometricFunction.AsinFunction.NAME, type);
    }

    private Number evaluateCos(Number number, DataType type) {
        return evaluate(number, TrigonometricFunction.CosFunction.NAME, type);
    }

    private Number evaluateAcos(Number number, DataType type) {
        return evaluate(number, TrigonometricFunction.AcosFunction.NAME, type);
    }

    private Number evaluateTan(Number number, DataType type) {
        return evaluate(number, TrigonometricFunction.TanFunction.NAME, type);
    }

    private Number evaluateAtan(Number number, DataType type) {
        return evaluate(number, TrigonometricFunction.AtanFunction.NAME, type);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testEvaluate() throws Exception {
        // SinFunction
        assertThat((Double) evaluateSin(1.0, DataTypes.DOUBLE), is(0.8414709848078965));
        assertThat((Double) evaluateSin(2.0F, DataTypes.FLOAT), is(0.9092974268256817));
        assertThat((Double) evaluateSin(3L, DataTypes.LONG), is(0.1411200080598672));
        assertThat((Double) evaluateSin(4, DataTypes.INTEGER), is(-0.7568024953079282));
        assertThat((Double) evaluateSin((short) 5, DataTypes.SHORT), is(-0.9589242746631385));

        // AsinFunction
        assertThat((Double) evaluateAsin(0.1234, DataTypes.DOUBLE), is(0.12371534584255098));
        assertThat((Double) evaluateAsin(0.4321F, DataTypes.FLOAT), is(0.44682008883801516));
        assertThat((Double) evaluateAsin(0L, DataTypes.LONG), is(0.0));
        assertThat((Double) evaluateAsin(1, DataTypes.INTEGER), is(1.5707963267948966));
        assertThat((Double) evaluateAsin((short) -1, DataTypes.SHORT), is(-1.5707963267948966));

        // CosFunction
        assertThat((Double) evaluateCos(1.0, DataTypes.DOUBLE), is(0.5403023058681398));
        assertThat((Double) evaluateCos(2.0F, DataTypes.FLOAT), is(-0.4161468365471424));
        assertThat((Double) evaluateCos(3L, DataTypes.LONG), is(-0.9899924966004454));
        assertThat((Double) evaluateCos(4, DataTypes.INTEGER), is(-0.6536436208636119));
        assertThat((Double) evaluateCos((short) 5, DataTypes.SHORT), is(0.28366218546322625));

        // AcosFunction
        assertThat((Double) evaluateAcos(0.1234, DataTypes.DOUBLE), is(1.4470809809523457));
        assertThat((Double) evaluateAcos(0.4321F, DataTypes.FLOAT), is(1.1239762379568814));
        assertThat((Double) evaluateAcos(0L, DataTypes.LONG), is(1.5707963267948966));
        assertThat((Double) evaluateAcos(1, DataTypes.INTEGER), is(0.0));
        assertThat((Double) evaluateAcos((short) -1, DataTypes.SHORT), is(3.141592653589793));

        // TanFunction
        assertThat((Double) evaluateTan(1.0, DataTypes.DOUBLE), is(1.5574077246549023));
        assertThat((Double) evaluateTan(2.0F, DataTypes.FLOAT), is(-2.185039863261519));
        assertThat((Double) evaluateTan(3L, DataTypes.LONG), is(-0.1425465430742778));
        assertThat((Double) evaluateTan(4, DataTypes.INTEGER), is(1.1578212823495777));
        assertThat((Double) evaluateTan((short) 5, DataTypes.SHORT), is(-3.380515006246586));

        // AtanFunction
        assertThat((Double) evaluateAtan(0.1234, DataTypes.DOUBLE), is(0.12277930094473836));
        assertThat((Double) evaluateAtan(0.4321F, DataTypes.FLOAT), is(0.4078690066146179));
        assertThat((Double) evaluateAtan(0L, DataTypes.LONG), is(0.0));
        assertThat((Double) evaluateAtan(1, DataTypes.INTEGER), is(0.7853981633974483));
        assertThat((Double) evaluateAtan((short) -1, DataTypes.SHORT), is(-0.7853981633974483));
    }

    @Test
    public void testEvaluateAsinOnIllegalArgument() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("asin(x): is defined only for values in the range of [-1.0, 1.0]");
        evaluateAsin(2.0, DataTypes.DOUBLE);
    }

    @Test
    public void testEvaluateAcosOnIllegalArgument() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("acos(x): is defined only for values in the range of [-1.0, 1.0]");
        evaluateAcos(2.0, DataTypes.DOUBLE);
    }

    @Test
    public void testEvaluateAtanOnIllegalArgument() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("atan(x): is defined only for values in the range of [-1.0, 1.0]");
        evaluateAtan(2.0, DataTypes.DOUBLE);
    }

    @Test
    public void testEvaluateOnNull() throws Exception {
        for (DataType type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            // SinFunction
            assertThat(evaluateSin(null, type), nullValue());

            // AsinFunction
            assertThat(evaluateAsin(null, type), nullValue());

            // CosFunction
            assertThat(evaluateCos(null, type), nullValue());

            // AcosFunction
            assertThat(evaluateAcos(null, type), nullValue());

            // TanFunction
            assertThat(evaluateTan(null, type), nullValue());

            // AtanFunction
            assertThat(evaluateAtan(null, type), nullValue());
        }
    }

    @Test
    public void testNormalize() throws Exception {
        // SinFunction
        assertNormalize("sin(1.0)", isLiteral(0.8414709848078965, DataTypes.DOUBLE));
        assertNormalize("sin(cast (2.0 as float))", isLiteral(0.9092974268256817, DataTypes.DOUBLE));
        assertNormalize("sin(3)", isLiteral(0.1411200080598672, DataTypes.DOUBLE));
        assertNormalize("sin(cast (4 as integer))", isLiteral(-0.7568024953079282, DataTypes.DOUBLE));
        assertNormalize("sin(cast (5 as short))", isLiteral(-0.9589242746631385, DataTypes.DOUBLE));

        // AsinFunction
        assertNormalize("asin(0.1234)", isLiteral(0.12371534584255098, DataTypes.DOUBLE));
        assertNormalize("asin(cast (0.4321 as float))", isLiteral(0.44682008883801516, DataTypes.DOUBLE));
        assertNormalize("asin(0)", isLiteral(0.0, DataTypes.DOUBLE));
        assertNormalize("asin(cast (1 as integer))", isLiteral(1.5707963267948966, DataTypes.DOUBLE));
        assertNormalize("asin(cast (-1 as short))", isLiteral(-1.5707963267948966, DataTypes.DOUBLE));

        // CosFunction
        assertNormalize("cos(1.0)", isLiteral(0.5403023058681398, DataTypes.DOUBLE));
        assertNormalize("cos(cast (2.0 as float))", isLiteral(-0.4161468365471424, DataTypes.DOUBLE));
        assertNormalize("cos(3)", isLiteral(-0.9899924966004454, DataTypes.DOUBLE));
        assertNormalize("cos(cast (4 as integer))", isLiteral(-0.6536436208636119, DataTypes.DOUBLE));
        assertNormalize("cos(cast (5 as short))", isLiteral(0.28366218546322625, DataTypes.DOUBLE));

        // AcosFunction
        assertNormalize("acos(0.1234)", isLiteral(1.4470809809523457, DataTypes.DOUBLE));
        assertNormalize("acos(cast (0.4321 as float))", isLiteral(1.1239762379568814, DataTypes.DOUBLE));
        assertNormalize("acos(0)", isLiteral(1.5707963267948966, DataTypes.DOUBLE));
        assertNormalize("acos(cast (1 as integer))", isLiteral(0.0, DataTypes.DOUBLE));
        assertNormalize("acos(cast (-1 as short))", isLiteral(3.141592653589793, DataTypes.DOUBLE));

        // TanFunction
        assertNormalize("tan(1.0)", isLiteral(1.5574077246549023, DataTypes.DOUBLE));
        assertNormalize("tan(cast (2.0 as float))", isLiteral(-2.185039863261519, DataTypes.DOUBLE));
        assertNormalize("tan(3)", isLiteral(-0.1425465430742778, DataTypes.DOUBLE));
        assertNormalize("tan(cast (4 as integer))", isLiteral(1.1578212823495777, DataTypes.DOUBLE));
        assertNormalize("tan(cast (5 as short))", isLiteral(-3.380515006246586, DataTypes.DOUBLE));

        // AtanFunction
        assertNormalize("atan(0.1234)", isLiteral(0.12277930094473836, DataTypes.DOUBLE));
        assertNormalize("atan(cast (0.4321 as float))", isLiteral(0.4078690066146179, DataTypes.DOUBLE));
        assertNormalize("atan(0)", isLiteral(0.0, DataTypes.DOUBLE));
        assertNormalize("atan(cast (1 as integer))", isLiteral(0.7853981633974483, DataTypes.DOUBLE));
        assertNormalize("atan(cast (-1 as short))", isLiteral(-0.7853981633974483, DataTypes.DOUBLE));
    }

    @Test
    public void testNormalizeOnNull() throws Exception {
        // SinFunction
        assertNormalize("sin(cast(null as double))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("sin(cast(null as float))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("sin(cast(null as long))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("sin(cast(null as integer))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("sin(cast(null as short))", isLiteral(null, DataTypes.DOUBLE));

        // AsinFunction
        assertNormalize("asin(cast(null as double))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("asin(cast(null as float))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("asin(cast(null as long))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("asin(cast(null as integer))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("asin(cast(null as short))", isLiteral(null, DataTypes.DOUBLE));

        // CosFunction
        assertNormalize("cos(cast(null as double))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("cos(cast(null as float))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("cos(cast(null as long))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("cos(cast(null as integer))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("cos(cast(null as short))", isLiteral(null, DataTypes.DOUBLE));

        // AcosFunction
        assertNormalize("acos(cast(null as double))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("acos(cast(null as float))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("acos(cast(null as long))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("acos(cast(null as integer))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("acos(cast(null as short))", isLiteral(null, DataTypes.DOUBLE));

        // TanFunction
        assertNormalize("tan(cast(null as double))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("tan(cast(null as float))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("tan(cast(null as long))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("tan(cast(null as integer))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("tan(cast(null as short))", isLiteral(null, DataTypes.DOUBLE));

        // AtanFunction
        assertNormalize("atan(cast(null as double))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("atan(cast(null as float))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("atan(cast(null as long))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("atan(cast(null as integer))", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("atan(cast(null as short))", isLiteral(null, DataTypes.DOUBLE));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        // SinFunction
        assertNormalize("sin(age)", isFunction(TrigonometricFunction.SinFunction.NAME));

        // AsinFunction
        assertNormalize("asin(age)", isFunction(TrigonometricFunction.AsinFunction.NAME));

        // CosFunction
        assertNormalize("cos(age)", isFunction(TrigonometricFunction.CosFunction.NAME));

        // AcosFunction
        assertNormalize("acos(age)", isFunction(TrigonometricFunction.AcosFunction.NAME));

        // TanFunction
        assertNormalize("tan(age)", isFunction(TrigonometricFunction.TanFunction.NAME));

        // AtanFunction
        assertNormalize("atan(age)", isFunction(TrigonometricFunction.AtanFunction.NAME));
    }
}
