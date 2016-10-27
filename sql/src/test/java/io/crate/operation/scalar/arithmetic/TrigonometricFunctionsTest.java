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
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.is;

public class TrigonometricFunctionsTest extends AbstractScalarFunctionsTest {

    private Scalar<Double, Number> getFunction(String name, DataType type) {
        return (Scalar) functions.get(new FunctionIdent(name, Arrays.asList(type)));
    }

    private Number evaluate(String name, Number number, DataType type) {
        return getFunction(name, type).evaluate((Input) Literal.of(type, number));
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testEvaluate() throws Exception {
        assertThat(evaluate("sin", 1.0, DataTypes.DOUBLE), is(0.8414709848078965));
        assertThat(evaluate("sin", 2.0F, DataTypes.FLOAT), is(0.9092974268256817));
        assertThat(evaluate("sin", 3L, DataTypes.LONG), is(0.1411200080598672));
        assertThat(evaluate("sin", 4, DataTypes.INTEGER), is(-0.7568024953079282));
        assertThat(evaluate("sin", (short) 5, DataTypes.SHORT), is(-0.9589242746631385));

        assertThat(evaluate("asin", 0.1234, DataTypes.DOUBLE), is(0.12371534584255098));
        assertThat(evaluate("asin", 0.4321F, DataTypes.FLOAT), is(0.44682008883801516));
        assertThat(evaluate("asin", 0L, DataTypes.LONG), is(0.0));
        assertThat(evaluate("asin", 1, DataTypes.INTEGER), is(1.5707963267948966));
        assertThat(evaluate("asin", (short) -1, DataTypes.SHORT), is(-1.5707963267948966));

        assertThat(evaluate("cos", 1.0, DataTypes.DOUBLE), is(0.5403023058681398));
        assertThat(evaluate("cos", 2.0F, DataTypes.FLOAT), is(-0.4161468365471424));
        assertThat(evaluate("cos", 3L, DataTypes.LONG), is(-0.9899924966004454));
        assertThat(evaluate("cos", 4, DataTypes.INTEGER), is(-0.6536436208636119));
        assertThat(evaluate("cos", (short) 5, DataTypes.SHORT), is(0.28366218546322625));

        assertThat(evaluate("acos", 0.1234, DataTypes.DOUBLE), is(1.4470809809523457));
        assertThat(evaluate("acos", 0.4321F, DataTypes.FLOAT), is(1.1239762379568814));
        assertThat(evaluate("acos", 0L, DataTypes.LONG), is(1.5707963267948966));
        assertThat(evaluate("acos", 1, DataTypes.INTEGER), is(0.0));
        assertThat(evaluate("acos", (short) -1, DataTypes.SHORT), is(3.141592653589793));

        assertThat(evaluate("tan", 1.0, DataTypes.DOUBLE), is(1.5574077246549023));
        assertThat(evaluate("tan", 2.0F, DataTypes.FLOAT), is(-2.185039863261519));
        assertThat(evaluate("tan", 3L, DataTypes.LONG), is(-0.1425465430742778));
        assertThat(evaluate("tan", 4, DataTypes.INTEGER), is(1.1578212823495777));
        assertThat(evaluate("tan", (short) 5, DataTypes.SHORT), is(-3.380515006246586));

        assertThat(evaluate("atan", 0.1234, DataTypes.DOUBLE), is(0.12277930094473836));
        assertThat(evaluate("atan", 0.4321F, DataTypes.FLOAT), is(0.4078690066146179));
        assertThat(evaluate("atan", 0L, DataTypes.LONG), is(0.0));
        assertThat(evaluate("atan", 1, DataTypes.INTEGER), is(0.7853981633974483));
        assertThat(evaluate("atan", (short) -1, DataTypes.SHORT), is(-0.7853981633974483));
    }

    @Test
    public void testEvaluateAsinOnIllegalArgument() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("input value 2.0 is out of range. Values must be in range of [-1.0, 1.0]");
        assertEvaluate("asin(2.0)", 0);
    }

    @Test
    public void testEvaluateAcosOnIllegalArgument() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("input value 2.0 is out of range. Values must be in range of [-1.0, 1.0]");
        assertEvaluate("acos(2.0)", 0);
    }

    @Test
    public void testEvaluateAtanOnIllegalArgument() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("input value 2.0 is out of range. Values must be in range of [-1.0, 1.0]");
        assertEvaluate("atan(2.0)", 0);
    }

    @Test
    public void testEvaluateOnNull() throws Exception {
        assertEvaluate("sin(null)", null);
        assertEvaluate("asin(null)", null);
        assertEvaluate("cos(null)", null);
        assertEvaluate("acos(null)", null);
        assertEvaluate("tan(null)", null);
        assertEvaluate("atan(null)", null);
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
        assertNormalize("sin(age)", isFunction("sin"));
        assertNormalize("asin(age)", isFunction("asin"));
        assertNormalize("cos(age)", isFunction("cos"));
        assertNormalize("acos(age)", isFunction("acos"));
        assertNormalize("tan(age)", isFunction("tan"));
        assertNormalize("atan(age)", isFunction("atan"));
    }
}
