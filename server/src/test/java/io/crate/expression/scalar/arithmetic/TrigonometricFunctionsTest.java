/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar.arithmetic;

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.assertj.core.data.Offset;
import org.assertj.core.data.Percentage;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class TrigonometricFunctionsTest extends ScalarTestCase {

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("sin(double_val)", 0.8414709848078965, Literal.of(1.0));
        assertEvaluate("sin(float_val)", 0.9092974268256817, Literal.of(2.0F));
        assertEvaluate("sin(x)", 0.1411200080598672, Literal.of(3L));
        assertEvaluate("sin(id)", -0.7568024953079282, Literal.of(4));
        assertEvaluate("sin(short_val)", -0.9589242746631385, Literal.of(DataTypes.SHORT, (short) 5));

        assertEvaluate("asin(double_val)", 0.12371534584255098, Literal.of(0.1234));
        assertEvaluate("asin(float_val)", 0.44682008883801516, Literal.of(0.4321F));
        assertEvaluate("asin(x)", 0.0, Literal.of(0L));
        assertEvaluate("asin(id)", 1.5707963267948966, Literal.of(1));
        assertEvaluate("asin(short_val)", -1.5707963267948966, Literal.of(DataTypes.SHORT, (short) -1));

        assertEvaluate("cos(double_val)", 0.5403023058681398, Literal.of(1.0));
        assertEvaluate("cos(float_val)", -0.4161468365471424, Literal.of(2.0F));
        assertEvaluate("cos(x)", -0.9899924966004454, Literal.of(3L));
        assertEvaluate("cos(id)", -0.6536436208636119, Literal.of(4));
        assertEvaluate("cos(short_val)", 0.28366218546322625, Literal.of(DataTypes.SHORT, (short) 5));

        assertEvaluate("acos(double_val)", 1.4470809809523457, Literal.of(0.1234));
        assertEvaluate("acos(float_val)", 1.1239762379568814, Literal.of(0.4321F));
        assertEvaluate("acos(x)", 1.5707963267948966, Literal.of(0L));
        assertEvaluate("acos(id)", 0.0, Literal.of(1));
        assertEvaluate("acos(short_val)", 3.141592653589793, Literal.of(DataTypes.SHORT, (short) -1));

        assertEvaluate("tan(double_val)", 1.5574077246549023, Literal.of(1.0));
        assertEvaluate("tan(float_val)", -2.185039863261519, Literal.of(2.0F));
        assertEvaluate("tan(x)", -0.1425465430742778, Literal.of(3L));
        assertEvaluate("tan(id)",
                       s -> assertThat((Double) s).isCloseTo(1.1578212823495777, Offset.offset(1E-15d)),
                       Literal.of(4));
        assertEvaluate("tan(short_val)", -3.380515006246586, Literal.of(DataTypes.SHORT, (short) 5));

        assertEvaluate("atan(double_val)", 0.12277930094473836, Literal.of(0.1234));
        assertEvaluate("atan(float_val)", 0.4078690066146179, Literal.of(0.4321F));
        assertEvaluate("atan(x)", 0.0, Literal.of(0L));
        assertEvaluate("atan(id)", 0.7853981633974483, Literal.of(1));
        assertEvaluate("atan(short_val)", -0.7853981633974483, Literal.of(DataTypes.SHORT, (short) -1));
    }

    @Test
    public void test_relationships() throws Exception {
        // https://en.wikipedia.org/wiki/Inverse_trigonometric_functions#Relationships_between_trigonometric_functions_and_inverse_trigonometric_functions
        Percentage percentage = Percentage.withPercentage(99.9);
        assertEvaluate("sin(asin(0.3))", 0.3);
        assertEvaluate("sin(acos(0.3))", Math.sqrt(1 - Math.pow(0.3, 2)));
        assertEvaluate("sin(atan(0.3))",
            (Double val) -> assertThat(val).isCloseTo(0.3 / Math.sqrt(1 + Math.pow(0.3, 2)), percentage));
        assertEvaluate("cos(asin(0.3))", Math.sqrt(1 - Math.pow(0.3, 2)));
        assertEvaluate("cos(acos(0.3))", (Double val) -> assertThat(val).isCloseTo(0.3, percentage));
    }

    @Test
    public void test_atan_values_greater_and_less_than_1() throws Exception {
        assertEvaluate("tan(atan(2))", 1.9999999999999996);
        assertEvaluate("tan(atan(-1))", -0.9999999999999999);
        assertEvaluate("atan(double_val)", 1.2626272556789115, Literal.of(Math.PI));
    }

    @Test
    public void testEvaluateAsinOnIllegalArgument() {
        assertThatThrownBy(() -> assertEvaluate("asin(2.0)", 0))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("input value 2.0 is out of range. Values must be in range of [-1.0, 1.0]");
    }

    @Test
    public void testEvaluateAcosOnIllegalArgument() {
        assertThatThrownBy(() -> assertEvaluate("acos(2.0)", 0))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("input value 2.0 is out of range. Values must be in range of [-1.0, 1.0]");
    }

    @Test
    public void testEvaluateOnNull() throws Exception {
        assertEvaluateNull("sin(null)");
        assertEvaluateNull("asin(null)");
        assertEvaluateNull("cos(null)");
        assertEvaluateNull("acos(null)");
        assertEvaluateNull("tan(null)");
        assertEvaluateNull("atan(null)");
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
        assertNormalize("tan(cast (4 as integer))", isLiteral(1.1578212823495775, 1E-15));
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

        // Atan2Function
        assertNormalize("atan2(null, null)", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("atan2(1, null::double)", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("atan2(null::float, 1)", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("atan2(null::long, 1)", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("atan2(null::integer, 1)", isLiteral(null, DataTypes.DOUBLE));
        assertNormalize("atan2(null::short, 1)", isLiteral(null, DataTypes.DOUBLE));
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

    @Test
    public void test_evaluate_cot_with_primitive_numeric_argument_types() {
        assertEvaluate("cot(double_val)", -0.6166933379738136, Literal.of(2.1234));
        assertEvaluate("cot(float_val)", 0.33894928708120026, Literal.of(1.244F));
        assertEvaluate("cot(x)", -0.45765755436028577, Literal.of(2L));
        assertEvaluate("cot(id)", 0.6420926159343306, Literal.of(1));
        assertEvaluate("cot(short_val)", -7.015252551434534, Literal.of(DataTypes.SHORT, (short) 3));
    }

    @Test
    public void test_cot_with_null_argument() {
        assertEvaluateNull("cot(null)");
    }

    @Test
    public void test_cot_with_argument_value_equal_to_zero_radian() {
        assertEvaluate("cot(0)", Double.POSITIVE_INFINITY);
    }

    @Test
    public void test_normalize_cot_with_primitive_numeric_argument_types() {
        assertNormalize("cot(2.1234::double)", isLiteral(-0.6166933379738136, DataTypes.DOUBLE));
        assertNormalize("cot(1.245::real)", isLiteral(0.33783472578914325, DataTypes.DOUBLE));
        assertNormalize("cot(2::bigint)", isLiteral(-0.45765755436028577, DataTypes.DOUBLE));
        assertNormalize("cot(1::int)", isLiteral(0.6420926159343306, DataTypes.DOUBLE));
        assertNormalize("cot(3::smallint)", isLiteral(-7.015252551434534, DataTypes.DOUBLE));
    }

    @Test
    public void test_atan2() {
        assertNormalize("atan2(2, 1)", isLiteral(1.1071487177940904));
        assertNormalize("atan2(-1, -1.0)", isLiteral(-2.356194490192345));
        assertNormalize("atan2(1, 1::short)", isLiteral(0.7853981633974483));
        assertNormalize("atan2(1, 1::int)", isLiteral(0.7853981633974483));
        assertNormalize("atan2(1, 1::real)", isLiteral(0.7853981633974483));
    }
}
