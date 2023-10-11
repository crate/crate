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

package io.crate.expression.scalar.cast;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isNotSameInstance;
import static io.crate.testing.DataTypeTesting.getDataGenerator;
import static io.crate.testing.DataTypeTesting.randomType;
import static io.crate.types.DataTypes.GEO_POINT;
import static io.crate.types.DataTypes.GEO_SHAPE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.Style;
import io.crate.geo.GeoJSONUtils;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.RegclassType;
import io.crate.types.TypeSignature;

// cast is just a wrapper around  DataType.value(val) which is why here are just a few tests
public class CastFunctionTest extends ScalarTestCase {

    private static String timezone;

    @BeforeClass
    public static void beforeTestClass() {
        timezone = System.getProperty("user.timezone");
        System.setProperty("user.timezone", "UTC");
    }

    @AfterClass
    public static void afterTestClass() {
        System.setProperty("user.timezone", timezone);
    }

    @Test
    public void testNormalize() {
        assertNormalize(
            "cast(name as bigint)",
            isFunction(
                ExplicitCastFunction.NAME,
                List.of(DataTypes.STRING, DataTypes.LONG)
            )
        );
    }

    @Test
    public void testCasts() {
        assertEvaluate("cast(10.4 as string)", "10.4");
        assertEvaluateNull("cast(null as string)");
        assertEvaluate("cast(10.4 as long)", 10L);
        assertEvaluate("cast([10.2, 12.3] as array(long))", List.of(10L, 12L));
    }

    @Test
    public void test_cast_json_string_to_object() {
        Map<String, Object> expected = Map.of("x", 10);
        assertEvaluate("'{\"x\": 10}'::object", expected);
        assertEvaluate("cast(name as object)", expected, Literal.of("{\"x\": 10}"));
    }

    @Test
    public void test_cast_string_literal_text_with_length_truncates_exceeding_chars() {
        assertEvaluate("'abcde'::varchar(2)", "ab");
    }

    @Test
    public void test_str_value_to_text_array() {
        assertEvaluate("cast('{a,abc}' as array(text))", List.of("a", "abc"));
        assertEvaluate("'{a,abc}'::text[]", List.of("a", "abc"));
    }

    @Test
    public void test_object_cast_to_text_results_in_json_string() {
        assertEvaluate("cast({x=10, y=20} as text)", "{\"x\":10,\"y\":20}");
    }

    @Test
    public void testPrecedenceOfDoubleColonCastIsHigherThanArithmetic() {
        // used to result in 2.0 as the precedence was like this: ((x::double) / a)::double
        assertEvaluate("x::double / a::double", 2.5, Literal.of(5), Literal.of(2L));
    }

    @Test
    public void testCastGeoShapeToObject() {
        Map<String, Object> shape = new HashMap<>();
        shape.put("type", "LineString");
        shape.put("coordinates", new Double[][]{new Double[]{0d, 0d}, new Double[]{2d, 0d}});
        assertEvaluate("geoshape::object", shape, Literal.of(shape));
    }

    @Test
    public void testDoubleColonOperatorCast() {
        assertEvaluate("10.4::string", "10.4");
        assertEvaluate("[1, 2, 0]::array(boolean)", List.of(true, true, false));
        assertEvaluate("(1+3)/2::string", 2);
        assertEvaluate("((1+3)/2)::string", "2");
        assertEvaluate("'10'::long + 5", 15L);
        assertEvaluate("(-4)::string", "-4");
        assertEvaluate("'-4'::long", -4L);
        assertEvaluate("(-4)::string || ' apples'", "-4 apples");
        assertEvaluate("'-4'::long + 10", 6L);
    }

    @Test
    public void testFromStringLiteralCast() {
        assertEvaluate("string '10.4'", "10.4");
        assertEvaluate("string '-4' || ' apples'", "-4 apples");
        assertEvaluate("long '-4' + 10", 6L);
        assertEvaluate("int4 '1'", 1);
        assertEvaluate("timestamp with time zone '2017-01-01T00:00:00'", 1483228800000L);
    }

    @Test
    public void testCastBetweenTimestampDataTypesOnLiterals() {
        long expected = 978310861000L;
        assertEvaluate("'2001-01-01T01:01:01+01'::timestamp without time zone", expected);
        assertEvaluate("'2001-01-01T01:01:01Z'::timestamp with time zone", expected);
    }

    @Test
    public void testCastBetweenTimestampDataTypesOnArrayLiterals() {
        assertEvaluate(
            "cast(['2001-01-01T01:01:01+01', '2001-01-01T01:01:01+10']" +
            " as array(timestamp without time zone))",
            List.of(978310861000L, 978310861000L)
        );
        assertEvaluate(
            "cast(['2001-01-01T01:01:01Z'] as array(timestamp with time zone))",
            List.of(978310861000L)
        );
    }

    @Test
    public void testCastBetweenTimestampDataTypesOnReferences() {
        assertEvaluate(
            "timestamp_tz::timestamp without time zone",
            978307261000L,
            Literal.of(
                DataTypes.TIMESTAMPZ,
                DataTypes.TIMESTAMPZ.implicitCast("2001-01-01T01:01:01+01")
            )
        );
        assertEvaluate(
            "timestamp::timestamp with time zone",
            978310861000L,
            Literal.of(
                DataTypes.TIMESTAMP,
                DataTypes.TIMESTAMP.implicitCast("2001-01-01T01:01:01Z")
            )
        );
    }

    @Test
    public void test_cast_geo_shape_array_to_object_array() {
        Map<String, Object> shape = Map.of(
            "type", "Point",
            "coordinates", new Double[]{0d, 0d});
        assertEvaluate("[geoshape]::array(object)", List.of(shape), Literal.of(shape));
    }

    @Test
    public void test_cast_wkt_point_string_array_to_geo_point_array() {
        assertEvaluate(
            "['POINT(2 3)','POINT(1 3)']::array(geo_point)",
            List.of(
                GEO_POINT.implicitCast("POINT(2 3)"),
                GEO_POINT.implicitCast("POINT(1 3)")
            )
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_cast_wkt_point_string_array_to_geo_shape_array() {
        Symbol funcSymbol = sqlExpressions.asSymbol("['POINT(2 3)']::array(geo_shape)");
        assertThat(funcSymbol.valueType(), is(new ArrayType<>(GEO_SHAPE)));
        var geoShapes = (List<Map<String, Object>>) ((Literal<?>) funcSymbol).value();
        assertThat(
            GEO_SHAPE.compare(
                geoShapes.get(0),
                Map.of(
                    GeoJSONUtils.TYPE_FIELD, GeoJSONUtils.POINT,
                    GeoJSONUtils.COORDINATES_FIELD, new Double[]{2.0, 3.0})
            ), is(0));
    }

    /**
     * Only {@link io.crate.exceptions.ConversionException} are caught on try_cast, cast will only convert
     * {@link ClassCastException} and {@link IllegalArgumentException}.
     * Ensure that this works as expected by try_cast random values for all primitive types.
     */
    @Test
    public void test_try_cast_for_all_data_types() {
        for (DataType<?> dataType : DataTypes.PRIMITIVE_TYPES) {
            DataType<?> randomType = randomType();
            Literal<?> val = Literal.ofUnchecked(randomType, getDataGenerator(randomType).get());
            assertEvaluate(
                "try_cast(" + val.toString(Style.QUALIFIED) + " as " + dataType.getName() + ")",
                f -> {});
        }
    }

    @Test
    public void test_resolve_cast_with_correct_return_type_based_on_function_argument() {
        var returnType = ObjectType.builder()
            .setInnerType("field", DataTypes.STRING)
            .build();

        var signature = Signature.scalar(
            ExplicitCastFunction.NAME,
            TypeSignature.parse("E"),
            TypeSignature.parse("V"),
            TypeSignature.parse("V")
        ).withTypeVariableConstraints(typeVariable("E"), typeVariable("V"));
        var functionImpl = sqlExpressions.nodeCtx.functions().getQualified(
            signature,
            List.of(DataTypes.UNTYPED_OBJECT, returnType),
            returnType
        );

        assertThat(functionImpl.boundSignature().returnType(), is(returnType));
    }

    @Test
    public void test_cast_numeric_to_numeric_with_changed_scale() {
        // Test that NumericType.equals() is implemented correctly otherwise the expression analyzer
        // would just skip the 2nd cast
        assertEvaluate("12.12::numeric(4, 2)::numeric(3, 1)", new BigDecimal("12.1"));
    }

    @Test
    public void test_cast_to_parametrized_numeric_returns_numeric_in_arithmetic_expression() {
        // Used to return 80.
        assertEvaluate("CAST(8.12 AS numeric(12, 5)) * 10", new BigDecimal("81.20000"));
        assertEvaluate("8.12::numeric(12, 5) * 10", new BigDecimal("81.20000"));
        //used to return "integer"
        assertEvaluate("pg_typeof(CAST(8.12 AS numeric(12, 5)) * 10)", "numeric");

        assertEvaluate("8.12::numeric * 10", new BigDecimal("81.20"));
    }

    @Test
    public void test_can_cast_object_to_json() throws Exception {
        assertEvaluate("{x = 10}::json", "{\"x\":10}");
    }

    @Test
    public void test_can_cast_json_to_object() throws Exception {
        assertEvaluate("('{\"x\":  10}'::json)::object", Map.of("x", 10));
    }

    @Test
    public void test_can_cast_text_to_json_array() throws Exception {
        assertEvaluate("'[{\"x\": 10}, {\"x\": 20}]'::json[]", List.of("{\"x\":10}", "{\"x\":20}"));
    }

    @Test
    public void test_implicit_cast_is_compiled() throws Exception {
        assertCompile("_cast(a, 'double precision')", isNotSameInstance());
    }

    @Test
    public void test_can_cast_bigint_to_regclass() {
        assertEvaluate("10::bigint::regclass",
            RegclassType.INSTANCE.explicitCast(10L, CoordinatorTxnCtx.systemTransactionContext().sessionSettings()));
    }

    @Test
    public void test_can_cast_timestamp_to_date() {
        assertEvaluate("'2020-02-09T17:50:44+0100'::timestamp::date", 1581206400000L);
        assertEvaluate("'2020-02-09T17:50:44+0100'::timestamp without time zone::date", 1581206400000L);
    }

    @Test
    public void test_can_cast_date_to_timestamp() {
        assertEvaluate("'2020-02-09T17:50:44+0100'::date::timestamp", 1581206400000L);
        assertEvaluate("'2020-02-09T17:50:44+0100'::date::timestamp without time zone", 1581206400000L);
    }

    @Test
    public void test_can_cast_jsonstring_to_object_array() throws Exception {
        assertEvaluate("'[{\"a\": 1}, {\"a\":2}]'::object[]", List.of(Map.of("a", 1), Map.of("a", 2)));
    }

    @Test
    public void test_cannot_cast_bogus_string_to_object_array() throws Exception {
        assertThatThrownBy(() -> assertEvaluate("'i-am-not-json'::object[]", null))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast value `i-am-not-json` to type `object`");
    }
}
