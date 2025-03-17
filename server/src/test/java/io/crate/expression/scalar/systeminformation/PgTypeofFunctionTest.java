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

package io.crate.expression.scalar.systeminformation;

import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class PgTypeofFunctionTest extends ScalarTestCase {

    @Test
    public void test_sample_case_with_qualified_function_name() {
        assertEvaluate("pg_catalog.pg_typeof(null)", DataTypes.UNDEFINED.getName());
    }

    @Test
    public void test_expressions() {
        assertEvaluate("pg_typeof(1 + 1::short)", DataTypes.INTEGER.getName());
    }

    @Test
    public void test_primitive_types() {
        assertEvaluate("pg_typeof(null)", DataTypes.UNDEFINED.getName());
        assertEvaluate("pg_typeof(null::bigint)", DataTypes.LONG.getName());
        assertEvaluate("pg_typeof(name)", DataTypes.STRING.getName(), Literal.of((String) null));

        assertEvaluate("pg_typeof(true)", DataTypes.BOOLEAN.getName());
        assertEvaluate("pg_typeof(is_awesome)", DataTypes.BOOLEAN.getName(), Literal.of(true));

        assertEvaluate("pg_typeof(58::\"char\")", DataTypes.BYTE.getName());
        assertEvaluate("pg_typeof(c)", DataTypes.BYTE.getName(), Literal.of(DataTypes.BYTE, (byte) 58));

        assertEvaluate("pg_typeof(10::smallint)", DataTypes.SHORT.getName());
        assertEvaluate("pg_typeof(short_val)", DataTypes.SHORT.getName(), Literal.of(DataTypes.SHORT, (short) 10));

        assertEvaluate("pg_typeof(10::integer)", DataTypes.INTEGER.getName());
        assertEvaluate("pg_typeof(a)", DataTypes.INTEGER.getName(), Literal.of(10));

        assertEvaluate("pg_typeof(8765134432441)", "bigint");
        assertEvaluate("pg_typeof(x)", DataTypes.LONG.getName(), Literal.of(8765134432441L));

        assertEvaluate("pg_typeof(42.0::real)", DataTypes.FLOAT.getName());
        assertEvaluate("pg_typeof(float_val)", DataTypes.FLOAT.getName(), Literal.of(42.0f));

        assertEvaluate("pg_typeof(42.0)", DataTypes.DOUBLE.getName());
        assertEvaluate("pg_typeof(double_val)", DataTypes.DOUBLE.getName(), Literal.of(42.0));

        assertEvaluate("pg_typeof('name')", DataTypes.STRING.getName());
        assertEvaluate("pg_typeof(name)", DataTypes.STRING.getName(), Literal.of("name"));

        assertEvaluate("pg_typeof('120.0.0.1'::ip)", DataTypes.IP.getName());
        assertEvaluate("pg_typeof(ip)", DataTypes.IP.getName(), Literal.of(DataTypes.IP, "127.0.0.1"));

        assertEvaluate("pg_typeof('1978-02-28T10:00:00+01'::timestamp with time zone)",
                       DataTypes.TIMESTAMPZ.getName());
        assertEvaluate("pg_typeof('1978-02-28T10:00:00+01'::timestamp without time zone)",
                       DataTypes.TIMESTAMP.getName());
        assertEvaluate(
            "pg_typeof(timestamp)",
            "timestamp without time zone",
            Literal.of(
                DataTypes.TIMESTAMP,
                DataTypes.TIMESTAMP.implicitCast("1978-02-28T14:30+05:30")
            )
        );
        assertEvaluate(
            "pg_typeof(timestamp_tz)",
            "timestamp with time zone",
            Literal.of(
                DataTypes.TIMESTAMPZ,
                DataTypes.TIMESTAMPZ.implicitCast("1978-02-28T14:30+05:30")
            )
        );
    }

    @Test
    public void test_geographic_types() {
        assertEvaluate("pg_typeof([1.0, 2.0]::geo_point)", DataTypes.GEO_POINT.getName());
        assertEvaluate(
            "pg_typeof(geopoint)",
            DataTypes.GEO_POINT.getName(),
            Literal.of(
                DataTypes.GEO_POINT,
                DataTypes.GEO_POINT.sanitizeValue(List.of(1.0, 2.0))
            )
        );

        assertEvaluate("pg_typeof(" +
                       "{type = 'Polygon', " +
                       "coordinates = [[[0.0, 0.0], [0.0, 0.0], [0.0, 0.0]]]}::geo_shape)",
                       DataTypes.GEO_SHAPE.getName());
        assertEvaluate("pg_typeof(geoshape)",
                       DataTypes.GEO_SHAPE.getName(),
                       (Literal<?>) sqlExpressions.asSymbol("{type = 'Polygon', coordinates = [[[0.0, 0.0], [0.0, 0.0], [0.0, 0.0]]]}::geo_shape")
        );
    }

    @Test
    public void test_compound_types() {
        assertEvaluate("pg_typeof({})", ObjectType.NAME);
        assertEvaluate("pg_typeof(obj)", ObjectType.NAME, Literal.of(Map.of()));
        assertEvaluate("pg_typeof(obj_ignored)", ObjectType.NAME, Literal.of(Map.of()));

        assertEvaluate("pg_typeof(['Hello', 'World'])", DataTypes.STRING_ARRAY.getName());
        assertEvaluate("pg_typeof([1::smallint, 2::smallint])", DataTypes.SHORT_ARRAY.getName());
        assertEvaluate("pg_typeof([1::integer, 2::integer])", DataTypes.INTEGER_ARRAY.getName());
        assertEvaluate("pg_typeof([1, 2])", DataTypes.INTEGER_ARRAY.getName());
        assertEvaluate("pg_typeof([1.0::double precision, 2.0::double precision])",
                       DataTypes.DOUBLE_ARRAY.getName());

        assertEvaluate("pg_typeof(tags)",
                       DataTypes.STRING_ARRAY.getName(),
                       Literal.of(List.of("Hello", "World"), DataTypes.STRING_ARRAY));
        assertEvaluate("pg_typeof(short_array)",
                       DataTypes.SHORT_ARRAY.getName(),
                       Literal.of(List.of((short) 1, (short) 2), DataTypes.SHORT_ARRAY));
        assertEvaluate("pg_typeof(int_array)",
                       DataTypes.INTEGER_ARRAY.getName(),
                       Literal.of(List.of(1, 2), DataTypes.INTEGER_ARRAY));
        assertEvaluate("pg_typeof(long_array)",
                       DataTypes.BIGINT_ARRAY.getName(),
                       Literal.of(List.of(1L, 2L), DataTypes.BIGINT_ARRAY));
        assertEvaluate("pg_typeof(double_array)",
                       DataTypes.DOUBLE_ARRAY.getName(),
                       Literal.of(List.of(1.0, 2.0), DataTypes.DOUBLE_ARRAY));
    }

    @Test
    public void test_subscript_expression_types_and_errors() {
        // Field exists
        assertNormalize("pg_typeof({x=1}['x'])", isLiteral(DataTypes.INTEGER.getName()));

        // Fields does not exist
        // STRICT (error based on type definition)
        assertThatThrownBy(() -> assertNormalize("pg_typeof({x=1}::object(STRICT)['y'])", isLiteral("")))
            .hasMessageContaining("The cast of `_map('x', 1)` to return type `OBJECT(STRICT)` does not contain the key `y`");

        // DYNAMIC (different error as the `::OBJECT(DYNAMIC)` cast gets normalized away due to object cast merges)
        assertThatThrownBy(() -> assertNormalize("pg_typeof({x=1}::object(DYNAMIC)['y'])", isLiteral("")))
            .hasMessageContaining("The return type `OBJECT(DYNAMIC) AS (\"x\" INTEGER)` of the expression `_map('x', 1)` does not contain the key `y`");

        // IGNORED
        assertNormalize("pg_typeof({x=1}::object(IGNORED)['y'])", isLiteral(DataTypes.UNDEFINED.getName()));

        sqlExpressions.setErrorOnUnknownObjectKey(false);

        // DYNAMIC
        assertNormalize("pg_typeof({x=1}::object(DYNAMIC)['y'])", isLiteral(DataTypes.UNDEFINED.getName()));
    }
}

