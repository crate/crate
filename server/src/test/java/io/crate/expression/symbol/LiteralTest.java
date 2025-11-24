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

package io.crate.expression.symbol;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.joda.time.Period;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Point;

import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.testing.Asserts;
import io.crate.types.ArrayType;
import io.crate.types.BooleanType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class LiteralTest extends ESTestCase {

    @Test
    public void testNestedArrayLiteral() throws Exception {
        for (DataType<?> type : DataTypes.PRIMITIVE_TYPES) {
            DataType<?> nestedType = new ArrayType<>(new ArrayType<>(type));
            Object value;

            if (type.id() == BooleanType.ID) {
                value = true;
            } else if (type.id() == DataTypes.IP.id()) {
                value = type.sanitizeValue("123.34.243.23");
            } else if (type.id() == DataTypes.INTERVAL.id()) {
                value = type.sanitizeValue(new Period().withSeconds(100));
            } else {
                value = type.implicitCast("0");
            }
            var nestedValue = List.of(List.of(value));
            Literal<?> nestedLiteral = Literal.ofUnchecked(nestedType, nestedValue);
            assertThat(nestedLiteral.valueType()).isEqualTo(nestedType);
            assertThat(nestedLiteral.value()).isEqualTo(nestedValue);
        }
    }

    @Test
    public void testHashCodeIsEqualOnArrayValues() throws Exception {
        Literal<Point> l1 = new Literal<>(
            DataTypes.GEO_POINT,
            DataTypes.GEO_POINT.implicitCast(new Double[]{10.0, 20.2})
        );
        Literal<Point> l2 = new Literal<>(
            DataTypes.GEO_POINT,
            DataTypes.GEO_POINT.implicitCast(new Double[]{10.0, 20.2})
        );
        assertThat(l1).hasSameHashCodeAs(l2);
    }

    @Test
    public void testCompareArrayValues() throws Exception {
        ArrayType<Integer> intTypeArr = new ArrayType<>(DataTypes.INTEGER);

        Literal<?> val1 = Literal.of(intTypeArr, List.of(1, 2, 3));
        Literal<?> val2 = Literal.of(intTypeArr, List.of(4,5,6));
        assertThat(val1).isNotEqualTo(val2);

        val1 = Literal.of(intTypeArr, List.of(1, 2, 3));
        val2 = Literal.of(intTypeArr, List.of(1,2,3));
        assertThat(val1).isEqualTo(val2);

        val1 = Literal.of(intTypeArr, List.of(3,2,1));
        val2 = Literal.of(intTypeArr, List.of(1,2,3));
        assertThat(val1).isNotEqualTo(val2);

        val1 = Literal.of(intTypeArr, List.of(1,2,3,4,5));
        val2 = Literal.of(intTypeArr, List.of(1,2,3));
        assertThat(val1).isNotEqualTo(val2);
    }

    @Test
    public void testCompareNestedArrayValues() throws Exception {
        ArrayType<List<Integer>> intTypeNestedArr = new ArrayType<>(new ArrayType<>(DataTypes.INTEGER));

        Literal<?> val1 = Literal.of(intTypeNestedArr, List.of(List.of(1, 2, 3)));
        Literal<?> val2 = Literal.of(intTypeNestedArr, List.of(List.of(4, 5, 6)));
        assertThat(val1).isNotEqualTo(val2);

        val1 = Literal.of(intTypeNestedArr, List.of(List.of(1, 2, 3)));
        val2 = Literal.of(intTypeNestedArr, List.of(List.of(1, 2, 3)));
        assertThat(val1).isEqualTo(val2);

        val1 = Literal.of(intTypeNestedArr, List.of(List.of(3, 2, 1)));
        val2 = Literal.of(intTypeNestedArr, List.of(List.of(1, 2, 3)));
        assertThat(val1).isNotEqualTo(val2);

        val1 = Literal.of(intTypeNestedArr, List.of(List.of(1, 2, 3, 4, 5)));
        val2 = Literal.of(intTypeNestedArr, List.of(List.of(1, 2, 3)));
        assertThat(val1).isNotEqualTo(val2);
    }

    @Test
    public void test_cast_on_literal_returns_cast_function() {
        Symbol intLiteral = Literal.of(1);
        Asserts.assertThat(intLiteral.cast(DataTypes.LONG, CastMode.IMPLICIT))
            .isFunction(ImplicitCastFunction.NAME, List.of(intLiteral.valueType()))
            .hasDataType(DataTypes.LONG);
    }
}
