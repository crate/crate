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

package io.crate.analyze.symbol;

import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.BooleanType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class LiteralTest extends CrateUnitTest {

    @Test
    public void testNestedArrayLiteral() throws Exception {
        for (DataType type : DataTypes.PRIMITIVE_TYPES) {
            DataType nestedType = new ArrayType(new ArrayType(type));
            Object value;

            if (type.id() == BooleanType.ID) {
                value = true;
            } else if (type.id() == DataTypes.IP.id()) {
                value = type.value("123.34.243.23");
            } else {
                value = type.value("0");
            }

            Object nestedValue = new Object[][]{
                new Object[]{value}
            };
            Literal nestedLiteral = Literal.of(nestedType, nestedValue);
            assertThat(nestedLiteral.valueType(), is(nestedType));
            assertThat(nestedLiteral.value(), is(nestedValue));
        }
    }

    @Test
    public void testCompareArrayValues() throws Exception {
        DataType intTypeArr = new ArrayType(DataTypes.INTEGER);

        Literal val1 = Literal.of(intTypeArr, new Object[] {1,2,3});
        Literal val2 = Literal.of(intTypeArr, new Object[] {4,5,6});
        assertThat(val1.equals(val2), is(false));

        val1 = Literal.of(intTypeArr, new Object[] {1,2,3});
        val2 = Literal.of(intTypeArr, new Object[] {1,2,3});
        assertThat(val1.equals(val2), is(true));

        val1 = Literal.of(intTypeArr, new Object[] {3,2,1});
        val2 = Literal.of(intTypeArr, new Object[] {1,2,3});
        assertThat(val1.equals(val2), is(false));

        val1 = Literal.of(intTypeArr, new Object[] {1,2,3,4,5});
        val2 = Literal.of(intTypeArr, new Object[] {1,2,3});
        assertThat(val1.equals(val2), is(false));
    }

    @Test
    public void testCompareNestedArrayValues() throws Exception {
        DataType intTypeNestedArr = new ArrayType(new ArrayType(DataTypes.INTEGER));

        Literal val1 = Literal.of(intTypeNestedArr, new Object[][] {new Object[] {1,2,3}});
        Literal val2 = Literal.of(intTypeNestedArr, new Object[][] {new Object[] {4,5,6}});
        assertThat(val1.equals(val2), is(false));

        val1 = Literal.of(intTypeNestedArr, new Object[][] {new Object[] {1,2,3}});
        val2 = Literal.of(intTypeNestedArr, new Object[][] {new Object[] {1,2,3}});
        assertThat(val1.equals(val2), is(true));

        val1 = Literal.of(intTypeNestedArr, new Object[][] {new Object[] {3,2,1}});
        val2 = Literal.of(intTypeNestedArr, new Object[][] {new Object[] {1,2,3}});
        assertThat(val1.equals(val2), is(false));

        val1 = Literal.of(intTypeNestedArr, new Object[][] {new Object[] {1,2,3,4,5}});
        val2 = Literal.of(intTypeNestedArr, new Object[][] {new Object[] {1,2,3}});
        assertThat(val1.equals(val2), is(false));
    }
}
