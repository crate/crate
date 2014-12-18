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

package io.crate.operation.scalar;

import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.planner.symbol.Literal;
import io.crate.sql.tree.Extract;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ExtractFunctionsTest {

    private Functions functions;

    String D_2014_02_15___21_33_23 = "2014-02-15T21:33:23";

    @Before
    public void setUp() throws Exception {
        functions = new ModulesBuilder().add(new ScalarFunctionModule()).createInjector().getInstance(Functions.class);
    }

    private void assertEval(Extract.Field field, String datetimeString, int expected) {
        Scalar scalar = (Scalar)functions.get(ExtractFunctions.functionInfo(field).ident());
        @SuppressWarnings("unchecked")Integer actual =
                (Integer) scalar.evaluate(Literal.newLiteral(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value(datetimeString)));
        assertThat(actual, is(expected));
    }

    @Test
    public void testYayNullValue() throws Exception {
        Scalar scalar = (Scalar)functions.get(ExtractFunctions.functionInfo(Extract.Field.DAY).ident());
        @SuppressWarnings("unchecked") Long result = (Long) scalar.evaluate(Literal.newLiteral(DataTypes.TIMESTAMP, null));
        assertThat(result, Matchers.nullValue());
    }

    @Test
    public void testExtractCentury() throws Exception {
        // ISO century, see http://joda-time.sourceforge.net/field.html
        assertEval(Extract.Field.CENTURY, D_2014_02_15___21_33_23, 20);
    }

    @Test
    public void testExtractYear() throws Exception {
        assertEval(Extract.Field.YEAR, D_2014_02_15___21_33_23, 2014);
    }

    @Test
    public void testExtractQuarter() throws Exception {
        assertEval(Extract.Field.QUARTER, D_2014_02_15___21_33_23, 1);
    }

    @Test
    public void testExtractMonth() throws Exception {
        assertEval(Extract.Field.MONTH, D_2014_02_15___21_33_23, 2);
    }

    @Test
    public void testExtractWeek() throws Exception {
        assertEval(Extract.Field.WEEK, D_2014_02_15___21_33_23, 7);
    }

    @Test
    public void testExtractDay() throws Exception {
        assertEval(Extract.Field.DAY, D_2014_02_15___21_33_23, 15);
        assertEval(Extract.Field.DAY_OF_MONTH, D_2014_02_15___21_33_23, 15);
    }

    @Test
    public void testDayOfWeek() throws Exception {
        assertEval(Extract.Field.DAY_OF_WEEK, D_2014_02_15___21_33_23, 6);
        assertEval(Extract.Field.DOW, D_2014_02_15___21_33_23, 6);
    }

    @Test
    public void testDayOfYear() throws Exception {
        assertEval(Extract.Field.DAY_OF_YEAR, D_2014_02_15___21_33_23, 46);
        assertEval(Extract.Field.DOY, D_2014_02_15___21_33_23, 46);
    }

    @Test
    public void testHour() throws Exception {
        assertEval(Extract.Field.HOUR, D_2014_02_15___21_33_23, 21);
    }

    @Test
    public void testMinute() throws Exception {
        assertEval(Extract.Field.MINUTE, D_2014_02_15___21_33_23, 33);
    }

    @Test
    public void testSecond() throws Exception {
        assertEval(Extract.Field.SECOND, D_2014_02_15___21_33_23, 23);
    }
}