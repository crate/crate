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

package io.crate.expression.scalar;

import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import org.junit.Test;

public class ExtractFunctionsTest extends AbstractScalarFunctionsTest {

    private static final String D_2014_02_15___21_33_23 = "2014-02-15T21:33:23";

    private void assertEvaluate(String functionExpression, Object expected) {
        assertEvaluate(functionExpression, expected,
            Literal.of(DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMPZ.value(D_2014_02_15___21_33_23)));
    }

    @Test
    public void testYayNullValue() {
        assertEvaluate(
            "extract(day from timestamp)",
            null,
            Literal.of(DataTypes.TIMESTAMPZ, null));
    }

    @Test
    public void testExtractCentury() throws Exception {
        // ISO century, see http://joda-time.sourceforge.net/field.html
        assertEvaluate("extract(century from timestamp)", 20);
    }

    @Test
    public void testExtractYear() throws Exception {
        assertEvaluate("extract(year from timestamp)", 2014);
    }

    @Test
    public void testExtractQuarter() throws Exception {
        assertEvaluate("extract(quarter from timestamp)", 1);
    }

    @Test
    public void testExtractMonth() throws Exception {
        assertEvaluate("extract(month from timestamp)", 2);
    }

    @Test
    public void testExtractWeek() throws Exception {
        assertEvaluate("extract(week from timestamp)", 7);
    }

    @Test
    public void testExtractDay() throws Exception {
        assertEvaluate("extract(day from timestamp)", 15);
        assertEvaluate("extract(day_of_month from timestamp)", 15);
    }

    @Test
    public void testDayOfWeek() throws Exception {
        assertEvaluate("extract(day_of_week from timestamp)", 6);
        assertEvaluate("extract(dow from timestamp)", 6);
    }

    @Test
    public void testDayOfYear() throws Exception {
        assertEvaluate("extract(day_of_year from timestamp)", 46);
        assertEvaluate("extract(doy from timestamp)", 46);
    }

    @Test
    public void testHour() throws Exception {
        assertEvaluate("extract(hour from timestamp)", 21);
    }

    @Test
    public void testMinute() throws Exception {
        assertEvaluate("extract(minute from timestamp)", 33);
    }

    @Test
    public void testSecond() throws Exception {
        assertEvaluate("extract(second from timestamp)", 23);
    }

    @Test
    public void testExtractEpoch() {
        assertEvaluate("extract(epoch from '1970-01-01T00:00:01')", 1.0);
        assertEvaluate("extract(epoch from '1970-01-01T00:00:00.5')", 0.5);
        assertEvaluate("extract(epoch from '1970-01-01T00:00:00')", 0.0);
        assertEvaluate("extract(epoch from '1969-12-31T23:59:59')", -1.0);
        assertEvaluate("extract(epoch from timestamp)", 1392500003.0);
    }

}
