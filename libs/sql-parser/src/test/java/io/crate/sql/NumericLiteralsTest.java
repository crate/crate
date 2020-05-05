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

package io.crate.sql;

import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.BigDecimalLiteral;
import io.crate.sql.tree.IntegerLiteral;
import io.crate.sql.tree.Literal;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.ShortLiteral;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class NumericLiteralsTest {

    private static void assertExpression(String expression, Literal expected) {
        var expr = SqlParser.createExpression(expression);
        assertThat(expr, instanceOf(expected.getClass()));
        assertThat(expr, is(expected));
    }

    @Test
    public void test_short_literal() {
        assertExpression("32767", new ShortLiteral(Short.MAX_VALUE));
    }

    @Test
    public void test_integer_literal() {
        assertExpression("2147483647", new IntegerLiteral(Integer.MAX_VALUE));
    }

    @Test
    public void test_long_literal() {
        assertExpression("2147483648", new LongLiteral(Integer.MAX_VALUE + 1L));
    }

    @Test
    public void test_float_literal() {
        assertExpression("12345.123", new BigDecimalLiteral(12345.123f));

        assertExpression("123.", new BigDecimalLiteral("123."));
        assertExpression("123.0", new BigDecimalLiteral("123.0"));
        assertExpression(".5", new BigDecimalLiteral(".5"));
        assertExpression("123.5", new BigDecimalLiteral("123.5"));

        assertExpression("123E7", new BigDecimalLiteral("123E7"));
        assertExpression("123.E7", new BigDecimalLiteral("123E7"));

        assertExpression("123.0E7", new BigDecimalLiteral("123.0E7"));
        assertExpression("123E+7", new BigDecimalLiteral("123E7"));
        assertExpression("123E-7", new BigDecimalLiteral("123E-7"));

        assertExpression("123.456E7", new BigDecimalLiteral("123.456E7"));
        assertExpression("123.456E+7", new BigDecimalLiteral("123.456E7"));
        assertExpression("123.456E-7", new BigDecimalLiteral("123.456E-7"));

        assertExpression(".4E-42", new BigDecimalLiteral(".4E-42"));
    }

    @Test
    public void test_double_literal() {
        assertExpression("12345.1234", new DoubleLiteral(12345.1234d));

        assertExpression(".4E42", new DoubleLiteral(.4E42));
        assertExpression(".4E+42", new DoubleLiteral(.4E42d));
    }
}
