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

package io.crate.sql;


import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.IntervalLiteral;
import org.junit.Test;

import static io.crate.sql.testing.Asserts.assertThrowsMatches;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class IntervalLiteralTest {

    @Test
    public void testYear() {
        IntervalLiteral interval = (IntervalLiteral) SqlParser.createExpression("INTERVAL +'1' YEAR");
        assertThat(interval.getValue(), is("1"));
        assertThat(interval.getSign(), is(IntervalLiteral.Sign.PLUS));
        assertThat(interval.getStartField(), is(IntervalLiteral.IntervalField.YEAR));
        assertThat(interval.getEndField(), is(nullValue()));
    }

    @Test
    public void testMonth() {
        IntervalLiteral interval = (IntervalLiteral) SqlParser.createExpression("INTERVAL +'1' MONTH");
        assertThat(interval.getValue(), is("1"));
        assertThat(interval.getSign(), is(IntervalLiteral.Sign.PLUS));
        assertThat(interval.getStartField(), is(IntervalLiteral.IntervalField.MONTH));
        assertThat(interval.getEndField(), is(nullValue()));
    }

    @Test
    public void testDay() {
        IntervalLiteral interval = (IntervalLiteral) SqlParser.createExpression("INTERVAL +'1' DAY");
        assertThat(interval.getValue(), is("1"));
        assertThat(interval.getSign(), is(IntervalLiteral.Sign.PLUS));
        assertThat(interval.getStartField(), is(IntervalLiteral.IntervalField.DAY));
        assertThat(interval.getEndField(), is(nullValue()));
    }

    @Test
    public void testHour() {
        IntervalLiteral interval = (IntervalLiteral) SqlParser.createExpression("INTERVAL +'1' HOUR");
        assertThat(interval.getValue(), is("1"));
        assertThat(interval.getSign(), is(IntervalLiteral.Sign.PLUS));
        assertThat(interval.getStartField(), is(IntervalLiteral.IntervalField.HOUR));
        assertThat(interval.getEndField(), is(nullValue()));
    }

    @Test
    public void testMinute() {
        IntervalLiteral interval = (IntervalLiteral) SqlParser.createExpression("INTERVAL +'1' MINUTE");
        assertThat(interval.getValue(), is("1"));
        assertThat(interval.getSign(), is(IntervalLiteral.Sign.PLUS));
        assertThat(interval.getStartField(), is(IntervalLiteral.IntervalField.MINUTE));
        assertThat(interval.getEndField(), is(nullValue()));
    }

    @Test
    public void testSecond() {
        IntervalLiteral interval = (IntervalLiteral) SqlParser.createExpression("INTERVAL +'1' SECOND");
        assertThat(interval.getValue(), is("1"));
        assertThat(interval.getSign(), is(IntervalLiteral.Sign.PLUS));
        assertThat(interval.getStartField(), is(IntervalLiteral.IntervalField.SECOND));
        assertThat(interval.getEndField(), is(nullValue()));
    }

    @Test
    public void testNegative() {
        IntervalLiteral interval = (IntervalLiteral) SqlParser.createExpression("INTERVAL -'1' HOUR");
        assertThat(interval.getValue(), is("1"));
        assertThat(interval.getSign(), is(IntervalLiteral.Sign.MINUS));
        assertThat(interval.getStartField(), is(IntervalLiteral.IntervalField.HOUR));
        assertThat(interval.getEndField(), is(nullValue()));
    }

    @Test
    public void testTo() {
        IntervalLiteral interval = (IntervalLiteral) SqlParser.createExpression("INTERVAL '1' HOUR TO SECOND");
        assertThat(interval.getValue(), is("1"));
        assertThat(interval.getSign(), is(IntervalLiteral.Sign.PLUS));
        assertThat(interval.getStartField(), is(IntervalLiteral.IntervalField.HOUR));
        assertThat(interval.getEndField(), is(IntervalLiteral.IntervalField.SECOND));
    }

    @Test
    public void testSecondToHour() {
        assertThrowsMatches(
            () -> SqlParser.createExpression("INTERVAL '1' SECOND TO HOUR"),
            IllegalArgumentException.class,
            "Startfield must be less significant than Endfield");
    }

    @Test
    public void testSecondToYear() {
        assertThrowsMatches(
            () -> SqlParser.createExpression("INTERVAL '1' SECOND TO YEAR"),
            IllegalArgumentException.class,
            "Startfield must be less significant than Endfield");
    }

    @Test
    public void testDayToYear() {
        assertThrowsMatches(
            () -> SqlParser.createExpression("INTERVAL '1' DAY TO YEAR"),
            IllegalArgumentException.class,
            "Startfield must be less significant than Endfield");
    }
}
