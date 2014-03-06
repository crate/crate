/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
package io.crate.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.crate.DataType;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceInfo;
import io.crate.operator.Input;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static junit.framework.Assert.assertSame;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DateTruncTimeZoneAwareFunctionTest {

    // timestamp for Do Feb 25 12:38:01.123 UTC 1999
    private static final Long TIMESTAMP = 919946281123L;

    private final FunctionInfo functionInfoTZ = new FunctionInfo(
            new FunctionIdent(
                    DateTruncTimeZoneAwareFunction.NAME,
                    ImmutableList.of(DataType.STRING, DataType.STRING, DataType.TIMESTAMP)),
            DataType.TIMESTAMP);
    private final DateTruncTimeZoneAwareFunction funcTZ = new DateTruncTimeZoneAwareFunction(functionInfoTZ);

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    protected class DateTruncInput implements Input<Object> {
        private Object o;
        public DateTruncInput(Object o) {
            this.o = o;
        }
        @Override
        public Object value() {
            return o;
        }
    }

    public Symbol normalizeTzAware(Symbol interval, Symbol tz, Symbol timestamp) {
        Function function = new Function(funcTZ.info(),
                ImmutableList.of(interval, tz, timestamp));
        return funcTZ.normalizeSymbol(function);
    }

    private void assertTruncated(String interval, String timeZone, Long timestamp, Long expected) throws Exception {
        BytesRef ival = null;
        if (interval != null) {
            ival = new BytesRef(interval);
        }
        BytesRef tz = null;
        if (timeZone != null) {
            tz = new BytesRef(timeZone);
        }
        Input[] inputs = new Input[] {
                new DateTruncInput(ival),
                new DateTruncInput(tz),
                new DateTruncInput(timestamp),
        };
        assertThat(funcTZ.evaluate(inputs), is(expected));
    }

    @Test
    public void testNormalizeSymbolTzAwareReferenceTimestamp() throws Exception {
        Function function = new Function(funcTZ.info(),
                ImmutableList.<Symbol>of(new StringLiteral("day"), new StringLiteral("+01:00"), new Reference(new ReferenceInfo(null,null,DataType.TIMESTAMP))));
        Symbol result = funcTZ.normalizeSymbol(function);
        assertSame(function, result);
    }

    @Test
    public void testNormalizeSymbolTzAwareTimestampLiteral() throws Exception {
        Symbol result = normalizeTzAware(new StringLiteral("day"), new StringLiteral("UTC"), new TimestampLiteral("2014-02-25T13:38:01.123"));
        assertThat(result, instanceOf(TimestampLiteral.class));
        assertThat(((TimestampLiteral) result).value(), is(1393286400000L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeSymbolTzAwareInvalidTZ() throws Exception {
        normalizeTzAware(new StringLiteral("day"), new StringLiteral("no time zone"), new TimestampLiteral("2014-02-25T13:38:01.123"));
    }

    @Test(expected = AssertionError.class)
    public void testEvaluatePreconditionTzAwareNullInterval() throws Exception {
        assertTruncated(null, "+01:00", TIMESTAMP, null);
    }

    @Test(expected = AssertionError.class)
    public void testEvaluatePreconditionTzAwareNullTimezone() throws Exception {
        assertTruncated("day", null, TIMESTAMP, null);
    }

    @Test(expected = AssertionError.class)
    public void testEvaluatePreconditionTzAwareUnknownInterval() throws Exception {
        assertTruncated("unknown interval", "+01:00", TIMESTAMP, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEvaluatePreconditionTzAwareIllegalInterval() throws Exception {
        assertTruncated("day", "no time zone", TIMESTAMP, null);
    }

    @Test
    public void testEvaluatePreconditionsTzAware() throws Exception {
        Long expected = 919946281000L;  // Thu Feb 25 12:38:01 UTC 1999
        assertTruncated("second", "UTC", null, null);
        assertTruncated("second", "UTC", TIMESTAMP, expected);
    }

    @Test
    public void testEvaluateTimeZoneAware() throws Exception {
        assertTruncated("hour", "Europe/Vienna", TIMESTAMP, 919944000000L); // Thu Feb 25 12:00:00.000 UTC 1999
        assertTruncated("hour", "CET", TIMESTAMP, 919944000000L);           // Thu Feb 25 12:00:00.000 UTC 1999
        assertTruncated("day", "UTC", TIMESTAMP, 919900800000L);            // Thu Feb 25 12:00:00.000 UTC 1999
        assertTruncated("day", "Europe/Moscow", TIMESTAMP, 919890000000L);  // Wed Feb 24 21:00:00.000 UTC 1999
        assertTruncated("day", "+01:00", TIMESTAMP, 919897200000L);         // Wed Feb 24 23:00:00.000 UTC 1999
        assertTruncated("day", "+03:00", TIMESTAMP, 919890000000L);         // Wed Feb 24 21:00:00.000 UTC 1999
        assertTruncated("day", "-08:00", TIMESTAMP, 919929600000L);         // Thu Feb 25 08:00:00.000 UTC 1999
    }

}
