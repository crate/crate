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
package io.crate.operation.scalar;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.CoreMatchers.is;

public class DateTruncTimeZoneAwareFunctionTest extends AbstractScalarFunctionsTest {

    // timestamp for Do Feb 25 12:38:01.123 UTC 1999
    private static final Long TIMESTAMP = 919946281123L;

    private final FunctionInfo functionInfoTZ = new FunctionInfo(
            new FunctionIdent(
                    DateTruncTimeZoneAwareFunction.NAME,
                    ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.STRING, DataTypes.TIMESTAMP)),
            DataTypes.TIMESTAMP);
    private final DateTruncTimeZoneAwareFunction funcTZ = new DateTruncTimeZoneAwareFunction(functionInfoTZ);

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
        Function function = new Function(funcTZ.info(), Arrays.asList(interval, tz, timestamp));
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
    @SuppressWarnings("unchecked")
    public void testDateTruncWithLongDataType() {
        Scalar implementation = (Scalar)functions.get(new FunctionIdent(DateTruncTimeZoneAwareFunction.NAME,
                Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING, DataTypes.LONG)));
        assertNotNull(implementation);

        Object day = implementation.evaluate(
                new Input() {
                    @Override
                    public BytesRef value() {
                        return new BytesRef("day");
                    }
                },
                new Input() {
                    @Override
                    public BytesRef value() {
                        return new BytesRef("Europe/Vienna");
                    }
                },
                new Input() {
                    @Override
                    public Long value() {
                        return 1401777485000L;
                    }
                }
        );
        assertThat((Long)day, is(1401746400000L));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDateTruncWithStringLiteral() {
        Scalar implementation = (Scalar)functions.get(new FunctionIdent(DateTruncTimeZoneAwareFunction.NAME,
                Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING)));
        assertNotNull(implementation);

        Function function = new Function(implementation.info(), Arrays.<Symbol>asList(
                Literal.newLiteral("day"),
                Literal.newLiteral("Europe/Vienna"),
                Literal.newLiteral("2014-06-03")
        ));
        Literal day = (Literal)implementation.normalizeSymbol(function);
        assertThat((Long)day.value(), is(1401746400000L));
    }

    @Test (expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void testDateTruncWithStringReference() {
        Scalar implementation = (Scalar)functions.get(new FunctionIdent(DateTruncFunction.NAME,
                Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING)));
        assertNotNull(implementation);
        Function function = new Function(implementation.info(), Arrays.<Symbol>asList(
                Literal.newLiteral("day"),
                createReference("dummy", DataTypes.STRING)
        ));
        implementation.normalizeSymbol(function);
    }

    @Test
    public void testNormalizeSymbolTzAwareReferenceTimestamp() throws Exception {
        Function function = new Function(funcTZ.info(),
                Arrays.<Symbol>asList(Literal.newLiteral("day"), Literal.newLiteral("+01:00"),
                        new Reference(new ReferenceInfo(null,null,DataTypes.TIMESTAMP))));
        Symbol result = funcTZ.normalizeSymbol(function);
        assertSame(function, result);
    }

    @Test
    public void testNormalizeSymbolTzAwareTimestampLiteral() throws Exception {
        Symbol result = normalizeTzAware(
                Literal.newLiteral("day"),
                Literal.newLiteral("UTC"),
                Literal.newLiteral(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value("2014-02-25T13:38:01.123")));
        assertLiteralSymbol(result, 1393286400000L, DataTypes.TIMESTAMP);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeSymbolTzAwareInvalidTZ() throws Exception {
        normalizeTzAware(
                Literal.newLiteral("day"),
                Literal.newLiteral("no time zone"),
                Literal.newLiteral(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value("2014-02-25T13:38:01.123"))
        );
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
