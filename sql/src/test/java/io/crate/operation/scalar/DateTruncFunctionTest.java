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
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;

public class DateTruncFunctionTest extends AbstractScalarFunctionsTest {

    // timestamp for Do Feb 25 12:38:01.123 UTC 1999
    private static final Long TIMESTAMP = 919946281123L;

    private static final FunctionInfo functionInfo = new FunctionInfo(
            new FunctionIdent(
                    DateTruncFunction.NAME,
                    ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.TIMESTAMP)),
            DataTypes.TIMESTAMP);
    private static final DateTruncFunction func = new DateTruncFunction(functionInfo);

    private static final FunctionInfo functionInfoTZ = new FunctionInfo(
            new FunctionIdent(
                    DateTruncFunction.NAME,
                    ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.STRING, DataTypes.TIMESTAMP)),
            DataTypes.TIMESTAMP);
    private static final DateTruncFunction funcTZ = new DateTruncFunction(functionInfoTZ);

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

    public Symbol normalize(Symbol interval, Symbol timestamp) {
        Function function = new Function(func.info(), Arrays.asList(interval, timestamp));
        return func.normalizeSymbol(function);
    }

    public Symbol normalize(Symbol interval, Symbol tz, Symbol timestamp) {
        Function function = new Function(funcTZ.info(), Arrays.asList(interval, tz, timestamp));
        return funcTZ.normalizeSymbol(function);
    }

    private void assertTruncated(String interval, Long timestamp, Long expected) throws Exception {
        BytesRef ival = null;
        if (interval != null) {
            ival = new BytesRef(interval);
        }
        Scalar<Long, Object> function = func.compile(Arrays.<Symbol>asList(
                Literal.newLiteral(interval),
                TimeZoneParser.DEFAULT_TZ_LITERAL,
                Literal.newLiteral(timestamp)));
        Input[] inputs = new Input[] {
                new DateTruncInput(ival),
                new DateTruncInput(timestamp),
        };
        assertThat(function.evaluate(inputs), is(expected));
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
        Scalar<Long, Object> function = funcTZ.compile(Arrays.<Symbol>asList(
                Literal.newLiteral(interval),
                Literal.newLiteral(tz),
                Literal.newLiteral(timestamp)));
        Input[] inputs = new Input[] {
                new DateTruncInput(ival),
                new DateTruncInput(tz),
                new DateTruncInput(timestamp),
        };
        assertThat(function.evaluate(inputs), is(expected));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testDateTruncWithLongLiteral() {
        Scalar implementation = (Scalar)functions.get(new FunctionIdent(DateTruncFunction.NAME,
                Arrays.<DataType>asList(DataTypes.STRING, DataTypes.LONG)));
        assertNotNull(implementation);

        Function function = new Function(implementation.info(), Arrays.<Symbol>asList(
                Literal.newLiteral("day"),
                Literal.newLiteral(1401777485000L)
        ));
        Literal day = (Literal)implementation.normalizeSymbol(function);
        assertThat((Long) day.value(), is(1401753600000L));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDateTruncWithStringLiteral() {
        Scalar implementation = (Scalar)functions.get(new FunctionIdent(DateTruncFunction.NAME,
                Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)));
        assertNotNull(implementation);

        Function function = new Function(implementation.info(), Arrays.<Symbol>asList(
                Literal.newLiteral("day"),
                Literal.newLiteral("2014-06-03")
        ));
        Literal day = (Literal)implementation.normalizeSymbol(function);
        assertThat((Long) day.value(), is(1401753600000L));
    }

    @Test
    public void testNormalizeSymbolReferenceTimestamp() throws Exception {
        Function function = new Function(func.info(),
                Arrays.<Symbol>asList(new Reference(new ReferenceInfo(null,null, DataTypes.STRING)), new Reference(new ReferenceInfo(null,null, DataTypes.TIMESTAMP))));
        Symbol result = func.normalizeSymbol(function);
        assertSame(function, result);
    }

    @Test
    public void testNormalizeSymbolTimestampLiteral() throws Exception {
        Symbol result = normalize(
                Literal.newLiteral("day"),
                Literal.newLiteral(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value("2014-02-25T13:38:01.123")));
        assertLiteralSymbol(result, 1393286400000L, DataTypes.TIMESTAMP);
    }

    @Test
    public void testNullInterval() throws Exception {
        String val = null;
        assertThat(
                normalize(Literal.newLiteral(val), Literal.newLiteral(TIMESTAMP)),
                isLiteral(null)
        );
    }

    @Test
    public void testInvalidInterval() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid interval 'invalid interval' for scalar 'date_trunc'");
        normalize(Literal.newLiteral("invalid interval"), Literal.newLiteral(TIMESTAMP));
    }

    @Test
    public void testEvaluatePreconditions() throws Exception {
        Long expected = 919946281000L;  // Thu Feb 25 12:38:01 UTC 1999
        assertTruncated("second", null, null);
        assertTruncated("second", TIMESTAMP, expected);
    }

    @Test
    public void testEvaluate() throws Exception {
        assertTruncated("second", TIMESTAMP, 919946281000L);     // Thu Feb 25 12:38:01.000 UTC 1999
        assertTruncated("minute", TIMESTAMP, 919946280000L);     // Thu Feb 25 12:38:00.000 UTC 1999
        assertTruncated("hour", TIMESTAMP, 919944000000L);       // Thu Feb 25 12:00:00.000 UTC 1999
        assertTruncated("day", TIMESTAMP, 919900800000L);        // Thu Feb 25 00:00:00.000 UTC 1999
        assertTruncated("week", TIMESTAMP, 919641600000L);       // Mon Feb 22 00:00:00.000 UTC 1999
        assertTruncated("month", TIMESTAMP, 917827200000L);      // Mon Feb  1 00:00:00.000 UTC 1999
        assertTruncated("year", TIMESTAMP, 915148800000L);       // Fri Jan  1 00:00:00.000 UTC 1999
        assertTruncated("quarter", TIMESTAMP, 915148800000L);    // Fri Jan  1 00:00:00.000 UTC 1999
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testDateTruncWithLongDataType() {
        Scalar implementation = (Scalar)functions.get(new FunctionIdent(DateTruncFunction.NAME,
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
    public void testDateTruncWithStringLiteralTzAware() {
        Scalar implementation = (Scalar)functions.get(new FunctionIdent(DateTruncFunction.NAME,
                Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING)));
        assertNotNull(implementation);

        Function function = new Function(implementation.info(), Arrays.<Symbol>asList(
                Literal.newLiteral("day"),
                Literal.newLiteral("Europe/Vienna"),
                Literal.newLiteral("2014-06-03")
        ));
        Literal day = (Literal)implementation.normalizeSymbol(function);
        assertThat((Long) day.value(), is(1401746400000L));
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
        Symbol result = normalize(
                Literal.newLiteral("day"),
                Literal.newLiteral("UTC"),
                Literal.newLiteral(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value("2014-02-25T13:38:01.123")));
        assertLiteralSymbol(result, 1393286400000L, DataTypes.TIMESTAMP);
    }

    @Test
    public void testNullTimezone() throws Exception {
        String tz = null;
        assertThat(
                normalize(Literal.newLiteral("day"), Literal.newLiteral(tz), Literal.newLiteral(TIMESTAMP)),
                isLiteral(null)
        );
    }

    @Test
    public void testInvalidTimeZone() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid time zone value 'no time zone'");
        normalize(Literal.newLiteral("day"), Literal.newLiteral("no time zone"), Literal.newLiteral(TIMESTAMP));
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

    @Test
    public void testCompileInputColumns() throws Exception {
        Scalar implementation = (Scalar)functions.get(new FunctionIdent(DateTruncFunction.NAME,
                Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING, DataTypes.LONG)));
        assertNotNull(implementation);

        List<Symbol> arguments = ImmutableList.<Symbol>of(
                new InputColumn(0, DataTypes.STRING),
                new InputColumn(1, DataTypes.STRING),
                new InputColumn(2, DataTypes.TIMESTAMP)
        );
        Scalar compiledImplementation = implementation.compile(arguments);
        assertThat(compiledImplementation, sameInstance(implementation));
    }
}
