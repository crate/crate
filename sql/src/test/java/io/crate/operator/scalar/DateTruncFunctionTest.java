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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceInfo;
import io.crate.operator.Input;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.junit.Test;

import static junit.framework.Assert.assertSame;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DateTruncFunctionTest {

    // timestamp for Do Feb 25 12:38:01.123 UTC 1999
    private static final Long TIMESTAMP = 919946281123L;

    private final FunctionInfo functionInfo = new FunctionInfo(
            new FunctionIdent(
                    DateTruncFunction.NAME,
                    ImmutableList.of(DataType.STRING, DataType.TIMESTAMP)),
            DataType.TIMESTAMP);
    private final DateTruncFunction func = new DateTruncFunction(functionInfo);

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

    public Symbol normalize(Symbol interval, Symbol timestamp) {
        Function function = new Function(func.info(),
                ImmutableList.of(interval, timestamp));
        return func.normalizeSymbol(function);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeSymbolUnknownInterval() throws Exception {
        normalize(new StringLiteral("unknown interval"), new StringLiteral(""));
    }

    @Test
    public void testNormalizeSymbolReferenceTimestamp() throws Exception {
        Function function = new Function(func.info(),
                ImmutableList.<Symbol>of(new StringLiteral("day"), new Reference(new ReferenceInfo(null,null,DataType.TIMESTAMP))));
        Symbol result = func.normalizeSymbol(function);
        assertSame(function, result);
    }

    @Test
    public void testNormalizeSymbolTimestampLiteral() throws Exception {
        Symbol result = normalize(new StringLiteral("day"), new TimestampLiteral("2014-02-25T13:38:01.123"));
        assertThat(result, instanceOf(TimestampLiteral.class));
        assertThat(((TimestampLiteral)result).value(), is(1393286400000L));
    }

    @Test(expected = AssertionError.class)
    public void testEvaluatePreconditionNullInterval() throws Exception {
        assertTruncated(null, TIMESTAMP, null);
    }

    @Test(expected = AssertionError.class)
    public void testEvaluatePreconditionUnknownInterval() throws Exception {
        assertTruncated("unknown interval", TIMESTAMP, null);
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

    private void assertTruncated(String interval, Long timestamp, Long expected) throws Exception {
        BytesRef ival = null;
        if (interval != null) {
            ival = new BytesRef(interval);
        }
        Input[] inputs = new Input[] {
                new DateTruncInput(ival),
                new DateTruncInput(timestamp),
        };
        assertThat(func.evaluate(inputs), is(expected));
    }

}
