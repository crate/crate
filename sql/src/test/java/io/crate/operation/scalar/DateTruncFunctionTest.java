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

public class DateTruncFunctionTest extends AbstractScalarFunctionsTest {

    // timestamp for Do Feb 25 12:38:01.123 UTC 1999
    private static final Long TIMESTAMP = 919946281123L;

    private final FunctionInfo functionInfo = new FunctionInfo(
            new FunctionIdent(
                    DateTruncFunction.NAME,
                    ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.TIMESTAMP)),
            DataTypes.TIMESTAMP);
    private final DateTruncFunction func = new DateTruncFunction(functionInfo);

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
        assertThat((Long)day.value(), is(1401753600000L));
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
        assertThat((Long)day.value(), is(1401753600000L));
    }

    @Test (expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void testDateTruncWithStringReference() {
        Scalar implementation = (Scalar)functions.get(new FunctionIdent(DateTruncFunction.NAME,
                Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)));
        assertNotNull(implementation);
        Function function = new Function(implementation.info(), Arrays.<Symbol>asList(
                Literal.newLiteral("day"),
                createReference("dummy", DataTypes.STRING)
        ));
        implementation.normalizeSymbol(function);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeSymbolUnknownInterval() throws Exception {
        normalize(Literal.newLiteral("unknown interval"), Literal.newLiteral(""));
    }

    @Test
    public void testNormalizeSymbolReferenceTimestamp() throws Exception {
        Function function = new Function(func.info(),
                Arrays.<Symbol>asList(Literal.newLiteral("day"), new Reference(new ReferenceInfo(null,null, DataTypes.TIMESTAMP))));
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

}
