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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.*;

public class DateFormatFunctionTest extends AbstractScalarFunctionsTest {

    Iterable<Literal> timestampEquivalents(String ts) {
        Literal tsLiteral = Literal.newLiteral(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value(ts));
        return ImmutableList.of(
                tsLiteral,
                Literal.convert(tsLiteral, DataTypes.LONG),
                Literal.newLiteral(ts)
        );
    }

    public Symbol normalizeForArgs(List<Symbol> args) {
        Function function = createFunction(DateFormatFunction.NAME, DataTypes.STRING, args);
        FunctionImplementation impl = functions.get(function.info().ident());
        if (randomBoolean()) {
            impl = ((Scalar)impl).compile(function.arguments());
        }

        return impl.normalizeSymbol(function);
    }
    
    public Object evaluateForArgs(List<Symbol> args) {
        Function function = createFunction(DateFormatFunction.NAME, DataTypes.STRING, args);
        Scalar impl = (Scalar)functions.get(function.info().ident());
        if (randomBoolean()) {
            impl = impl.compile(function.arguments());
        }
        Input[] inputs = new Input[args.size()];
        for (int i = 0; i < args.size(); i++) {
            inputs[i] = (Input)args.get(i);
        }
        return impl.evaluate(inputs);
    }

    @Test
    public void testNormalizeDefault() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("1970-01-01T00:00:00")) {
            List<Symbol> args = Lists.<Symbol>newArrayList(
                    tsLiteral
            );
            assertThat(
                    normalizeForArgs(args),
                    isLiteral(new BytesRef("1970-01-01T00:00:00.000000Z")));
        }
    }

    @Test
    public void testNormalizeDefaultTimezone() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("1970-02-01")) {
            List<Symbol> args = Lists.<Symbol>newArrayList(
                    Literal.newLiteral("%d.%m.%Y"),
                    tsLiteral
            );
            assertThat(
                    normalizeForArgs(args),
                    isLiteral(new BytesRef("01.02.1970")));
        }
    }

    @Test
    public void testNormalizeWithTimezone() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("1970-01-01T00:00:00")) {
            List<Symbol> args = Lists.<Symbol>newArrayList(
                    Literal.newLiteral("%d.%m.%Y %T"),
                    Literal.newLiteral("Europe/Rome"),
                    tsLiteral
            );
            assertThat(
                    normalizeForArgs(args),
                    isLiteral(new BytesRef("01.01.1970 01:00:00")));
        }
    }

    @Test
    public void testNormalizeReferenceWithTimezone() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("1970-01-01T00:00:00")) {
            List<Symbol> args = Lists.newArrayList(
                    Literal.newLiteral("%d.%m.%Y %H:%i:%S"),
                    createReference("timezone", DataTypes.STRING),
                    tsLiteral
            );
            Function function = createFunction(DateFormatFunction.NAME, DataTypes.STRING, args);

            FunctionImplementation dateFormat = functions.get(function.info().ident());
            if (randomBoolean()) {
                dateFormat = ((Scalar)dateFormat).compile(args);
            }
            Symbol normalized = dateFormat.normalizeSymbol(function);

            assertSame(function, normalized);
        }
    }

    @Test
    public void testNormalizeWithNullLiteral() throws Exception {
        Literal timestampNull = Literal.newLiteral(DataTypes.TIMESTAMP, null);
        List<List<Symbol>> argLists = Arrays.asList(
                Arrays.<Symbol>asList(
                        Literal.newLiteral("%d.%m.%Y %H:%i:%S"),
                        Literal.newLiteral(DataTypes.STRING, null),
                        timestampNull
                ),
                Arrays.<Symbol>asList(
                        Literal.newLiteral(DataTypes.STRING, null),
                        Literal.newLiteral("Europe/Berlin"),
                        Literal.newLiteral(DataTypes.TIMESTAMP, 0L)
                ),
                Arrays.<Symbol>asList(timestampNull),
                Arrays.<Symbol>asList(Literal.newLiteral(DataTypes.STRING, null), timestampNull)
        );
        for (List<Symbol> argList : argLists) {
            assertThat(normalizeForArgs(argList), isLiteral(null));
        }
    }

    @Test
    public void testEvaluateDefault() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("2015-06-10T09:03:00.004+02")) {

            List<Symbol> args = Arrays.<Symbol>asList(
                    tsLiteral
            );
            Object value = evaluateForArgs(args);
            assertThat(value, instanceOf(BytesRef.class));
            assertThat(((BytesRef)value).utf8ToString(), is("2015-06-10T07:03:00.004000Z"));
        }
    }

    @Test
    public void testEvaluateDefaultTimezone() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("2055-01-01")) {

            List<Symbol> args = Arrays.<Symbol>asList(
                    Literal.newLiteral(
                        "%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M %m %p %r " +
                        "%S %s %T %U %u %V %v %W %w %X %x %Y %y"),
                    tsLiteral
            );
            Object value = evaluateForArgs(args);
            assertThat(value, instanceOf(BytesRef.class));
            assertThat(((BytesRef)value).utf8ToString(), is(
                    "Fri Jan 1 1st 01 1 000000 00 12 12 00 001 0 12 January 01 AM 12:00:00 AM " +
                    "00 00 00:00:00 00 00 52 53 Friday 5 2054 2054 2055 55"));
        }
    }

    @Test
    public void testEvaluateWithTimezone() throws Exception {
        for (Literal tsLiteral : timestampEquivalents("1871-01-01T09:00:00.000Z")) {

            List<Symbol> args = Arrays.<Symbol>asList(
                    Literal.newLiteral(
                            "%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M %m %p %r " +
                            "%S %s %T %U %u %V %v %W %w %X %x %Y %y"),
                    Literal.newLiteral("EST"),
                    tsLiteral
            );
            Object value = evaluateForArgs(args);
            assertThat(value, instanceOf(BytesRef.class));
            assertThat(((BytesRef)value).utf8ToString(), is(
                    "Sun Jan 1 1st 01 1 000000 04 04 04 00 001 4 4 January 01 AM 04:00:00 AM " +
                    "00 00 04:00:00 01 00 01 52 Sunday 0 1871 1870 1871 71"));
        }
    }

    @Test
    public void testEvaluateWithNullInputs() throws Exception {
        Literal timestampNull = Literal.newLiteral(DataTypes.TIMESTAMP, null);
        List<List<Symbol>> argLists = Arrays.asList(
                Arrays.<Symbol>asList(
                        Literal.newLiteral("%d.%m.%Y %H:%i:%S"),
                        Literal.newLiteral(DataTypes.STRING, null),
                        timestampNull
                ),
                Arrays.<Symbol>asList(
                        Literal.newLiteral(DataTypes.STRING, null),
                        Literal.newLiteral("Europe/Berlin"),
                        Literal.newLiteral(DataTypes.TIMESTAMP, 0L)
                ),
                Arrays.<Symbol>asList(timestampNull),
                Arrays.<Symbol>asList(Literal.newLiteral(DataTypes.STRING, null), timestampNull)
        );
        for (List<Symbol> argList : argLists) {
            Object value = evaluateForArgs(argList);
            assertThat(value, is(nullValue()));
        }
    }

    @Test
    public void testNormalizeInvalidTimeZone() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid time zone value 'wrong timezone'");
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("%d.%m.%Y"),
                Literal.newLiteral("wrong timezone"),
                Literal.newLiteral(0L)
        );
        normalizeForArgs(args);
    }

    @Test
    public void testEvaluateInvalidTimeZone() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid time zone value 'wrong timezone'");
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("%d.%m.%Y"),
                Literal.newLiteral("wrong timezone"),
                Literal.newLiteral(0L)
        );
        evaluateForArgs(args);
    }

    @Test
    public void testNormalizeInvalidTimestamp() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid format: \"NO TIMESTAMP\"");
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("%d.%m.%Y"),
                Literal.newLiteral("NO TIMESTAMP")
        );
        normalizeForArgs(args);

    }

    @Test
    public void testEvaluateInvalidTimestamp() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid format: \"NO TIMESTAMP\"");
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("%d.%m.%Y"),
                Literal.newLiteral("NO TIMESTAMP")
        );
        evaluateForArgs(args);
    }

    @Test
    public void testMySQLCompatibilityForWeeks() throws Exception {
        Map<String, Map<String, String>> dateToInputOutputMap = ImmutableMap.<String, Map<String, String>>builder()
                .put("1970-01-30", ImmutableMap.<String, String>builder()
                        .put("%u", "05")
                        .put("%U", "04")
                        .put("%v", "05")
                        .put("%V", "04")
                        .put("%x", "1970")
                        .put("%X", "1970")
                        .build())
                .put("1996-01-01", ImmutableMap.of(
                        "%X %V", "1995 53",
                        "%x %v", "1996 01",
                        "%u", "01",
                        "%U", "00"
                ))
                .put("2000-01-01", ImmutableMap.<String, String>builder()
                        .put("%u", "00")
                        .put("%U", "00")
                        .put("%v", "52")
                        .put("%V", "52")
                        .put("%x", "1999")
                        .put("%X", "1999")
                        .build())
                .put("2004-01-01", ImmutableMap.<String, String>builder()
                        .put("%u", "01")
                        .put("%U", "00")
                        .put("%v", "01")
                        .put("%V", "52")
                        .put("%x", "2004")
                        .put("%X", "2003")
                        .build())
                .put("2008-02-20", ImmutableMap.<String, String>builder()
                        .put("%u", "08")
                        .put("%U", "07")
                        .put("%v", "08")
                        .put("%V", "07")
                        .put("%x", "2008")
                        .put("%X", "2008")
                        .build())
                .put("2008-12-31", ImmutableMap.<String, String>builder()
                        .put("%u", "53")
                        .put("%U", "52")
                        .put("%v", "01")
                        .put("%V", "52")
                        .put("%x", "2009")
                        .put("%X", "2008")
                        .build())
                .put("2009-01-01", ImmutableMap.<String, String>builder()
                        .put("%u", "01")
                        .put("%U", "00")
                        .put("%v", "01")
                        .put("%V", "52")
                        .put("%x", "2009")
                        .put("%X", "2008")
                        .build())
                .build();
        for (Map.Entry<String, Map<String, String>> entry : dateToInputOutputMap.entrySet()) {
            for (Map.Entry<String, String> ioEntry : entry.getValue().entrySet()) {
                List<Symbol> args = Arrays.<Symbol>asList(
                        Literal.newLiteral(ioEntry.getKey()),
                        Literal.newLiteral(entry.getKey())
                );
                Object result = evaluateForArgs(args);
                assertThat(result, instanceOf(BytesRef.class));
                assertThat(String.format("Format String '%s' returned wrong result for date '%s'", ioEntry.getKey(), entry.getKey()),
                        ((BytesRef)result).utf8ToString(),
                        is(ioEntry.getValue()));
            }
        }
    }

    @Test
    public void testEvaluateUTF8Support() throws Exception {
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("%Y®%m\uD834\uDD1E%d € %H:%i:%S"),
                Literal.newLiteral("2000-01-01")
        );
        Object result = evaluateForArgs(args);
        assertThat(result, instanceOf(BytesRef.class));
        assertThat(((BytesRef)result).utf8ToString(), is("2000®01\uD834\uDD1E01 € 00:00:00"));
    }

    @Test
    public void testInvalidFormats() throws Exception {
        List<Symbol> args = Arrays.<Symbol>asList(
                Literal.newLiteral("%t%%%Z"),
                Literal.newLiteral("2000-01-01")
        );
        Object result = evaluateForArgs(args);
        assertThat(result, instanceOf(BytesRef.class));
        assertThat(((BytesRef)result).utf8ToString(), is("t%Z"));
    }
}
