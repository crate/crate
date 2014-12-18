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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Extract;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.joda.Joda;
import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

public class ExtractFunctions {

    private final static ImmutableList<DataType> ARGUMENT_TYPES = ImmutableList.<DataType>of(DataTypes.TIMESTAMP);
    private final static String NAME_TMPL = "extract_%s";

    public static void register(ScalarFunctionModule scalarFunctionModule) {
        scalarFunctionModule.register(new ExtractCentury());
        scalarFunctionModule.register(new ExtractYear());
        scalarFunctionModule.register(new ExtractQuarter());
        scalarFunctionModule.register(new ExtractMonth());
        scalarFunctionModule.register(new ExtractWeek());
        scalarFunctionModule.register(new ExtractDayOfMonth());
        scalarFunctionModule.register(new ExtractDayOfWeek());
        scalarFunctionModule.register(new ExtractDayOfYear());
        scalarFunctionModule.register(new ExtractHour());
        scalarFunctionModule.register(new ExtractMinute());
        scalarFunctionModule.register(new ExtractSecond());
    }

    public static FunctionInfo functionInfo(Extract.Field field) {
        switch (field) {
            case CENTURY:
                return ExtractCentury.INFO;
            case YEAR:
                return ExtractYear.INFO;
            case QUARTER:
                return ExtractQuarter.INFO;
            case MONTH:
                return ExtractMonth.INFO;
            case WEEK:
                return ExtractWeek.INFO;
            case DAY:
            case DAY_OF_MONTH:
                return ExtractDayOfMonth.INFO;
            case DAY_OF_WEEK:
            case DOW:
                return ExtractDayOfWeek.INFO;
            case DAY_OF_YEAR:
            case DOY:
                return ExtractDayOfYear.INFO;
            case HOUR:
                return ExtractHour.INFO;
            case MINUTE:
                return ExtractMinute.INFO;
            case SECOND:
                return ExtractSecond.INFO;
            case TIMEZONE_HOUR:
                break;
            case TIMEZONE_MINUTE:
                break;
        }
        throw new UnsupportedOperationException(String.format("Extract( %s from <expression>) is not supported", field));
    }

    private static FunctionInfo createFunctionInfo(Extract.Field field) {
        return new FunctionInfo(
                new FunctionIdent(String.format(NAME_TMPL, field.toString()), ARGUMENT_TYPES),
                DataTypes.INTEGER,
                FunctionInfo.Type.SCALAR
        );
    }

    private abstract static class GenericExtractFunction extends Scalar<Number, Long> {

        public abstract int evaluate(long value);

        @Override
        public Integer evaluate(Input... args) {
            assert args.length == 1 : "extract only takes one argument";
            Object value = args[0].value();
            if (value == null) {
                return null;
            }
            assert value instanceof Long : "value of argument to extract must be of type long (timestamp)";
            return evaluate((Long) value);
        }

        @Override
        public Symbol normalizeSymbol(Function symbol) {
            assert symbol.arguments().size() == 1 : "extract only takes one argument";
            Symbol arg = symbol.arguments().get(0);
            if (arg.symbolType().isValueSymbol()) {
                return Literal.newLiteral(evaluate(((Input) arg)));
            }
            return symbol;
        }
    }

    private static class ExtractCentury extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.CENTURY);
        private static final DateTimeField CENTURY = ISOChronology.getInstanceUTC().centuryOfEra();

        @Override
        public int evaluate(long value) {
            return CENTURY.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class ExtractYear extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.YEAR);
        private static final DateTimeField YEAR = ISOChronology.getInstanceUTC().year();

        @Override
        public int evaluate(long value) {
            return YEAR.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class ExtractQuarter extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.QUARTER);
        private static final DateTimeField QUARTER = Joda.QuarterOfYear.getField(ISOChronology.getInstanceUTC());

        @Override
        public int evaluate(long value) {
            return QUARTER.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class ExtractMonth extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.MONTH);
        private static final DateTimeField MONTH = ISOChronology.getInstanceUTC().monthOfYear();

        @Override
        public int evaluate(long value) {
            return MONTH.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class ExtractWeek extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.WEEK);
        private static final DateTimeField WEEK_OF_WEEK_YEAR = ISOChronology.getInstanceUTC().weekOfWeekyear();

        @Override
        public int evaluate(long value) {
            return WEEK_OF_WEEK_YEAR.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class ExtractDayOfMonth extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.DAY_OF_MONTH);
        private static final DateTimeField DAY_OF_MONTH = ISOChronology.getInstanceUTC().dayOfMonth();

        @Override
        public int evaluate(long value) {
            return DAY_OF_MONTH.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class ExtractDayOfWeek extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.DAY_OF_WEEK);
        private static final DateTimeField DAY_OF_WEEK = ISOChronology.getInstanceUTC().dayOfWeek();

        @Override
        public int evaluate(long value) {
            return DAY_OF_WEEK.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class ExtractSecond extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.SECOND);
        private static final DateTimeField SECOND_OF_MINUTE = ISOChronology.getInstanceUTC().secondOfMinute();

        @Override
        public int evaluate(long value) {
            return SECOND_OF_MINUTE.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class ExtractMinute extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.MINUTE);
        private static final DateTimeField MINUTE_OF_HOUR = ISOChronology.getInstanceUTC().minuteOfHour();

        @Override
        public int evaluate(long value) {
            return MINUTE_OF_HOUR.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class ExtractHour extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.HOUR);
        private static final DateTimeField HOUR_OF_DAY = ISOChronology.getInstanceUTC().hourOfDay();

        @Override
        public int evaluate(long value) {
            return HOUR_OF_DAY.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class ExtractDayOfYear extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.DAY_OF_YEAR);
        private static final DateTimeField DAY_OF_YEAR = ISOChronology.getInstanceUTC().dayOfYear();

        @Override
        public int evaluate(long value) {
            return DAY_OF_YEAR.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }
}
