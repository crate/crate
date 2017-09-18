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
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.analyze.symbol.format.FunctionFormatSpec;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.sql.tree.Extract;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.BytesRefs;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.Locale;

public class ExtractFunctions {

    private static final ImmutableList<DataType> ARGUMENT_TYPES = ImmutableList.<DataType>of(DataTypes.TIMESTAMP);
    private static final String NAME_PREFIX = "extract_";
    public static final FunctionInfo GENERIC_INFO = new FunctionInfo(
        new FunctionIdent("_extract", ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.TIMESTAMP)), DataTypes.INTEGER);

    private static final String EXTRACT_CENTURY_PREFIX = "extract(century from ";
    private static final String EXTRACT_YEAR_PREFIX = "extract(year from ";
    private static final String EXTRACT_QUARTER_PREFIX = "extract(quarter from ";
    private static final String EXTRACT_MONTH_PREFIX = "extract(month from ";
    private static final String EXTRACT_WEEK_PREFIX = "extract(week from ";
    private static final String EXTRACT_DAY_OF_MONTH_PREFIX = "extract(day_of_month from ";
    private static final String EXTRACT_DAY_OF_WEEK_PREFIX = "extract(day_of_week from ";
    private static final String EXTRACT_SECOND_PREFIX = "extract(second from ";
    private static final String EXTRACT_MINUTE_PREFIX = "extract(minute from ";
    private static final String EXTRACT_HOUR_PREFIX = "extract(hour from ";
    private static final String EXTRACT_DAY_OF_YEAR_PREFIX = "extract(day_of_year from ";

    public static void register(ScalarFunctionModule scalarFunctionModule) {
        scalarFunctionModule.register(ExtractCentury.INSTANCE);
        scalarFunctionModule.register(ExtractYear.INSTANCE);
        scalarFunctionModule.register(ExtractQuarter.INSTANCE);
        scalarFunctionModule.register(ExtractMonth.INSTANCE);
        scalarFunctionModule.register(ExtractWeek.INSTANCE);
        scalarFunctionModule.register(ExtractDayOfMonth.INSTANCE);
        scalarFunctionModule.register(ExtractDayOfWeek.INSTANCE);
        scalarFunctionModule.register(ExtractDayOfYear.INSTANCE);
        scalarFunctionModule.register(ExtractHour.INSTANCE);
        scalarFunctionModule.register(ExtractMinute.INSTANCE);
        scalarFunctionModule.register(ExtractSecond.INSTANCE);
        scalarFunctionModule.register(ExtractFunction.INSTANCE);
    }

    static Scalar<Number, Long> getScalar(Extract.Field field) {
        switch (field) {
            case CENTURY:
                return ExtractCentury.INSTANCE;
            case YEAR:
                return ExtractYear.INSTANCE;
            case QUARTER:
                return ExtractQuarter.INSTANCE;
            case MONTH:
                return ExtractMonth.INSTANCE;
            case WEEK:
                return ExtractWeek.INSTANCE;
            case DAY:
            case DAY_OF_MONTH:
                return ExtractDayOfMonth.INSTANCE;
            case DAY_OF_WEEK:
            case DOW:
                return ExtractDayOfWeek.INSTANCE;
            case DAY_OF_YEAR:
            case DOY:
                return ExtractDayOfYear.INSTANCE;
            case HOUR:
                return ExtractHour.INSTANCE;
            case MINUTE:
                return ExtractMinute.INSTANCE;
            case SECOND:
                return ExtractSecond.INSTANCE;
            // fall through
            case TIMEZONE_HOUR:
            case TIMEZONE_MINUTE:
            default:
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH, "Extract( %s from <expression>) is not supported", field));
        }
    }

    private static FunctionInfo createFunctionInfo(Extract.Field field) {
        return new FunctionInfo(
            new FunctionIdent(NAME_PREFIX + field.toString(), ARGUMENT_TYPES),
            DataTypes.INTEGER,
            FunctionInfo.Type.SCALAR
        );
    }

    /**
     * This is a generic ExtractFunction variant: _extract(field from ts).
     * Where the field doesn't yet have a value (either because it is a parameter or because of lazy evaluation)
     *
     * As soon as the field has an actual value it will be re-written to one of the concrete extract_ variants.
     */
    private static class ExtractFunction extends Scalar<Object, Object> implements FunctionFormatSpec {

        public static final FunctionImplementation INSTANCE = new ExtractFunction();

        private ExtractFunction() {
        }

        @Override
        public FunctionInfo info() {
            return GENERIC_INFO;
        }

        @Override
        public Symbol normalizeSymbol(Function symbol, TransactionContext transactionContext) {
            Symbol arg1 = symbol.arguments().get(0);
            if (arg1.isValue()) {
                String field = ValueSymbolVisitor.STRING.process(arg1);

                Scalar<Number, Long> scalar = getScalar(Extract.Field.valueOf(field.toUpperCase(Locale.ENGLISH)));
                Function function = new Function(scalar.info(), ImmutableList.of(symbol.arguments().get(1)));
                return scalar.normalizeSymbol(function, transactionContext);
            }
            return super.normalizeSymbol(symbol, transactionContext);
        }

        @Override
        public final Object evaluate(Input[] args) {
            String field = BytesRefs.toString(args[0].value());
            Scalar<Number, Long> scalar = getScalar(Extract.Field.valueOf(field.toUpperCase(Locale.ENGLISH)));
            return scalar.evaluate(args[1]);
        }

        @Override
        public String beforeArgs(Function function) {
            return SymbolFormatter.format("extract(%s from %s", function.arguments().toArray(new Symbol[0]));
        }

        @Override
        public String afterArgs(Function function) {
            return SymbolPrinter.Strings.PAREN_CLOSE;
        }

        @Override
        public boolean formatArgs(Function function) {
            return false;
        }
    }

    private abstract static class GenericExtractFunction extends Scalar<Number, Long> implements FunctionFormatSpec {

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
        public boolean formatArgs(Function function) {
            return true;
        }

        @Override
        public String afterArgs(Function function) {
            return SymbolPrinter.Strings.PAREN_CLOSE;
        }
    }

    private static class ExtractCentury extends GenericExtractFunction {

        private static final FunctionInfo INFO = createFunctionInfo(Extract.Field.CENTURY);
        static final ExtractCentury INSTANCE = new ExtractCentury();
        static final DateTimeField CENTURY = ISOChronology.getInstanceUTC().centuryOfEra();

        private ExtractCentury() {
        }

        @Override
        public int evaluate(long value) {
            return CENTURY.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_CENTURY_PREFIX;
        }
    }

    private static class ExtractYear extends GenericExtractFunction {

        private static final FunctionInfo INFO = createFunctionInfo(Extract.Field.YEAR);
        static final DateTimeField YEAR = ISOChronology.getInstanceUTC().year();
        static final ExtractYear INSTANCE = new ExtractYear();

        @Override
        public int evaluate(long value) {
            return YEAR.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_YEAR_PREFIX;
        }
    }

    private static class ExtractQuarter extends GenericExtractFunction {

        public static final ExtractQuarter INSTANCE = new ExtractQuarter();
        private static final FunctionInfo INFO = createFunctionInfo(Extract.Field.QUARTER);
        private static final DateTimeField QUARTER = Joda.QuarterOfYear.getField(ISOChronology.getInstanceUTC());

        private ExtractQuarter() {
        }

        @Override
        public int evaluate(long value) {
            return QUARTER.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_QUARTER_PREFIX;
        }
    }

    private static class ExtractMonth extends GenericExtractFunction {

        private static final FunctionInfo INFO = createFunctionInfo(Extract.Field.MONTH);
        private static final DateTimeField MONTH = ISOChronology.getInstanceUTC().monthOfYear();
        public static final ExtractMonth INSTANCE = new ExtractMonth();

        private ExtractMonth() {
        }

        @Override
        public int evaluate(long value) {
            return MONTH.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_MONTH_PREFIX;
        }
    }

    private static class ExtractWeek extends GenericExtractFunction {

        public static final FunctionInfo INFO = createFunctionInfo(Extract.Field.WEEK);
        static final DateTimeField WEEK_OF_WEEK_YEAR = ISOChronology.getInstanceUTC().weekOfWeekyear();
        public static final ExtractWeek INSTANCE = new ExtractWeek();

        private ExtractWeek() {
        }

        @Override
        public int evaluate(long value) {
            return WEEK_OF_WEEK_YEAR.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_WEEK_PREFIX;
        }
    }

    private static class ExtractDayOfMonth extends GenericExtractFunction {

        private static final FunctionInfo INFO = createFunctionInfo(Extract.Field.DAY_OF_MONTH);
        private static final DateTimeField DAY_OF_MONTH = ISOChronology.getInstanceUTC().dayOfMonth();
        public static final ExtractDayOfMonth INSTANCE = new ExtractDayOfMonth();

        private ExtractDayOfMonth() {
        }

        @Override
        public int evaluate(long value) {
            return DAY_OF_MONTH.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_DAY_OF_MONTH_PREFIX;
        }
    }

    private static class ExtractDayOfWeek extends GenericExtractFunction {

        private static final FunctionInfo INFO = createFunctionInfo(Extract.Field.DAY_OF_WEEK);
        private static final DateTimeField DAY_OF_WEEK = ISOChronology.getInstanceUTC().dayOfWeek();
        public static final ExtractDayOfWeek INSTANCE = new ExtractDayOfWeek();

        @Override
        public int evaluate(long value) {
            return DAY_OF_WEEK.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_DAY_OF_WEEK_PREFIX;
        }
    }

    private static class ExtractSecond extends GenericExtractFunction {

        private static final FunctionInfo INFO = createFunctionInfo(Extract.Field.SECOND);
        private static final DateTimeField SECOND_OF_MINUTE = ISOChronology.getInstanceUTC().secondOfMinute();
        public static final ExtractSecond INSTANCE = new ExtractSecond();

        private ExtractSecond() {
        }

        @Override
        public int evaluate(long value) {
            return SECOND_OF_MINUTE.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_SECOND_PREFIX;
        }
    }

    private static class ExtractMinute extends GenericExtractFunction {

        private static final FunctionInfo INFO = createFunctionInfo(Extract.Field.MINUTE);
        private static final DateTimeField MINUTE_OF_HOUR = ISOChronology.getInstanceUTC().minuteOfHour();
        public static final ExtractMinute INSTANCE = new ExtractMinute();

        private ExtractMinute() {
        }

        @Override
        public int evaluate(long value) {
            return MINUTE_OF_HOUR.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_MINUTE_PREFIX;
        }
    }

    private static class ExtractHour extends GenericExtractFunction {

        private static final FunctionInfo INFO = createFunctionInfo(Extract.Field.HOUR);
        private static final DateTimeField HOUR_OF_DAY = ISOChronology.getInstanceUTC().hourOfDay();
        public static final ExtractHour INSTANCE = new ExtractHour();

        private ExtractHour() {
        }

        @Override
        public int evaluate(long value) {
            return HOUR_OF_DAY.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_HOUR_PREFIX;
        }
    }

    private static class ExtractDayOfYear extends GenericExtractFunction {

        private static final FunctionInfo INFO = createFunctionInfo(Extract.Field.DAY_OF_YEAR);
        private static final DateTimeField DAY_OF_YEAR = ISOChronology.getInstanceUTC().dayOfYear();
        private static final ExtractDayOfYear INSTANCE = new ExtractDayOfYear();

        private ExtractDayOfYear() {
        }

        @Override
        public int evaluate(long value) {
            return DAY_OF_YEAR.get(value);
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_DAY_OF_YEAR_PREFIX;
        }
    }
}
