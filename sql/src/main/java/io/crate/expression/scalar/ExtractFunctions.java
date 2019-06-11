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

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.format.FunctionFormatSpec;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Scalar;
import io.crate.sql.tree.Extract;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.joda.Joda;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.List;
import java.util.Locale;

import static io.crate.sql.tree.Extract.Field.DAY_OF_MONTH;
import static io.crate.sql.tree.Extract.Field.DAY_OF_WEEK;
import static io.crate.sql.tree.Extract.Field.DAY_OF_YEAR;

public class ExtractFunctions {

    private static final String NAME_PREFIX = "extract_";

    private static final String EXTRACT_EPOCH_PREFIX = "extract(epoch from ";
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

    public static void register(ScalarFunctionModule module) {
        for (var argType : List.of(DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMP)) {
            module.register(new ExtractCentury(argType));
            module.register(new ExtractYear(argType));
            module.register(new ExtractQuarter(argType));
            module.register(new ExtractMonth(argType));
            module.register(new ExtractWeek(argType));
            module.register(new ExtractDayOfMonth(argType));
            module.register(new ExtractDayOfWeek(argType));
            module.register(new ExtractDayOfYear(argType));
            module.register(new ExtractHour(argType));
            module.register(new ExtractMinute(argType));
            module.register(new ExtractSecond(argType));
            module.register(new ExtractEpoch(argType));
        }
    }

    public static String functionNameFrom(Extract.Field field) {
        switch (field) {
            case CENTURY:
            case YEAR:
            case QUARTER:
            case MONTH:
            case WEEK:
            case HOUR:
            case MINUTE:
            case SECOND:
            case EPOCH:
                return NAME_PREFIX + field.toString();
            case DAY_OF_MONTH:
            case DAY:
                return NAME_PREFIX + DAY_OF_MONTH.toString();
            case DAY_OF_WEEK:
            case DOW:
                return NAME_PREFIX + DAY_OF_WEEK.toString();
            case DAY_OF_YEAR:
            case DOY:
                return NAME_PREFIX + DAY_OF_YEAR.toString();
            case TIMEZONE_HOUR:
            case TIMEZONE_MINUTE:
            default:
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH, "Extract( %s from <expression>) is not supported", field));
        }
    }

    private abstract static class GenericExtractFunction extends Scalar<Number, Long> implements FunctionFormatSpec {

        private final FunctionInfo info;

        GenericExtractFunction(Extract.Field field, DataType argumentType) {
            info = createFunctionInfo(field, argumentType);
        }

        private static FunctionInfo createFunctionInfo(Extract.Field field, DataType argumentType) {
            final DataType returnType;
            switch (field) {
                case EPOCH:
                    returnType = DataTypes.DOUBLE;
                    break;
                default:
                    returnType = DataTypes.INTEGER;
            }
            return new FunctionInfo(
                new FunctionIdent(functionNameFrom(field), List.of(argumentType)),
                returnType,
                FunctionInfo.Type.SCALAR
            );
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        public abstract Number evaluate(long value);

        @Override
        public Number evaluate(TransactionContext txnCtx, Input... args) {
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

        static final DateTimeField CENTURY = ISOChronology.getInstanceUTC().centuryOfEra();

        ExtractCentury(DataType argumentType) {
            super(Extract.Field.CENTURY, argumentType);
        }

        @Override
        public Integer evaluate(long value) {
            return CENTURY.get(value);
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_CENTURY_PREFIX;
        }
    }

    private static class ExtractYear extends GenericExtractFunction {

        static final DateTimeField YEAR = ISOChronology.getInstanceUTC().year();

        ExtractYear(DataType argumentType) {
            super(Extract.Field.YEAR, argumentType);
        }

        @Override
        public Integer evaluate(long value) {
            return YEAR.get(value);
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_YEAR_PREFIX;
        }
    }

    private static class ExtractQuarter extends GenericExtractFunction {

        private static final DateTimeField QUARTER = Joda.QuarterOfYear.getField(ISOChronology.getInstanceUTC());

        ExtractQuarter(DataType argumentType) {
            super(Extract.Field.QUARTER, argumentType);
        }

        @Override
        public Integer evaluate(long value) {
            return QUARTER.get(value);
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_QUARTER_PREFIX;
        }
    }

    private static class ExtractMonth extends GenericExtractFunction {

        private static final DateTimeField MONTH = ISOChronology.getInstanceUTC().monthOfYear();

        ExtractMonth(DataType argumentType) {
            super(Extract.Field.MONTH, argumentType);
        }

        @Override
        public Integer evaluate(long value) {
            return MONTH.get(value);
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_MONTH_PREFIX;
        }
    }

    private static class ExtractWeek extends GenericExtractFunction {

        static final DateTimeField WEEK_OF_WEEK_YEAR = ISOChronology.getInstanceUTC().weekOfWeekyear();

        ExtractWeek(DataType argumentType) {
            super(Extract.Field.WEEK, argumentType);
        }

        @Override
        public Integer evaluate(long value) {
            return WEEK_OF_WEEK_YEAR.get(value);
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_WEEK_PREFIX;
        }
    }

    private static class ExtractDayOfMonth extends GenericExtractFunction {

        private static final DateTimeField DAY_OF_MONTH = ISOChronology.getInstanceUTC().dayOfMonth();

        ExtractDayOfMonth(DataType argumentType) {
            super(Extract.Field.DAY_OF_MONTH, argumentType);
        }

        @Override
        public Integer evaluate(long value) {
            return DAY_OF_MONTH.get(value);
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_DAY_OF_MONTH_PREFIX;
        }
    }

    private static class ExtractDayOfWeek extends GenericExtractFunction {

        private static final DateTimeField DAY_OF_WEEK = ISOChronology.getInstanceUTC().dayOfWeek();

        ExtractDayOfWeek(DataType argumentType) {
            super(Extract.Field.DAY_OF_WEEK, argumentType);
        }

        @Override
        public Integer evaluate(long value) {
            return DAY_OF_WEEK.get(value);
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_DAY_OF_WEEK_PREFIX;
        }
    }

    private static class ExtractSecond extends GenericExtractFunction {

        private static final DateTimeField SECOND_OF_MINUTE = ISOChronology.getInstanceUTC().secondOfMinute();

        ExtractSecond(DataType argumentType) {
            super(Extract.Field.SECOND, argumentType);
        }

        @Override
        public Integer evaluate(long value) {
            return SECOND_OF_MINUTE.get(value);
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_SECOND_PREFIX;
        }
    }

    private static class ExtractMinute extends GenericExtractFunction {

        private static final DateTimeField MINUTE_OF_HOUR = ISOChronology.getInstanceUTC().minuteOfHour();

        ExtractMinute(DataType argumentType) {
            super(Extract.Field.MINUTE, argumentType);
        }

        @Override
        public Integer evaluate(long value) {
            return MINUTE_OF_HOUR.get(value);
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_MINUTE_PREFIX;
        }
    }

    private static class ExtractHour extends GenericExtractFunction {

        private static final DateTimeField HOUR_OF_DAY = ISOChronology.getInstanceUTC().hourOfDay();

        ExtractHour(DataType argumentType) {
            super(Extract.Field.HOUR, argumentType);
        }

        @Override
        public Integer evaluate(long value) {
            return HOUR_OF_DAY.get(value);
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_HOUR_PREFIX;
        }
    }

    private static class ExtractDayOfYear extends GenericExtractFunction {

        private static final DateTimeField DAY_OF_YEAR = ISOChronology.getInstanceUTC().dayOfYear();

        ExtractDayOfYear(DataType argumentType) {
            super(Extract.Field.DAY_OF_YEAR, argumentType);
        }

        @Override
        public Integer evaluate(long value) {
            return DAY_OF_YEAR.get(value);
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_DAY_OF_YEAR_PREFIX;
        }
    }

    private static class ExtractEpoch extends GenericExtractFunction {

        ExtractEpoch(DataType argumentType) {
            super(Extract.Field.EPOCH, argumentType);
        }

        @Override
        public Double evaluate(long value) {
            return (double) value / 1000;
        }

        @Override
        public String beforeArgs(Function function) {
            return EXTRACT_EPOCH_PREFIX;
        }
    }
}
