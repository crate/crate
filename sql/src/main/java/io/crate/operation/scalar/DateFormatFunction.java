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

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TimestampType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;

public class DateFormatFunction extends Scalar<BytesRef, Object> {

    public static final String NAME = "date_format";
    public static final BytesRef DEFAULT_FORMAT = new BytesRef("%Y-%m-%dT%H:%i:%s.%fZ");

    public static void register(ScalarFunctionModule module) {
        List<DataType> supportedTimestampTypes = ImmutableList.<DataType>of(
                DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.STRING);
        for (DataType dataType : supportedTimestampTypes) {
            // without format
            module.register(new DateFormatFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.of(dataType)),
                    DataTypes.STRING)
            ));
            // with format
            module.register(new DateFormatFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.of(DataTypes.STRING, dataType)),
                    DataTypes.STRING)
            ));
            // time zone aware variant
            module.register(new DateFormatFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.of(DataTypes.STRING, DataTypes.STRING, dataType)),
                    DataTypes.STRING)
            ));
        }
    }

    private final FunctionInfo info;
    private final Optional<DateTimeZone> dateTimeZone;
    private final Optional<BytesRef> format;


    public DateFormatFunction(FunctionInfo info) {
        this(info, null, null);
    }

    private DateFormatFunction(FunctionInfo info, @Nullable DateTimeZone dateTimeZone, @Nullable BytesRef format) {
        this.info = info;
        this.dateTimeZone = Optional.fromNullable(dateTimeZone);
        this.format = Optional.fromNullable(format);
    }

    @Override
    public Scalar<BytesRef, Object> compile(List<Symbol> arguments) {
        DateTimeZone timeZone = DateTimeZone.UTC;
        BytesRef format = null;
        if (Iterables.any(arguments, Predicates.instanceOf(Input.class))) {
            Symbol firstArg = arguments.get(0);
            if (arguments.size() == 1) {
                format = DEFAULT_FORMAT;
            } else if (firstArg instanceof Input){
                format = BytesRefs.toBytesRef(((Input)firstArg).value());
            }
            if (arguments.size() == 3) {
                Symbol zoneArg = arguments.get(1);
                if (zoneArg instanceof Input) {
                    Object formatValue = ((Input)zoneArg).value();
                    if (formatValue != null) {
                        timeZone = TimeZoneParser.parseTimeZone(BytesRefs.toBytesRef(formatValue));
                    }
                }
            }
            return new DateFormatFunction(this.info, timeZone, format);
        }
        return this;
    }

    @Override
    public BytesRef evaluate(Input<Object>... args) {
        if (hasNullInputs(args)) {
            return null;
        }
        BytesRef format;
        DateTimeZone timezone;
        Input<?> timezoneLiteral = null;
        if (this.format.isPresent()) {
            format = this.format.get();
        } else {
            if (args.length == 1) {
                format = DEFAULT_FORMAT;
            } else {
                format = (BytesRef) args[0].value();
                if (args.length == 3) {
                    timezoneLiteral = args[1];
                }
            }
        }

        if (this.dateTimeZone.isPresent()) {
            timezone = this.dateTimeZone.get();
        } else {
            timezone = DateTimeZone.UTC;
            if (timezoneLiteral != null) {
                timezone = TimeZoneParser.parseTimeZone(
                        BytesRefs.toBytesRef(timezoneLiteral.value()));
            }
        }

        Object tsValue = args[args.length-1].value();
        Long timestamp = TimestampType.INSTANCE.value(tsValue);
        DateTime dateTime = new DateTime(timestamp, timezone);
        return TimestampFormatter.format(format, dateTime);
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert symbol != null;
        assert symbol.arguments().size() > 0 && symbol.arguments().size() < 4 : "invalid number of arguments";

        return evaluateIfLiterals(this, symbol);
    }
}
