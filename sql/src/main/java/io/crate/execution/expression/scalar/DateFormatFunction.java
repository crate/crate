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

package io.crate.execution.expression.scalar;

import com.google.common.collect.ImmutableList;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TimestampType;
import org.apache.lucene.util.BytesRef;
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

    private FunctionInfo info;


    public DateFormatFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public BytesRef evaluate(Input<Object>... args) {
        BytesRef format;
        Input<?> timezoneLiteral = null;
        if (args.length == 1) {
            format = DEFAULT_FORMAT;
        } else {
            format = (BytesRef) args[0].value();
            if (format == null) {
                return null;
            }
            if (args.length == 3) {
                timezoneLiteral = args[1];
            }
        }
        Object tsValue = args[args.length - 1].value();
        if (tsValue == null) {
            return null;
        }
        Long timestamp = TimestampType.INSTANCE.value(tsValue);
        DateTimeZone timezone = DateTimeZone.UTC;
        if (timezoneLiteral != null) {
            Object timezoneValue = timezoneLiteral.value();
            if (timezoneValue == null) {
                return null;
            }
            timezone = TimeZoneParser.parseTimeZone(BytesRefs.toBytesRef(timezoneValue));
        }
        DateTime dateTime = new DateTime(timestamp, timezone);
        return TimestampFormatter.format(format, dateTime);
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
