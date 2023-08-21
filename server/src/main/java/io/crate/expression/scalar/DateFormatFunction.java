/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class DateFormatFunction extends Scalar<String, Object> {

    public static final String NAME = "date_format";
    public static final String DEFAULT_FORMAT = "%Y-%m-%dT%H:%i:%s.%fZ";

    public static void register(ScalarFunctionModule module) {
        List<DataType<?>> supportedTimestampTypes = List.of(
            DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMP, DataTypes.LONG);
        for (DataType<?> dataType : supportedTimestampTypes) {
            // without format
            module.register(
                Signature.scalar(
                    NAME,
                    dataType.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature()
                ), DateFormatFunction::new
            );

            // with format
            module.register(
                Signature.scalar(
                    NAME,
                    DataTypes.STRING.getTypeSignature(),
                    dataType.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature()
                ), DateFormatFunction::new
            );

            // time zone aware variant
            module.register(
                Signature.scalar(
                    NAME,
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    dataType.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature()
                ), DateFormatFunction::new
            );
        }
    }

    public DateFormatFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        String format;
        Input<?> timezoneLiteral = null;
        if (args.length == 1) {
            format = DEFAULT_FORMAT;
        } else {
            format = (String) args[0].value();
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
        Long timestamp = DataTypes.TIMESTAMPZ.sanitizeValue(tsValue);
        DateTimeZone timezone = DateTimeZone.UTC;
        if (timezoneLiteral != null) {
            Object timezoneValue = timezoneLiteral.value();
            if (timezoneValue == null) {
                return null;
            }
            timezone = TimeZoneParser.parseTimeZone((String) timezoneValue);
        }
        DateTime dateTime = new DateTime(timestamp, timezone);
        return TimestampFormatter.format(format, dateTime);
    }
}
