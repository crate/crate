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
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.StringType;
import io.crate.types.TimestampType;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.joda.time.DateTimeZone;

public class DateTruncFunction extends BaseDateTruncFunction {

    public static void register(ScalarFunctionModule module) {
        FunctionIdent timestampFunctionIdent = new FunctionIdent(NAME,
                ImmutableList.<DataType>of(StringType.INSTANCE, TimestampType.INSTANCE));
        module.register(new DateTruncFunction(
                new FunctionInfo(timestampFunctionIdent, TimestampType.INSTANCE)));
    }

    private static final DateTimeZone DEFAULT_TZ = DateTimeZone.UTC;

    public DateTruncFunction(FunctionInfo info) {
        super(info);
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert (symbol.arguments().size() == 2);

        Literal interval = (Literal) symbol.arguments().get(0);
        isValidInterval(interval, symbol);

        if (symbol.arguments().get(1).symbolType().isValueSymbol()) {
            Literal timestamp = (Literal)symbol.arguments().get(1);
            return Literal.newLiteral(
                    DataTypes.TIMESTAMP,
                    evaluate(interval, timestamp)
            );
        }
        return symbol;
    }

    @Override
    public Long evaluate(Input<Object>... args) {
        assert (args.length == 2);
        assert (args[0].value() != null);
        if (args[1].value() == null) {
            return null;
        }
        DateTimeUnit fieldParser = DATE_FIELD_PARSERS.get(args[0].value());
        assert fieldParser != null;

        return truncate(fieldParser, (Long) args[1].value(), DEFAULT_TZ);
    }

}
