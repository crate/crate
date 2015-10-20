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

package io.crate.operation.scalar.timestamp;

import com.google.common.collect.ImmutableList;
import com.google.common.math.LongMath;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.joda.time.DateTimeUtils;

import java.math.RoundingMode;

public class CurrentTimestampFunction extends Scalar<Long, Integer> {

    public static final String NAME = "CURRENT_TIMESTAMP";
    public static final int DEFAULT_PRECISION = 3;

    public static final FunctionInfo INFO = new FunctionInfo(
            new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.INTEGER)),
            DataTypes.TIMESTAMP);

    public static void register(ScalarFunctionModule function) {
        function.register(new CurrentTimestampFunction());
    }


    @Override
    public Long evaluate(Input<Integer>... args) {
        long millis = DateTimeUtils.currentTimeMillis();
        if (args.length == 1) {
            Integer precision = args[0].value();
            if (precision == null) {
                throw new IllegalArgumentException(String.format("NULL precision not supported for %s", NAME));
            }
            int factor;
            switch (precision) {
                case 0:
                    factor = 1000;
                    break;
                case 1:
                    factor = 100;
                    break;
                case 2:
                    factor = 10;
                    break;
                case 3:
                    factor = 1;
                    break;
                default:
                    throw new IllegalArgumentException("Precision must be between 0 and 3");
            }
            millis = LongMath.divide(millis, factor, RoundingMode.DOWN) * factor;
        }
        return millis;
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        if (symbol.arguments().isEmpty()) {
            return Literal.newLiteral(INFO.returnType(), evaluate());
        }
        Symbol precision = symbol.arguments().get(0);
        if (precision.symbolType().isValueSymbol()) {
            return Literal.newLiteral(INFO.returnType(), evaluate((Input) precision));
        } else {
            throw new IllegalArgumentException(String.format("Invalid argument to %s", NAME));
        }

    }
}
