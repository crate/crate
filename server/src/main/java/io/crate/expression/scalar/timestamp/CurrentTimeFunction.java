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

package io.crate.expression.scalar.timestamp;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.TimeTZ;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static io.crate.expression.scalar.timestamp.TimezoneFunction.UTC;

public class CurrentTimeFunction extends Scalar<TimeTZ, Integer> {

    public static final String NAME = "current_time";
    public static final int DEFAULT_PRECISION = 6; // micro seconds
    private static final Signature SIGNATURE = Signature.scalar(
        NAME,
        DataTypes.INTEGER.getTypeSignature(),
        DataTypes.TIMETZ.getTypeSignature()
    );

    private static final FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(NAME, List.of(DataTypes.INTEGER)),
        DataTypes.TIMETZ,
        FunctionInfo.Type.SCALAR,
        Collections.emptySet());

    public static void register(ScalarFunctionModule module) {
        module.register(SIGNATURE, (signature, args) -> new CurrentTimeFunction());
    }

    @Override
    @SafeVarargs
    public final TimeTZ evaluate(TransactionContext txnCtx, Input<Integer>... args) {
        Integer precision = DEFAULT_PRECISION;
        if (args.length == 1) {
            precision = args[0].value();
            if (precision == null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH, "NULL precision not supported for %s", NAME));
            }
        }
        if (precision < 0 || precision > DEFAULT_PRECISION) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH, "precision must range between [0..%d]", DEFAULT_PRECISION));
        }
        // for now we do not mind precision, we just check it is within bounds
        long microsFromMidnight = Instant.ofEpochMilli(txnCtx.currentTimeMillis())
                                      .atZone(UTC)
                                      .getLong(ChronoField.NANO_OF_DAY) / 1000L;
        return new TimeTZ(microsFromMidnight, 0);
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Nullable
    @Override
    public Signature signature() {
        return SIGNATURE;
    }
}
