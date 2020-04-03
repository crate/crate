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

import com.google.common.math.LongMath;
import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class CurrentTimestampFunction extends Scalar<Long, Integer> {

    public static final String NAME = "current_timestamp";
    public static final int DEFAULT_PRECISION = 3;

    public static final FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(NAME, List.of(DataTypes.INTEGER)),
        DataTypes.TIMESTAMPZ,
        FunctionInfo.Type.SCALAR,
        Collections.emptySet());

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.TIMESTAMPZ.getTypeSignature()
            ),
            (signature, args) -> new CurrentTimestampFunction(signature)
        );
    }

    private final Signature signature;

    public CurrentTimestampFunction(Signature signature) {
        this.signature = signature;
    }

    @Override
    @SafeVarargs
    public final Long evaluate(TransactionContext txnCtx, Input<Integer>... args) {
        long millis = txnCtx.currentTimeMillis();
        Integer precision = 3;
        if (args.length == 1) {
            precision = args[0].value();
            if (precision == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "NULL precision not supported for %s", NAME));
            }
        }
        return applyPrecision(millis, precision);
    }

    private static long applyPrecision(long millis, int precision) {
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
                return millis;
            default:
                throw new IllegalArgumentException("Precision must be between 0 and 3");
        }
        return LongMath.divide(millis, factor, RoundingMode.DOWN) * factor;
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }
}
