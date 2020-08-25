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
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import java.math.RoundingMode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

public class CurrentTimestampFunction extends Scalar<Long, Integer> {

    public static final String NAME = "current_timestamp";
    public static final int DEFAULT_PRECISION = 3;

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.TIMESTAMPZ.getTypeSignature()
            ).withFeatures(NO_FEATURES),
            CurrentTimestampFunction::new
        );
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.TIMESTAMPZ.getTypeSignature()
            ).withFeatures(NO_FEATURES),
            CurrentTimestampFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    public CurrentTimestampFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    @SafeVarargs
    public final Long evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Integer>... args) {
        Integer precision = DEFAULT_PRECISION;
        if (args.length == 1) {
            precision = args[0].value();
            if (precision == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "NULL precision not supported for %s", NAME));
            }
        }
        return applyPrecision(ChronoUnit.MILLIS.between(Instant.EPOCH, txnCtx.currentInstant()), precision);
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
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }
}
