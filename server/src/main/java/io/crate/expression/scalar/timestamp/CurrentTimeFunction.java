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
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.TimeTZ;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

public class CurrentTimeFunction extends Scalar<TimeTZ, Integer> {

    public static final String NAME = "current_time";
    private static final int MICRO_PRECISION = 6; // microseconds

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.TIMETZ.getTypeSignature()
            ),
            CurrentTimeFunction::new
        );
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.TIMETZ.getTypeSignature()
            ),
            CurrentTimeFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    private CurrentTimeFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    @SafeVarargs
    public final TimeTZ evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Integer>... args) {
        Integer precision = MICRO_PRECISION;
        if (args.length == 1) {
            precision = args[0].value();
            if (precision == null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "NULL precision not supported for %s",
                    signature.getName().name()));
            }
            if (precision < 0 || precision > MICRO_PRECISION) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH, "precision must be between [0..%d]", MICRO_PRECISION));
            }
        }

        Instant currentInstant = txnCtx.currentInstant();
        long microsSinceMidnight = ChronoUnit.MICROS.between(currentInstant.truncatedTo(ChronoUnit.DAYS), currentInstant);
        long factor = (long) Math.pow(10, MICRO_PRECISION - precision);
        return new TimeTZ((microsSinceMidnight / factor) * factor, 0);
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
