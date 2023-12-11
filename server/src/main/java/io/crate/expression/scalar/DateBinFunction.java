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

import org.joda.time.Period;

import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.role.RoleLookup;

public class DateBinFunction extends Scalar<Long, Object> {

    public static final String NAME = "date_bin";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTERVAL.getTypeSignature(),
                DataTypes.TIMESTAMPZ.getTypeSignature(), // source
                DataTypes.TIMESTAMPZ.getTypeSignature(), // origin
                DataTypes.TIMESTAMPZ.getTypeSignature()
            ).withFeatures(Scalar.DETERMINISTIC_AND_COMPARISON_REPLACEMENT),
            DateBinFunction::new);

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTERVAL.getTypeSignature(),
                DataTypes.TIMESTAMP.getTypeSignature(), // source
                DataTypes.TIMESTAMP.getTypeSignature(), // origin
                DataTypes.TIMESTAMP.getTypeSignature()
            ).withFeatures(Scalar.DETERMINISTIC_AND_COMPARISON_REPLACEMENT),
            DateBinFunction::new);
    }

    private DateBinFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Scalar<Long, Object> compile(List<Symbol> arguments, String currentUser, RoleLookup userLookup) {
        assert arguments.size() == 3 : "Invalid number of arguments";

        if (arguments.get(0) instanceof Input<?> input) {
            var value = input.value();
            if (value != null) {
                Period p = (Period) value;
                checkMonthsAndYears(p);
                long intervalInMs = p.toStandardDuration().getMillis();
                if (intervalInMs == 0) {
                    throw new IllegalArgumentException("Interval cannot be zero");
                }
                return new CompiledDateBin(signature, boundSignature, intervalInMs);
            }
        }
        return this;
    }

    @Override
    @SafeVarargs
    public final Long evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object> ... args) {
        assert args.length == 3 : "Invalid number of arguments";

        var interval = args[0].value();
        var timestamp = args[1].value();
        var origin = args[2].value();

        if (interval == null || timestamp == null || origin == null) {
            return null;
        }

        Period p = (Period) interval;
        checkMonthsAndYears(p);
        return getBinnedTimestamp(p.toStandardDuration().getMillis(), (long) timestamp, (long) origin);
    }

    /**
     * Similar to the check called in {@link Period#toStandardDuration()} anyway,
     * but do it beforehand to provide a better error message.
     */
    private static void checkMonthsAndYears(Period p) {
        if (p.getMonths() != 0 || p.getYears() != 0) {
            throw new IllegalArgumentException("Cannot use intervals containing months or years");
        }
    }

    private static long getBinnedTimestamp(long interval, long timestamp, long origin) {
        if (interval == 0) {
            throw new IllegalArgumentException("Interval cannot be zero");
        }

        /*
         in Java % operator returns negative result only if dividend is negative.
         https://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.17.3

         We need to shift timestamp by the timeline to the "earlier" direction, to the left.
         If diff is negative, remainder will be also negative (see link above), we subtract negative
         to move to right side of the bin and then subtract abs(interval) to move to beginning of the bin.
        */

        long diff = timestamp - origin;
        if (diff >= 0) {
            // diff % interval >= 0 regardless of the interval sign.
            return timestamp - diff % interval;
        } else {
            // diff % interval < 0 regardless of the interval sign.
            if (interval > 0) {
                return timestamp - diff % interval - interval;
            } else {
                return timestamp - diff % interval + interval;
            }
        }
    }

    private static class CompiledDateBin extends Scalar<Long, Object> {

        private final long intervalInMs;

        private CompiledDateBin(Signature signature, BoundSignature boundSignature, long intervalInMs) {
            super(signature, boundSignature);
            this.intervalInMs = intervalInMs;
        }

        @Override
        @SafeVarargs
        public final Long evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
            // Validation for arguments amount is done in compile.

            var timestamp = args[1].value();
            var origin = args[2].value();

            if (timestamp == null || origin == null) {
                return null;
            }

            return getBinnedTimestamp(intervalInMs, (long) timestamp, (long) origin);
        }
    }
}
