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

import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DateBinFunction extends Scalar<Long, Object> {

    public static final String NAME = "date_bin";

    private final Function<Object, Long> intervalOperator;

    // First argument is a result of intervalOperator
    private final BiFunction<Long, Object, Long> timeBucketOperator;

    // Interval is same for all rows, computed once in compile()
    // in order to avoid toStandardDuration() call and object allocation per row.
    @Nullable
    private Long compiledInterval;

    private enum IntervalOperators {
        LONG(
            (Function<Object, Long>) interval -> (Long) interval
        ),
        INTERVAL(
            (Function<Object, Long>) interval -> Long.valueOf(((Period) interval).toStandardDuration().getMillis())
        );

        private final Function function;

        IntervalOperators(Function function) {
            this.function = function;
        }

        public Function getFunction() {
            return function;
        }
    }

    private enum BucketOperators {
        LONG(
            (BiFunction<Long, Object, Long>) (interval, timestamp) -> getBucketedTimestamp(interval, (long) timestamp)
        ),
        TIMESTAMP(
            (BiFunction<Long, Object, Long>) (interval, timestamp) -> getBucketedTimestamp(interval, DataTypes.TIMESTAMP.implicitCast(timestamp))
        ),
        TIMESTAMPZ(
            (BiFunction<Long, Object, Long>) (interval, timestamp) -> getBucketedTimestamp(interval, DataTypes.TIMESTAMPZ.implicitCast(timestamp))
        );


        private static long getBucketedTimestamp(long interval, long timestamp) {
            if (interval <= 0) {
                throw new IllegalArgumentException("Interval cannot be negative or equal to zero");
            }

            /*
             in Java % operator returns negative result only if dividend is negative.
             https://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.17.3

             We need to shift timestamp by the timeline to the "earlier" direction, to the left.
             But if timestamp is negative (before 1970), remainder will be also negative (see link above)
             and we need to add it as subtracting would move it to the opposite direction.
             See test_interval_timestamp_without_zone_works as example.
            */

            if (timestamp < 0) {
                return timestamp - timestamp % interval - interval;
            }
            return timestamp - timestamp % interval;
        }

        private final BiFunction function;

        BucketOperators(BiFunction function) {
            this.function = function;
        }

        public BiFunction getFunction() {
            return function;
        }
    }

    public static void register(ScalarFunctionModule module) {

        List<DataType<?>> supportedIntervalTypes = List.of(
            DataTypes.LONG, DataTypes.INTERVAL);

        List<DataType<?>> supportedTimestampTypes = List.of(
            DataTypes.TIMESTAMP, DataTypes.TIMESTAMPZ, DataTypes.LONG);

        for (DataType<?> intervalType : supportedIntervalTypes) {
            for (DataType<?> dataType : supportedTimestampTypes) {
                module.register(
                    Signature.scalar(
                        NAME,
                        intervalType.getTypeSignature(),
                        dataType.getTypeSignature(),
                        DataTypes.LONG.getTypeSignature()
                    ).withFeatures(DETERMINISTIC_ONLY),
                    DateBinFunction::new
                );
            }
        }
    }

    private final Signature signature;
    private final Signature boundSignature;

    DateBinFunction(Signature signature, Signature boundSignature) {
        this(signature, boundSignature, null);
    }

    private DateBinFunction(Signature signature, Signature boundSignature, @Nullable Long interval) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.compiledInterval = interval;

        if (signature.getArgumentDataTypes().get(0).id() == DataTypes.LONG.id()) {
            intervalOperator = IntervalOperators.LONG.getFunction();
        } else if (signature.getArgumentDataTypes().get(0).id() == DataTypes.INTERVAL.id()) {
            intervalOperator = IntervalOperators.INTERVAL.getFunction();
        } else {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "%s is not supported as interval argument type", signature.getArgumentDataTypes().get(0).getName()));
        }

        if (signature.getArgumentDataTypes().get(1).id() == DataTypes.LONG.id()) {
            timeBucketOperator = BucketOperators.LONG.getFunction();
        } else if (signature.getArgumentDataTypes().get(1).id() == DataTypes.TIMESTAMP.id()) {
            timeBucketOperator = BucketOperators.TIMESTAMP.getFunction();
        } else if (signature.getArgumentDataTypes().get(1).id() == DataTypes.TIMESTAMPZ.id()) {
            timeBucketOperator = BucketOperators.TIMESTAMPZ.getFunction();
        } else {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "%s is not supported as timestamp argument type", signature.getArgumentDataTypes().get(1).getName()));
        }

    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Scalar<Long, Object> compile(List<Symbol> arguments) {
        assert arguments.size() == 2 : "Invalid number of arguments";

        if (!arguments.get(0).symbolType().isValueSymbol()) {
            // interval is not a value, we can't compile
            return this;
        }

        Long interval = intervalOperator.apply(((Input<?>) arguments.get(0)).value());
        return new DateBinFunction(signature, boundSignature, interval);
    }

    @Override
    public final Long evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        assert args.length == 2 : "Invalid number of arguments";
        var interval = args[0].value();
        var timestamp = args[1].value();

        if (interval == null || timestamp == null) {
            return null;
        }

        if (compiledInterval != null) {
            return timeBucketOperator.apply(compiledInterval, timestamp);
        } else {
            // If all arguments are literals function gets normalized to literal (for example test run with some literal value for timestamp)
            // and compile is never called (and it's not needed as normalized version is already fast)
            return timeBucketOperator.apply(intervalOperator.apply(interval), timestamp);
        }
    }
}
