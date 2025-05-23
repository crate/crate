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

package io.crate.execution.engine.aggregation.impl;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.jetbrains.annotations.Nullable;

import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.statistics.NumericStandardDeviationSamp;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class NumericStandardDeviationSampAggregation
    extends NumericStandardDeviationAggregation<NumericStandardDeviationSamp> {

    public static final String NAME = "stddev_samp";

    static {
        DataTypes.register(NumericStdDevSampStateType.ID, _ -> NumericStdDevSampStateType.INSTANCE);
    }

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.NUMERIC.getTypeSignature())
                .returnType(DataTypes.NUMERIC.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            NumericStandardDeviationSampAggregation::new
        );
    }

    public static class NumericStdDevSampStateType extends StdDevNumericStateType<NumericStandardDeviationSamp> {

        public static final NumericStdDevSampStateType INSTANCE = new NumericStdDevSampStateType();
        public static final int ID = 8194;

        @Override
        public int id() {
            return ID;
        }

        @Override
        public String getName() {
            return "stddev_sampl_numeric_state";
        }

        @Override
        public NumericStandardDeviationSamp readValueFrom(StreamInput in) throws IOException {
            return new NumericStandardDeviationSamp(in);
        }
    }

    public NumericStandardDeviationSampAggregation(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public DataType<?> partialType() {
        return new NumericStdDevSampStateType();
    }

    @Nullable
    @Override
    public NumericStandardDeviationSamp newState(RamAccounting ramAccounting,
                                                 Version indexVersionCreated,
                                                 Version minNodeInCluster,
                                                 MemoryManager memoryManager) {
        NumericStandardDeviationSamp newState = new NumericStandardDeviationSamp();
        ramAccounting.addBytes(newState.size());
        return newState;
    }
}
