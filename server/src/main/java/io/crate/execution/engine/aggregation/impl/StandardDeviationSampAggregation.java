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
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.statistics.StandardDeviationSamp;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class StandardDeviationSampAggregation extends StandardDeviationAggregation<StandardDeviationSamp> {

    public static final String NAME = "stddev_samp";

    static {
        DataTypes.register(StdDevSampStateType.ID, _ -> StdDevSampStateType.INSTANCE);
    }

    private static final List<DataType<?>> SUPPORTED_TYPES = Lists.concat(
        DataTypes.NUMERIC_PRIMITIVE_TYPES, DataTypes.TIMESTAMPZ);

    public static void register(Functions.Builder builder) {
        for (var supportedType : SUPPORTED_TYPES) {
            builder.add(
                Signature.builder(NAME, FunctionType.AGGREGATE)
                    .argumentTypes(supportedType.getTypeSignature())
                    .returnType(DataTypes.DOUBLE.getTypeSignature())
                    .features(Scalar.Feature.DETERMINISTIC)
                    .build(),
                 StandardDeviationSampAggregation::new
            );
        }
    }

    public static class StdDevSampStateType extends StdDevStateType<StandardDeviationSamp> {

        public static final StdDevSampStateType INSTANCE = new StdDevSampStateType();
        public static final int ID = 8193;

        @Override
        public int id() {
            return ID;
        }

        @Override
        public String getName() {
            return "stddev_sampl_state";
        }

        @Override
        public StandardDeviationSamp readValueFrom(StreamInput in) throws IOException {
            return new StandardDeviationSamp(in);
        }
    }

    public StandardDeviationSampAggregation(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public DataType<?> partialType() {
        return new StdDevSampStateType();
    }

    @Override
    protected StandardDeviationSamp newVariance() {
        return new StandardDeviationSamp();
    }

    @Nullable
    @Override
    public StandardDeviationSamp newState(RamAccounting ramAccounting,
                                         Version minNodeInCluster,
                                         MemoryManager memoryManager) {
        ramAccounting.addBytes(StandardDeviationSamp.fixedSize());
        return new StandardDeviationSamp();
    }
}
