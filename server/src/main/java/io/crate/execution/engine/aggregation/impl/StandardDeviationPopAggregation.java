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
import io.crate.execution.engine.aggregation.statistics.StandardDeviationPop;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class StandardDeviationPopAggregation extends StandardDeviationAggregation<StandardDeviationPop> {

    public static final String NAME = "stddev_pop";

    static {
        DataTypes.register(StdDevPopStateType.ID, _ -> StdDevPopStateType.INSTANCE);
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
                 StandardDeviationPopAggregation::new
            );
        }
    }

    public static class StdDevPopStateType extends StdDevStateType<StandardDeviationPop> {

        public static final StdDevPopStateType INSTANCE = new StdDevPopStateType();
        public static final int ID = 8192;

        @Override
        public int id() {
            return ID;
        }

        @Override
        public String getName() {
            return "stddev_pop_state";
        }

        @Override
        public StandardDeviationPop readValueFrom(StreamInput in) throws IOException {
            return new StandardDeviationPop(in);
        }
    }

    public StandardDeviationPopAggregation(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public DataType<?> partialType() {
        return new StdDevPopStateType();
    }

    @Nullable
    @Override
    public StandardDeviationPop newState(RamAccounting ramAccounting,
                                         Version minNodeInCluster,
                                         MemoryManager memoryManager) {
        ramAccounting.addBytes(StandardDeviationPop.fixedSize());
        return new StandardDeviationPop();
    }
}
