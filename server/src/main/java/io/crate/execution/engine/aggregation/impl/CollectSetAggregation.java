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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.UncheckedObjectType;

public class CollectSetAggregation extends AggregationFunction<Map<Object, Object>, List<Object>> {

    /**
     * Used to signal there is a value for a key in order to simulate {@link java.util.HashSet#add(Object)} semantics
     * using a plain {@link HashMap}.
     */
    private static final Object PRESENT = null;

    public static final String NAME = "collect_set";

    public static void register(Functions.Builder builder) {
        for (DataType<?> supportedType : DataTypes.PRIMITIVE_TYPES) {
            var returnType = new ArrayType<>(supportedType);
            builder.add(
                Signature.aggregate(
                        NAME,
                        supportedType.getTypeSignature(),
                        returnType.getTypeSignature())
                    .withFeature(Scalar.Feature.DETERMINISTIC),
                CollectSetAggregation::new
            );
        }
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final DataType<?> partialReturnType;
    private final DataType<Object> elementType;

    @SuppressWarnings("unchecked")
    private CollectSetAggregation(Signature signature, BoundSignature boundSignature) {
        this.elementType = (DataType<Object>) ((ArrayType<?>) boundSignature.returnType()).innerType();
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.partialReturnType = UncheckedObjectType.INSTANCE;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @Override
    public AggregationFunction<Map<Object, Long>, List<Object>> optimizeForExecutionAsWindowFunction() {
        return new RemovableCumulativeCollectSet(signature, boundSignature);
    }

    @Override
    public Map<Object, Object> iterate(RamAccounting ramAccounting,
                                       MemoryManager memoryManager,
                                       Map<Object, Object> state,
                                       Input<?>... args) throws CircuitBreakingException {
        Object value = args[0].value();
        if (value == null) {
            return state;
        }
        if (state.put(value, PRESENT) == null) {
            ramAccounting.addBytes(
                // values size + 32 bytes for entry, 4 bytes for increased capacity
                RamUsageEstimator.alignObjectSize(elementType.valueBytes(value) + 36L)
            );
        }
        return state;
    }

    @Nullable
    @Override
    public Map<Object, Object> newState(RamAccounting ramAccounting,
                                        Version indexVersionCreated,
                                        Version minNodeInCluster,
                                        MemoryManager memoryManager) {
        ramAccounting.addBytes(RamUsageEstimator.alignObjectSize(64L)); // overhead for HashMap: 32 * 0 + 16 * 4 bytes
        return new HashMap<>();
    }

    @Override
    public DataType<?> partialType() {
        return partialReturnType;
    }

    @Override
    public Map<Object, Object> reduce(RamAccounting ramAccounting,
                                      Map<Object, Object> state1,
                                      Map<Object, Object> state2) {
        for (Object newValue : state2.keySet()) {
            if (state1.put(newValue, PRESENT) == null) {
                ramAccounting.addBytes(
                    // value size + 32 bytes for entry + 4 bytes for increased capacity
                    RamUsageEstimator.alignObjectSize(elementType.valueBytes(newValue) + 36L)
                );
            }
        }
        return state1;
    }

    @Override
    public List<Object> terminatePartial(RamAccounting ramAccounting, Map<Object, Object> state) {
        return new ArrayList<>(state.keySet());
    }

    @Override
    public boolean isRemovableCumulative() {
        return false;
    }

    /**
     * collect_set implementation that is removable cumulative. It tracks the number of occurrences for every key it
     * sees in order to be able to only remove a value from the aggregated state when it's occurrence count is 1.
     *
     * eg. for a window definition of CURRENT ROW -> UNBOUNDED FOLLOWING for {1, 2, 2, 2, 3 } the window frames and
     * corresponding collect_set outputs are:
     *  {1, 2, 2, 2, 3}  - [1, 2, 3]
     *  {2, 2, 2, 3}     - [2, 3]
     *  {2, 2, 3}        - [2, 3]
     *  {2, 3}           - [2, 3]
     *  {3}              - [3]
     */
    private static class RemovableCumulativeCollectSet extends AggregationFunction<Map<Object, Long>, List<Object>> {

        private final Signature signature;
        private final BoundSignature boundSignature;
        private final DataType<?> partialType;
        private final DataType<Object> elementType;

        @SuppressWarnings("unchecked")
        RemovableCumulativeCollectSet(Signature signature, BoundSignature boundSignature) {
            this.elementType = (DataType<Object>) ((ArrayType<?>) boundSignature.returnType()).innerType();
            this.signature = signature;
            this.boundSignature = boundSignature;
            this.partialType = UncheckedObjectType.INSTANCE;
        }

        @Nullable
        @Override
        public Map<Object, Long> newState(RamAccounting ramAccounting,
                                          Version indexVersionCreated,
                                          Version minNodeInCluster,
                                          MemoryManager memoryManager) {
            ramAccounting.addBytes(RamUsageEstimator.alignObjectSize(64L)); // overhead for HashMap: 32 * 0 + 16 * 4 bytes
            return new HashMap<>();
        }

        @Override
        public Map<Object, Long> iterate(RamAccounting ramAccounting,
                                         MemoryManager memoryManager, Map<Object, Long> state,
                                         Input<?>... args) throws CircuitBreakingException {
            Object value = args[0].value();
            if (value == null) {
                return state;
            }
            upsertOccurrenceForValue(state, value, 1, ramAccounting, elementType);
            return state;
        }

        private static void upsertOccurrenceForValue(final Map<Object, Long> state,
                                                     final Object value,
                                                     final long occurrenceIncrement,
                                                     final RamAccounting ramAccountingContext,
                                                     final DataType<Object> elementType) {
            state.compute(value, (k, v) -> {
                if (v == null) {
                    ramAccountingContext.addBytes(
                        // values size + 32 bytes for entry, 4 bytes for increased capacity, 8 bytes for the new array
                        // instance and 4 for the occurrence count we store
                        RamUsageEstimator.alignObjectSize(elementType.valueBytes(value) + 48L)
                    );
                    return occurrenceIncrement;
                } else {
                    return v + occurrenceIncrement;
                }
            });
        }

        @Override
        public boolean isRemovableCumulative() {
            return true;
        }

        @Override
        public Map<Object, Long> removeFromAggregatedState(RamAccounting ramAccounting,
                                                           Map<Object, Long> previousAggState,
                                                           Input<?>[]stateToRemove) {
            Object value = stateToRemove[0].value();
            if (value == null) {
                return previousAggState;
            }
            Long numTimesValueSeen = previousAggState.get(value);
            if (numTimesValueSeen == null) {
                return previousAggState;
            }
            if (numTimesValueSeen == 1) {
                previousAggState.remove(value);
                ramAccounting.addBytes(
                    // we initially accounted for values size + 32 bytes for entry, 4 bytes for increased capacity
                    // and 12 bytes for the array container and the int value it stored
                    - RamUsageEstimator.alignObjectSize(elementType.valueBytes(value) + 48L)
                );
            } else {
                previousAggState.put(value, numTimesValueSeen - 1);
            }
            return previousAggState;
        }

        @Override
        public Map<Object, Long> reduce(RamAccounting ramAccounting,
                                        Map<Object, Long> state1,
                                        Map<Object, Long> state2) {
            for (Map.Entry<Object, Long> state2Entry : state2.entrySet()) {
                upsertOccurrenceForValue(
                    state1,
                    state2Entry.getKey(),
                    state2Entry.getValue(),
                    ramAccounting,
                    elementType
                );
            }
            return state1;
        }

        @Override
        public List<Object> terminatePartial(RamAccounting ramAccounting, Map<Object, Long> state) {
            return new ArrayList<>(state.keySet());
        }

        @Override
        public DataType<?> partialType() {
            return partialType;
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public BoundSignature boundSignature() {
            return boundSignature;
        }
    }
}
