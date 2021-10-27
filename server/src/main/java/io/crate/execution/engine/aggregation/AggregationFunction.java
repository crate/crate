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

package io.crate.execution.engine.aggregation;

import io.crate.breaker.RamAccounting;
import io.crate.data.Input;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.types.DataType;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A special FunctionImplementation that compute a single result from a set of input values
 *
 * @param <TPartial> the intermediate type of the value used during aggregation
 * @param <TFinal>   the final type of the value after the aggregation is finished
 */
public abstract class AggregationFunction<TPartial, TFinal> implements FunctionImplementation {

    /**
     * Called once per "aggregation cycle" to create an initial partial-state-value.
     *
     * @param ramAccounting used to account the memory used for the state.
     * @param indexVersionCreated the version the current index was created on, this is useful for BWC
     * @return a new state instance or null
     */
    @Nullable
    public abstract TPartial newState(RamAccounting ramAccounting,
                                      Version indexVersionCreated,
                                      Version minNodeInCluster,
                                      MemoryManager memoryManager);

    /**
     * the "aggregate" function.
     *
     * @param ramAccounting used to account for additional memory usage if the state grows in size
     * @param state                the previous aggregation state
     * @param args                 arguments / input values matching the types of FunctionInfo.argumentTypes.
     *                             These are usually used to increment/modify the previous state
     * @return The new/changed state. This might be either a new instance or the same but mutated instance.
     * Users of the AggregationFunction should always use the return value, but must be aware that the input state might have changed too.
     */
    public abstract TPartial iterate(RamAccounting ramAccounting,
                                     MemoryManager memoryManager,
                                     TPartial state,
                                     Input... args)
        throws CircuitBreakingException;

    /**
     * This function merges two aggregation states together and returns that merged state.
     * <p>
     * This is used in a distributed aggregation workflow where 2 partial aggregations are reduced into 1 partial aggregation
     *
     * @return the reduced state. This might be a new instance or a mutated state1 or state2.
     */
    public abstract TPartial reduce(RamAccounting ramAccounting, TPartial state1, TPartial state2);

    /**
     * Called to transform partial states into their final form.
     * This might result in a loss of "meta data" that was necessary to compute the final value.
     */
    public abstract TFinal terminatePartial(RamAccounting ramAccounting, TPartial state);

    public abstract DataType<?> partialType();

    /**
     * Executing aggregations as window functions might require different runtime implementations in order to still be
     * performant. This attempts to compile a new implementation that will be optimized for the window functions
     * scenario (eg. a function might use a different execution path in order to become removable cumulative)
     */
    public AggregationFunction<?, TFinal> optimizeForExecutionAsWindowFunction() {
        return this;
    }

    /**
     * Indicates if this aggregation permits the removal of state from the previous aggregate state as defined in
     * http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
     * If a function is removable cumulative it will allow clients to remove previously aggregate values from the partial
     * state using {@link #removeFromAggregatedState(RamAccounting, Object, Input[])}
     */
    public boolean isRemovableCumulative() {
        return false;
    }

    public TPartial removeFromAggregatedState(RamAccounting ramAccounting,
                                              TPartial previousAggState,
                                              Input[] stateToRemove) {
        throw new UnsupportedOperationException("Cannot remove state from the aggregated state as the function is " +
                                                "not removable cumulative");
    }

    @Nullable
    public DocValueAggregator<?> getDocValueAggregator(List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       List<Literal<?>> optionalParams) {
        return null;
    }
}
