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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jetbrains.annotations.Nullable;

import io.crate.common.TriConsumer;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.impl.templates.BinaryDocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.SortedNumericDocValueAggregator;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.types.DataType;
import io.crate.types.NumericStorage;
import io.crate.types.NumericType;

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
     * @param minNodeInCluster the version the oldest node in the cluster, this is useful for BWC
     * @return a new state instance or null
     */
    @Nullable
    public abstract TPartial newState(RamAccounting ramAccounting,
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
                                     Input<?>... args)
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
                                              Input<?>[] stateToRemove) {
        throw new UnsupportedOperationException("Cannot remove state from the aggregated state as the function is " +
                                                "not removable cumulative");
    }

    /**
     * @param referenceResolver A LuceneReferenceResolver to resolve references.
     * @param aggregationReferences contains a list of references of the size of the input values of the function.
     *                              If the value at the position is not a reference in the inputs the value in
     *                              the list is null.
     * @param table DocTableInfo for the underlying table which is the source of the doc-values.
     * @param optionalParams contains a list of literals of the size of the input values of the function.
     *                       If the value at the position is not a literal in the inputs the value in the list is
     *                       null.
     * @return A DocValueAggregator or null if there is no doc-value support for the given input parameters.
     */
    @Nullable
    public DocValueAggregator<?> getDocValueAggregator(LuceneReferenceResolver referenceResolver,
                                                       List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       Version shardCreatedVersion,
                                                       List<Literal<?>> optionalParams) {
        return null;
    }

    protected Reference getAggReference(List<Reference> aggregationReferences) {
        if (aggregationReferences.isEmpty()) {
            return null;
        }
        Reference reference = aggregationReferences.getFirst();
        if (reference == null) {
            return null;
        }
        if (!reference.hasDocValues() || reference.granularity() != RowGranularity.DOC) {
            return null;
        }
        return reference;
    }

    protected DocValueAggregator<?> getNumericDocValueAggregator(
        List<Reference> aggregationReferences,
        TriConsumer<RamAccounting, TPartial, BigDecimal> applyToState) {

        Reference reference = getAggReference(aggregationReferences);
        if (reference == null) {
            return null;
        }

        DataType<?> valueType = reference.valueType();
        if (valueType.id() != NumericType.ID) {
            return null;
        }

        NumericType numericType = (NumericType) valueType;
        Integer precision = numericType.numericPrecision();
        Integer scale = numericType.scale();
        if (precision == null || scale == null) {
            throw new UnsupportedOperationException(
                "NUMERIC type requires precision and scale to support aggregation");
        }
        if (precision <= NumericStorage.COMPACT_PRECISION) {
            return new SortedNumericDocValueAggregator<>(
                reference.storageIdent(),
                (ramAccounting, memoryManager, version) ->
                    newState(ramAccounting, version, memoryManager),
                (ramAccounting, values, state) -> {
                    long docValue = values.nextValue();
                    applyToState.accept(ramAccounting, state, BigDecimal.valueOf(docValue, scale));
                }
            );
        } else {
            return new BinaryDocValueAggregator<>(
                reference.storageIdent(),
                (ramAccounting, memoryManager, version) ->
                    newState(ramAccounting, version, memoryManager),
                (ramAccounting, values, state) -> {
                    BytesRef bytesRef = values.lookupOrd(values.nextOrd());
                    BigInteger bigInteger = NumericUtils.sortableBytesToBigInt(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                    applyToState.accept(ramAccounting, state, new BigDecimal(bigInteger, scale));
                }
            );
        }
    }
}
