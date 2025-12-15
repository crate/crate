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

package io.crate.execution.engine.aggregation.impl.average.numeric;

import static io.crate.execution.engine.aggregation.impl.average.AverageAggregation.NAMES;
import static io.crate.execution.engine.aggregation.impl.average.numeric.NumericAverageStateType.INIT_SIZE;

import java.math.BigDecimal;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jspecify.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.util.BigDecimalValueWrapper;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.NumericType;

public class NumericAverageAggregation extends AggregationFunction<NumericAverageState<?>, BigDecimal> {

    static {
        DataTypes.register(NumericAverageStateType.ID, _ -> NumericAverageStateType.INSTANCE);
    }

    public static void register(Functions.Builder builder) {
        for (var functionName : NAMES) {
            builder.add(
                    Signature.builder(functionName, FunctionType.AGGREGATE)
                            .argumentTypes(DataTypes.NUMERIC.getTypeSignature())
                            .returnType(DataTypes.NUMERIC.getTypeSignature())
                            .features(Scalar.Feature.DETERMINISTIC)
                            .build(),
                    NumericAverageAggregation::new
            );
        }
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final DataType<BigDecimal> returnType;

    @SuppressWarnings("unchecked")
    private NumericAverageAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;

        // We want to preserve the scale and precision from the
        // numeric argument type for the return type. So we use
        // the incoming numeric type as return type instead of
        // the return type from the signature `avg(count::numeric(16, 2))`
        // should return the type `numeric(16, 2)` not `numeric`
        var argumentType = boundSignature.argTypes().get(0);
        if (argumentType instanceof NumericAverageStateType) {
            this.returnType = (DataType<BigDecimal>) boundSignature.returnType();
        } else {
            this.returnType = (DataType<BigDecimal>) argumentType;
        }
    }

    @Nullable
    @Override
    public NumericAverageState<?> newState(RamAccounting ramAccounting,
                                           Version minNodeInCluster,
                                           MemoryManager memoryManager) {
        ramAccounting.addBytes(INIT_SIZE);
        return new NumericAverageState<>(new BigDecimalValueWrapper(BigDecimal.ZERO), 0L);
    }

    @Override
    public NumericAverageState<?> iterate(RamAccounting ramAccounting,
                                          MemoryManager memoryManager,
                                          NumericAverageState<?> state,
                                          Input<?> ... args) throws CircuitBreakingException {
        if (state != null) {
            BigDecimal value = returnType.implicitCast(args[0].value());
            if (value != null) {
                BigDecimal newValue = state.sum.value().add(value);
                ramAccounting.addBytes(NumericType.sizeDiff(newValue, state.sum.value()));
                (state.sum).setValue(newValue);
                state.count++;
            }
        }
        return state;
    }

    @Override
    public NumericAverageState<?> reduce(RamAccounting ramAccounting,
                                         NumericAverageState<?> state1,
                                         NumericAverageState<?> state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        BigDecimal newValue = state1.sum.value().add(state2.sum.value());
        ramAccounting.addBytes(NumericType.sizeDiff(newValue, state1.sum.value()));
        (state1.sum).setValue(newValue);
        state1.count += state2.count;
        return state1;
    }

    @Override
    public BigDecimal terminatePartial(RamAccounting ramAccounting, NumericAverageState<?> state) {
        return state.value();
    }

    @Override
    public DataType<?> partialType() {
        return NumericAverageStateType.INSTANCE;
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
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public NumericAverageState<?> removeFromAggregatedState(
        RamAccounting ramAccounting,
        NumericAverageState<?> previousAggState,
        Input<?>[]stateToRemove) {

        if (previousAggState != null) {
            BigDecimal value = returnType.implicitCast(stateToRemove[0].value());
            if (value != null) {
                BigDecimal newValue = previousAggState.sum.value().subtract(value);
                ramAccounting.addBytes(NumericType.sizeDiff(newValue, previousAggState.sum.value()));
                previousAggState.count--;
                (previousAggState.sum).setValue(newValue);
            }
        }
        return previousAggState;
    }

    @Nullable
    @Override
    public DocValueAggregator<?> getDocValueAggregator(LuceneReferenceResolver referenceResolver,
                                                       List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       Version shardCreatedVersion,
                                                       List<Literal<?>> optionalParams) {
        return getNumericDocValueAggregator(
            aggregationReferences,
            (ramAccounting, state, bigDecimal) -> {
                BigDecimal newValue = state.sum.value().add(bigDecimal);
                ramAccounting.addBytes(NumericType.sizeDiff(newValue, state.sum.value()));
                state.sum.setValue(newValue);
                state.count++;
            });
    }
}
