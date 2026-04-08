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

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.util.BigDecimalValueWrapper;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.NumericType;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.util.List;

/**
 * A base class for min() and max() aggregations for the numeric type that returns
 * a doc value aggregator.
 */
abstract class NumericMinMaxBase extends AggregationFunction<BigDecimalValueWrapper, BigDecimal> {
    private final Signature signature;
    private final BoundSignature boundSignature;
    private final DataType<BigDecimal> returnType;

    protected NumericMinMaxBase(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;

        // We want to preserve the scale and precision from the
        // numeric argument type for the return type. So we use
        // the incoming numeric type as return type instead of
        // the return type from the signature `sum(count::numeric(16, 2))`
        // should return the type `numeric(16, 2)` not `numeric`
        var argumentType = boundSignature.argTypes().get(0);
        assert argumentType.id() == DataTypes.NUMERIC.id();
        //noinspection unchecked
        this.returnType = (DataType<BigDecimal>) argumentType;
    }

    @Override
    @Nullable
    public BigDecimalValueWrapper newState(RamAccounting ramAccounting, Version minNodeInCluster, MemoryManager memoryManager) {
        return new BigDecimalValueWrapper(null);
    }

    @Override
    public @Nullable DocValueAggregator<?> getDocValueAggregator(LuceneReferenceResolver referenceResolver,
                                                                 List<Reference> aggregationReferences,
                                                                 DocTableInfo table,
                                                                 Version shardCreatedVersion,
                                                                 List<Literal<?>> optionalParams) {
        return getNumericDocValueAggregator(
            aggregationReferences,
            (ramAccounting, state, bigDecimal) -> {
                setNewStateValue(state, bigDecimal, ramAccounting);
            });
    }

    protected abstract void setNewStateValue(BigDecimalValueWrapper state, BigDecimal value, RamAccounting ramAccounting);

    @Override
    public BigDecimalValueWrapper iterate(RamAccounting ramAccounting,
                                          MemoryManager memoryManager,
                                          BigDecimalValueWrapper state,
                                          Input<?>... args) throws CircuitBreakingException {
        BigDecimal value = returnType.implicitCast(args[0].value());
        setNewStateValue(state, value, ramAccounting);
        return state;
    }

    @Override
    public BigDecimal terminatePartial(RamAccounting ramAccounting, BigDecimalValueWrapper state) {
        if (state.value() != null) {
            ramAccounting.addBytes(NumericType.size(state.value()));
        }

        return state.value();
    }

    @Override
    public DataType<?> partialType() {
        return returnType;
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
