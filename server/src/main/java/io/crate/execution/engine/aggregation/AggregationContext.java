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

import java.util.List;

import io.crate.data.Input;
import io.crate.types.DataType;

public final class AggregationContext {

    private final AggregationFunction impl;
    private final Input<Boolean> filter;
    private final Input<?>[] inputs;
    private final DataType<?> partialType;

    public AggregationContext(AggregationFunction aggregationFunction,
                              Input<Boolean> filter,
                              List<Input<?>> inputs,
                              DataType<?> partialType) {
        this.impl = aggregationFunction;
        this.filter = filter;
        this.inputs = inputs.toArray(new Input[0]);
        this.partialType = partialType;
    }

    public AggregationFunction function() {
        return impl;
    }

    /**
     * The partial-state type the plan chose for this aggregation (the type used to stream the partial
     * state cluster-wide). Passed into {@link AggregationFunction#newState(io.crate.data.breaker.RamAccounting,
     * DataType, org.elasticsearch.Version, io.crate.memory.MemoryManager)} so the accumulator layout follows
     * the wire format rather than being re-derived from a local version.
     */
    public DataType<?> partialType() {
        return partialType;
    }

    public Input<?>[] inputs() {
        return inputs;
    }

    public Input<Boolean> filter() {
        return filter;
    }
}
