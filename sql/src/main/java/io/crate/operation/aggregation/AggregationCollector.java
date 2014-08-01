/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.aggregation;

import io.crate.operation.Input;
import io.crate.operation.collect.RowCollector;
import io.crate.planner.symbol.Aggregation;

import java.util.Locale;

public class AggregationCollector implements RowCollector {

    private final Input[] inputs;
    private final AggregationFunction aggregationFunction;
    private final FromImpl fromImpl;
    private final ToImpl toImpl;

    private AggregationState aggregationState;

    public AggregationCollector(Aggregation a, AggregationFunction aggregationFunction, Input... inputs) {
        if (a.fromStep() == Aggregation.Step.PARTIAL && inputs.length > 1) {
            throw new UnsupportedOperationException("Aggregation from PARTIAL is only allowed with one input.");
        }

        switch (a.fromStep()) {
            case ITER:
                fromImpl = new FromIter();
                break;
            case PARTIAL:
                fromImpl = new FromPartial();
                break;
            case FINAL:
                throw new UnsupportedOperationException("Can't start from FINAL");
            default:
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "invalid from step %s", a.fromStep().name()));
        }

        switch (a.toStep()) {
            case ITER:
                throw new UnsupportedOperationException("Can't aggregate to ITER");
            case PARTIAL:
                toImpl = new ToPartial();
                break;
            case FINAL:
                toImpl = new ToFinal();
                break;
            default:
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "invalid to step %s", a.toStep().name()));
        }

        this.inputs = inputs;
        this.aggregationFunction = aggregationFunction;
    }


    public boolean startCollect() {
        aggregationState = fromImpl.startCollect();
        return true;
    }

    public boolean processRow() {
        return fromImpl.processRow();
    }


    public Object finishCollect() {
        return toImpl.finishCollect();
    }

    public AggregationState state() {
        return aggregationState;
    }

    public void state(AggregationState state) {
        aggregationState = state;
    }

    abstract class FromImpl {

        public AggregationState startCollect() {
            return aggregationFunction.newState();
        }

        public abstract boolean processRow();
    }

    class FromIter extends FromImpl {

        @Override
        @SuppressWarnings("unchecked")
        public boolean processRow() {
            return aggregationFunction.iterate(aggregationState, inputs);
        }
    }

    class FromPartial extends FromImpl {

        @Override
        @SuppressWarnings("unchecked")
        public boolean processRow() {
            aggregationState.reduce((AggregationState)inputs[0].value());
            return true;
        }
    }

    static abstract class ToImpl {
        public abstract Object finishCollect();
    }

    class ToPartial extends ToImpl {
        @Override
        public Object finishCollect() {
            return aggregationState;
        }
    }

    class ToFinal extends ToImpl {
        @Override
        public Object finishCollect() {
            aggregationState.terminatePartial();
            return aggregationState.value();
        }
    }
}
