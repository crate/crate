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

import io.crate.analyze.symbol.Aggregation;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;

/**
 * A wrapper around an AggregationFunction that is aware of the aggregation steps (iter, partial, final)
 * and will call the correct functions on the aggregationFunction depending on these steps.
 */
public class Aggregator {

    private final Input[] inputs;
    private final AggregationFunction aggregationFunction;
    private final FromImpl fromImpl;
    private final ToImpl toImpl;

    public Aggregator(RamAccountingContext ramAccountingContext,
                      Aggregation a,
                      AggregationFunction aggregationFunction,
                      Input... inputs) {
        if (a.mode() == Aggregation.Mode.PARTIAL_FINAL && inputs.length > 1) {
            throw new UnsupportedOperationException("Aggregation from PARTIAL is only allowed with one input.");
        }

        switch (a.mode()) {
            case ITER_FINAL:
                fromImpl = new FromIter(ramAccountingContext);
                toImpl = new ToFinal(ramAccountingContext);
                break;

            case ITER_PARTIAL:
                fromImpl = new FromIter(ramAccountingContext);
                toImpl = new ToPartial(ramAccountingContext);
                break;

            case PARTIAL_FINAL:
                fromImpl = new FromPartial(ramAccountingContext);
                toImpl = new ToFinal(ramAccountingContext);
                break;

            default:
                throw new UnsupportedOperationException("invalid mode " + a.mode().name());
        }
        this.inputs = inputs;
        this.aggregationFunction = aggregationFunction;
    }


    public Object prepareState() {
        return fromImpl.prepareState();
    }

    public Object processRow(Object value) {
        return fromImpl.processRow(value);
    }

    public Object finishCollect(Object state) {
        return toImpl.finishCollect(state);
    }

    abstract class FromImpl {

        protected final RamAccountingContext ramAccountingContext;

        public FromImpl(RamAccountingContext ramAccountingContext) {
            this.ramAccountingContext = ramAccountingContext;
        }

        public Object prepareState() {
            return aggregationFunction.newState(ramAccountingContext);
        }

        public abstract Object processRow(Object value);
    }

    class FromIter extends FromImpl {

        public FromIter(RamAccountingContext ramAccountingContext) {
            super(ramAccountingContext);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object processRow(Object value) {
            return aggregationFunction.iterate(ramAccountingContext, value, inputs);
        }
    }

    class FromPartial extends FromImpl {

        public FromPartial(RamAccountingContext ramAccountingContext) {
            super(ramAccountingContext);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object processRow(Object value) {
            return aggregationFunction.reduce(ramAccountingContext, value, inputs[0].value());
        }
    }

    static abstract class ToImpl {
        protected final RamAccountingContext ramAccountingContext;

        public ToImpl(RamAccountingContext ramAccountingContext) {
            this.ramAccountingContext = ramAccountingContext;
        }

        public abstract Object finishCollect(Object state);
    }

    class ToPartial extends ToImpl {

        public ToPartial(RamAccountingContext ramAccountingContext) {
            super(ramAccountingContext);
        }

        @Override
        public Object finishCollect(Object state) {
            return state;
        }
    }

    class ToFinal extends ToImpl {

        public ToFinal(RamAccountingContext ramAccountingContext) {
            super(ramAccountingContext);
        }

        @Override
        public Object finishCollect(Object state) {
            //noinspection unchecked
            return aggregationFunction.terminatePartial(ramAccountingContext, state);
        }
    }
}
