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

package io.crate.operator.aggregation;

import io.crate.operator.Input;
import io.crate.operator.RowCollector;
import io.crate.planner.symbol.Aggregation;

import java.util.List;

public class AggregationCollector implements RowCollector {

    private final Input[] inputs;
    private final Aggregation aggregation;
    private AggregationState aggregationState;
    private AggregationFunction aggregationFunction;

    public AggregationCollector(Aggregation a, AggregationFunction aggregationFunction, Input... inputs) {
        // TODO: implement othe start end steps
        assert (a.fromStep() == Aggregation.Step.ITER);
        assert (a.toStep() == Aggregation.Step.FINAL);
        this.inputs = inputs;
        this.aggregationFunction = aggregationFunction;
        this.aggregation = a;
    }

    public boolean startCollect() {
        aggregationState = aggregationFunction.newState();
        return true;
    }

    public boolean processRow() {
        return aggregationFunction.iterate(aggregationState, inputs);
    }


    public Object finishCollect() {
        aggregationState.terminatePartial();
        return aggregationState.value();
    }

    public AggregationState state() {
        return aggregationState;
    }
}
