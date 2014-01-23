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

import io.crate.metadata.FunctionImplementation;
import io.crate.operator.Input;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;

import java.util.List;

public abstract class AggregationFunction<T extends AggregationState> implements FunctionImplementation<Aggregation> {


    /**
     * Apply the columnValue to the argument AggState using the logic in this AggFunction
     *
     * @param state the aggregation state for the iteration
     * @param args  the arguments according to FunctionInfo.argumentTypes
     * @return false if we do not need any further iteration for this state
     */
    public abstract boolean iterate(T state, Input... args);


    /**
     * Creates a new state for this aggregation
     *
     * @return a new state instance
     */
    public abstract T newState();


    @Override
    public Symbol normalizeSymbol(Aggregation symbol) {
        return symbol;
    }
}
