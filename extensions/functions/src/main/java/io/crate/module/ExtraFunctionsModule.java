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

package io.crate.module;

import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.AbstractFunctionModule;
import io.crate.operation.aggregation.HyperLogLogDistinctAggregation;
import io.crate.window.NthValueFunctions;
import io.crate.window.OffsetValueFunctions;
import io.crate.window.RankFunctions;

public class ExtraFunctionsModule extends AbstractFunctionModule<AggregationFunction<?, ?>> {

    @Override
    public void configureFunctions() {
        HyperLogLogDistinctAggregation.register(this);
        NthValueFunctions.register(this);
        OffsetValueFunctions.register(this);
        RankFunctions.register(this);
    }
}
