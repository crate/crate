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

package io.crate.planner.operators;

import io.crate.expression.symbol.Symbol;
import io.crate.planner.optimizer.costs.PlanStats;

import java.util.Collections;
import java.util.List;

import static io.crate.planner.operators.GroupHashAggregate.approximateDistinctValues;

public final class Distinct {

    public static LogicalPlan create(LogicalPlan source, boolean distinct, List<Symbol> outputs, PlanStats planStats) {
        if (!distinct) {
            return source;
        }
        long numExpectedRows = approximateDistinctValues(planStats.get(source).numDocs(), planStats.tableStats(), outputs);
        return new GroupHashAggregate(source, outputs, Collections.emptyList(), numExpectedRows);
    }
}
