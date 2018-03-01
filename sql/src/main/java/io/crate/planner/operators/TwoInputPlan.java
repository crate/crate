/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.operators;

import io.crate.expression.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * A {@link LogicalPlan} with two other LogicalPlans as input.
 */
abstract class TwoInputPlan extends LogicalPlanBase {

    final LogicalPlan lhs;
    final LogicalPlan rhs;

    TwoInputPlan(LogicalPlan left, LogicalPlan right, List<Symbol> outputs) {
        super(outputs, new HashMap<>(), new ArrayList<>(), Collections.emptyMap());
        this.lhs = left;
        this.rhs = right;
        this.baseTables.addAll(lhs.baseTables());
        this.baseTables.addAll(rhs.baseTables());
        this.expressionMapping.putAll(lhs.expressionMapping());
        this.expressionMapping.putAll(rhs.expressionMapping());
    }

    @Override
    public LogicalPlan tryOptimize(@Nullable LogicalPlan pushDown, SymbolMapper mapper) {
        if (pushDown != null) {
            return null;
        }
        LogicalPlan newLhs = lhs.tryOptimize(null, mapper);
        LogicalPlan newRhs = rhs.tryOptimize(null, mapper);
        if (newLhs != lhs || newRhs != rhs) {
            return updateSources(newLhs, newRhs);
        }
        return this;
    }

    /**
     * Creates a new LogicalPlan with an updated source. This is necessary
     * when we collapse plans during plan building or "push down" plans
     * later on to optimize their execution.
     *
     * {@link LogicalPlan}s should be immutable. Fields like sources may only
     * be updated by creating a new instance of the plan. Since Java does not
     * allow to copy an instance easily and also instances might apply a custom
     * logic when they clone itself, this method has to be implemented.
     * @param newLeftSource A new {@link} LogicalPlan as the left source.
     * @param newRightSource A new {@link} LogicalPlan as the right source.
     * @return A new copy of this {@link OneInputPlan} with the new sources.
     */
    protected abstract LogicalPlan updateSources(LogicalPlan newLeftSource, LogicalPlan newRightSource);
}
