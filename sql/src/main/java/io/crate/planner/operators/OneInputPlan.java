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

import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * A {@link LogicalPlan} with one other LogicalPlan as input.
 */
abstract class OneInputPlan extends LogicalPlanBase {

    protected final LogicalPlan source;

    OneInputPlan(LogicalPlan source) {
        this(source, source.outputs());
    }

    OneInputPlan(LogicalPlan source, Map<LogicalPlan, SelectSymbol> dependencies) {
        this(source, source.outputs(), source.expressionMapping(), source.baseTables(), dependencies);
    }

    OneInputPlan(LogicalPlan source, List<Symbol> outputs) {
        this(source, outputs, source.expressionMapping(), source.baseTables(), source.dependencies());
    }

    OneInputPlan(LogicalPlan source,
                 List<Symbol> outputs,
                 Map<Symbol, Symbol> expressionMapping,
                 List<AbstractTableRelation> baseTables,
                 Map<LogicalPlan, SelectSymbol> dependencies) {
        super(outputs, expressionMapping, baseTables, dependencies);
        this.source = source;
    }


    @Override
    public LogicalPlan tryOptimize(@Nullable LogicalPlan pushDown, SymbolMapper mapper) {
        if (pushDown != null) {
            // can't push down
            return null;
        }
        LogicalPlan newPlan = source.tryOptimize(null, mapper);
        if (newPlan != source) {
            return updateSource(newPlan, mapper);
        }
        return this;
    }

    /**
     * If no other information available, return the source's number of rows.
     * @return The number of rows of the source plan.
     */
    @Override
    public long numExpectedRows() {
        return source.numExpectedRows();
    }

    @Override
    public long estimatedRowSize() {
        return source.estimatedRowSize();
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
     * @param newSource A new {@link LogicalPlan} as a source.
     * @return A new copy of this {@link OneInputPlan} with the new source.
     */
    protected abstract LogicalPlan updateSource(LogicalPlan newSource, SymbolMapper mapper);

}
