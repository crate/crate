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
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExecutionPlanVisitor;
import io.crate.planner.Merge;
import io.crate.planner.UnionExecutionPlan;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.NestedLoop;

import java.util.function.Function;

public class ExecutionPlanSymbolMapper extends ExecutionPlanVisitor<Function<? super Symbol, ? extends Symbol>, Void> {

    private static final ExecutionPlanSymbolMapper INSTANCE = new ExecutionPlanSymbolMapper();

    private ExecutionPlanSymbolMapper() {
    }

    public static void map(ExecutionPlan plan, Function<? super Symbol, ? extends Symbol> mapper) {
        INSTANCE.process(plan, mapper);
    }

    @Override
    protected Void visitPlan(ExecutionPlan executionPlan, Function<? super Symbol, ? extends Symbol> mapper) {
        throw new UnsupportedOperationException("Subselect not supported in " + executionPlan);
    }

    @Override
    public Void visitQueryThenFetch(QueryThenFetch plan, Function<? super Symbol, ? extends Symbol> mapper) {
        process(plan.subPlan(), mapper);
        // plan.fetchPhase() -> has only references - no selectSymbols
        return null;
    }

    @Override
    public Void visitCollect(Collect plan, Function<? super Symbol, ? extends Symbol> mapper) {
        plan.collectPhase().replaceSymbols(mapper);
        return null;
    }

    @Override
    public Void visitMerge(Merge merge, Function<? super Symbol, ? extends Symbol> mapper) {
        merge.mergePhase().replaceSymbols(mapper);
        process(merge.subPlan(), mapper);
        return null;
    }

    @Override
    public Void visitNestedLoop(NestedLoop plan, Function<? super Symbol, ? extends Symbol> mapper) {
        process(plan.left(), mapper);
        process(plan.right(), mapper);
        plan.nestedLoopPhase().replaceSymbols(mapper);
        return null;
    }

    @Override
    public Void visitCountPlan(CountPlan countPlan, Function<? super Symbol, ? extends Symbol> mapper) {
        countPlan.countPhase().replaceSymbols(mapper);
        return null;
    }

    @Override
    public Void visitUnionPlan(UnionExecutionPlan union, Function<? super Symbol, ? extends Symbol> mapper) {
        process(union.left(), mapper);
        process(union.right(), mapper);
        union.mergePhase().replaceSymbols(mapper);
        return null;
    }
}
