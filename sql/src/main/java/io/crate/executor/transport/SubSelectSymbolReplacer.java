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

package io.crate.executor.transport;

import com.google.common.util.concurrent.FutureCallback;
import io.crate.analyze.symbol.FunctionCopyVisitor;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.collections.Lists2;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExecutionPlanVisitor;
import io.crate.planner.Merge;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.planner.node.dml.DeleteById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.ESGet;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.NestedLoop;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;

/**
 * Replaces all symbols in the querySpec that match the given selectSymbol once the futureCallback
 * triggers with a Literal containing the value from the callback.
 */
class SubSelectSymbolReplacer implements FutureCallback<Object> {

    private final ExecutionPlan executionPlan;
    private final SelectSymbol selectSymbolToReplace;
    private static final ExecutionPlanSymbolVisitor PLAN_SYMBOL_VISITOR = new ExecutionPlanSymbolVisitor();

    SubSelectSymbolReplacer(ExecutionPlan executionPlan, SelectSymbol selectSymbolToReplace) {
        this.executionPlan = executionPlan;
        this.selectSymbolToReplace = selectSymbolToReplace;
    }

    @Override
    public void onSuccess(@Nullable Object result) {
        // sub-selects on the same level may run concurrently, but symbol mutation is not thread-safe
        synchronized (executionPlan) {
            PLAN_SYMBOL_VISITOR.process(executionPlan, new SymbolReplacer(selectSymbolToReplace, result));
        }
    }

    @Override
    public void onFailure(@Nonnull Throwable t) {
    }

    private static class ExecutionPlanSymbolVisitor extends ExecutionPlanVisitor<SymbolReplacer, Void> {

        @Override
        protected Void visitPlan(ExecutionPlan executionPlan, SymbolReplacer context) {
            throw new UnsupportedOperationException("Subselect not supported in " + executionPlan);
        }

        @Override
        public Void visitDeleteById(DeleteById plan, SymbolReplacer replacer) {
            for (DocKeys.DocKey docKey : plan.docKeys()) {
                Lists2.replaceItems(docKey.values(), replacer);
            }
            return null;
        }

        @Override
        public Void visitDeletePartitions(DeletePartitions plan, SymbolReplacer replacer) {
            for (List<Symbol> symbols : plan.partitions()) {
                Lists2.replaceItems(symbols, replacer);
            }
            return null;
        }

        @Override
        public Void visitMultiPhasePlan(MultiPhasePlan multiPhasePlan, SymbolReplacer replacer) {
            process(multiPhasePlan.rootPlan(), replacer);
            return null;
        }

        @Override
        public Void visitQueryThenFetch(QueryThenFetch plan, SymbolReplacer replacer) {
            process(plan.subPlan(), replacer);
            // plan.fetchPhase() -> has only references - no selectSymbols
            return null;
        }

        @Override
        public Void visitCollect(Collect plan, SymbolReplacer replacer) {
            plan.collectPhase().replaceSymbols(replacer);
            return null;
        }

        @Override
        public Void visitMerge(Merge merge, SymbolReplacer replacer) {
            merge.mergePhase().replaceSymbols(replacer);
            process(merge.subPlan(), replacer);
            return null;
        }

        @Override
        public Void visitGetPlan(ESGet plan, SymbolReplacer replacer) {
            Lists2.replaceItems(plan.outputs(), replacer);
            Lists2.replaceItems(plan.sortSymbols(), replacer);
            for (DocKeys.DocKey current : plan.docKeys()) {
                Lists2.replaceItems(current.values(), replacer);
            }
            return null;
        }

        @Override
        public Void visitNestedLoop(NestedLoop plan, SymbolReplacer replacer) {
            process(plan.left(), replacer);
            process(plan.right(), replacer);
            plan.nestedLoopPhase().replaceSymbols(replacer);
            return null;
        }

        @Override
        public Void visitCountPlan(CountPlan countPlan, SymbolReplacer replacer) {
            countPlan.countPhase().replaceSymbols(replacer);
            return null;
        }
    }

    private static class SymbolReplacer extends FunctionCopyVisitor<Void> implements Function<Symbol, Symbol> {

        private final SelectSymbol selectSymbolToReplace;
        private final Object value;

        private SymbolReplacer(SelectSymbol selectSymbolToReplace, Object value) {
            super();
            this.selectSymbolToReplace = selectSymbolToReplace;
            this.value = value;
        }

        @Override
        public Symbol visitSelectSymbol(SelectSymbol selectSymbol, Void context) {
            if (selectSymbol == selectSymbolToReplace) {
                return Literal.of(selectSymbolToReplace.valueType(), value);
            }
            return selectSymbol;
        }

        @Nullable
        @Override
        public Symbol apply(@Nullable Symbol input) {
            return process(input, null);
        }
    }
}
