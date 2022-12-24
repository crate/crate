/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.optimizer.iterative;

import java.util.List;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.iterative.rule.Rule;
import io.crate.planner.optimizer.iterative.matcher.Captures;
import io.crate.planner.optimizer.iterative.matcher.Match;
import io.crate.statistics.TableStats;

public class IterativeOptimizer {

    private static final Logger LOGGER = LogManager.getLogger(IterativeOptimizer.class);

    private final List<Rule<?>> rules;
    private final NodeContext nodeCtx;

    public IterativeOptimizer(NodeContext nodeCtx, List<Rule<?>> rules) {
        this.rules = rules;
        this.nodeCtx = nodeCtx;
    }

    public LogicalPlan optimize(LogicalPlan plan, TableStats tableStats, CoordinatorTxnCtx txnCtx) {
        Memo memo = new Memo(txnCtx.idAllocator(), plan);

        Lookup lookup = node -> {
            if (node instanceof GroupReference) {
                return Stream.of(memo.getNode(((GroupReference) node).groupId()));
            }
            return Stream.of(node);
        };

        exploreGroup(memo.getRootGroup(), new Context(memo, lookup, txnCtx, tableStats));

        return memo.extract();
    }

    private boolean exploreGroup(int group, Context context) {
        // tracks whether this group or any children groups change as
        // this method executes
        boolean progress = exploreNode(group, context);

        while (exploreChildren(group, context)) {
            progress = true;
            // if children changed, try current group again
            // in case we can match additional rules
            if (!exploreNode(group, context)) {
                // no additional matches, so bail out
                break;
            }
        }
        return progress;
    }

    private boolean exploreNode(int group, Context context) {
        final boolean isTraceEnabled = LOGGER.isTraceEnabled();
        LogicalPlan node = context.memo().getNode(group);
        boolean done = false;
        boolean progress = false;

        while (!done) {
            done = true;
            for (Rule rule : rules) {
                Match<?> match = rule.pattern().accept(node, Captures.empty(), context.lookup);
                if (match.isPresent()) {
                    if (isTraceEnabled) {
                        LOGGER.trace("Rule '" + rule.getClass().getSimpleName() + "' matched");
                    }
                    @SuppressWarnings("unchecked")
                    LogicalPlan transformed = rule.apply(node,
                                                         match.captures(),
                                                         context.tableStats(),
                                                         context.txnCtx,
                                                         nodeCtx,
                                                         context.lookup);

                    if (transformed != null) {
                        context.memo().replace(group, transformed, rule.getClass().getName());
                        node = transformed;
                        done = false;
                        progress = true;
                    }
                }
            }
        }

        return progress;
    }

    private boolean exploreChildren(int group, Context context) {
        boolean progress = false;

        LogicalPlan expression = context.memo().getNode(group);
        for (LogicalPlan child : expression.sources()) {
            if (!(child instanceof GroupReference)) {
                throw new IllegalStateException(
                    "Expected child to be a group reference. Found: " + child.getClass().getName());
            }

            if (exploreGroup(((GroupReference) child).groupId(), context)) {
                progress = true;
            }
        }

        return progress;
    }

    private static class Context {
        private final Memo memo;
        private final Lookup lookup;
        private final CoordinatorTxnCtx txnCtx;
        private final TableStats tableStats;

        public Context(Memo memo, Lookup lookup, CoordinatorTxnCtx txnCtx, TableStats tableStats) {
            this.memo = memo;
            this.lookup = lookup;
            this.txnCtx = txnCtx;
            this.tableStats = tableStats;
        }

        public Memo memo() {
            return memo;
        }

        public Lookup lookup() {
            return lookup;
        }

        public TableStats tableStats() {
            return tableStats;
        }
    }
}
