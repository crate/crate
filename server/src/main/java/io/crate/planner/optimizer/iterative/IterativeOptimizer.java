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

import static io.crate.planner.optimizer.Optimizer.removeExcludedRules;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.statistics.TableStats;

/**
 * The optimizer takes an operator tree of logical plans and creates an optimized plan.
 * The optimization loop applies rules recursively until a fixpoint is reached.
 */
public class IterativeOptimizer {

    private static final Logger LOGGER = LogManager.getLogger(IterativeOptimizer.class);

    private final List<Rule<?>> rules;
    private final Supplier<Version> minNodeVersionInCluster;
    private final NodeContext nodeCtx;

    public IterativeOptimizer(NodeContext nodeCtx, Supplier<Version> minNodeVersionInCluster, List<Rule<?>> rules) {
        this.rules = rules;
        this.minNodeVersionInCluster = minNodeVersionInCluster;
        this.nodeCtx = nodeCtx;
    }

    public LogicalPlan optimize(LogicalPlan plan, TableStats tableStats, CoordinatorTxnCtx txnCtx) {
        // Create a new memo for the given plan tree
        var memo = new Memo(plan);

        // Resolves a Group Reference to it's referenced LogicalPlan.
        // This is used inside the rules to manifest the LogicalPlan from the sources
        Function<LogicalPlan, LogicalPlan> groupReferenceResolver = node -> {
            if (node instanceof GroupReference g) {
                return memo.resolve(g.groupId());
            }
            // not a group reference, return same node
            return node;
        };
        // Remove rules from the applicable rule set if some are disabled by a session setting
        var applicableRules = removeExcludedRules(rules, txnCtx.sessionSettings().excludedOptimizerRules());
        // Start exploring from the root, root group is root node from the tree
        exploreGroup(memo.getRootGroup(), new Context(memo, groupReferenceResolver, applicableRules, txnCtx, tableStats));
        // Return the final tree with all group references resolved
        return memo.extract();
    }

    private boolean exploreGroup(int group, Context context) {
        // tracks whether this group or any children groups change as
        // this method executes
        var progress = exploreNode(group, context);

        while (exploreChildren(group, context)) {
            progress = true;
            // This is an important part! We keep track
            // if the children changed and try again the
            // current group in case we can match additional rules
            if (!exploreNode(group, context)) {
                // no additional matches, so bail out
                break;
            }
        }
        return progress;
    }

    private boolean exploreNode(int group, Context context) {
        final boolean isTraceEnabled = LOGGER.isTraceEnabled();
        var rules = context.rules;
        var resolvePlan = context.groupReferenceResolver;
        var node = context.memo.resolve(group);

        var done = false;
        var progress = false;
        var minVersion = minNodeVersionInCluster.get();
        while (!done) {
            done = true;
            for (Rule rule : rules) {
                if (minVersion.before(rule.requiredVersion())) {
                    continue;
                }
                Match<?> match = rule.pattern().accept(node, Captures.empty(), resolvePlan);
                if (match.isPresent()) {
                    if (isTraceEnabled) {
                        LOGGER.trace("Rule '" + rule.getClass().getSimpleName() + "' matched");
                    }
                    LogicalPlan transformed = rule.apply(
                        match.value(),
                        match.captures(),
                        context.tableStats,
                        context.txnCtx,
                        nodeCtx,
                        context.groupReferenceResolver
                    );
                    if (transformed != null) {
                        if (isTraceEnabled) {
                            LOGGER.trace("Rule '" + rule.getClass().getSimpleName() + "' transformed the logical plan");
                        }
                        // the plan changed, update memo to reference to the new plan
                        context.memo.replace(group, transformed);
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

        var expression = context.memo.resolve(group);
        for (var child : expression.sources()) {
            if (child instanceof GroupReference g) {
                if (exploreGroup(g.groupId(), context)) {
                    progress = true;
                }
            } else {
                throw new IllegalStateException("Expected child to be a group reference. Found: " + child.getClass().getName());
            }
        }
        return progress;
    }

    private static class Context {
        final Memo memo;
        final Function<LogicalPlan, LogicalPlan> groupReferenceResolver;
        final CoordinatorTxnCtx txnCtx;
        final TableStats tableStats;
        final List<Rule<?>> rules;

        public Context(Memo memo,
                       Function<LogicalPlan, LogicalPlan> groupReferenceResolver,
                       List<Rule<?>> rules,
                       CoordinatorTxnCtx txnCtx,
                       TableStats tableStats) {
            this.memo = memo;
            this.groupReferenceResolver = groupReferenceResolver;
            this.txnCtx = txnCtx;
            this.tableStats = tableStats;
            this.rules = rules;
        }
    }
}
