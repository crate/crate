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
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.elasticsearch.Version;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Optimizer;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.tracer.OptimizerTracer;

/**
 * The optimizer takes an operator tree of logical plans and creates an optimized plan.
 * The optimization loop applies rules recursively until a fixpoint is reached.
 */
public class IterativeOptimizer {

    private final List<Rule<?>> rules;
    private final Supplier<Version> minNodeVersionInCluster;
    private final NodeContext nodeCtx;

    public IterativeOptimizer(NodeContext nodeCtx, Supplier<Version> minNodeVersionInCluster, List<Rule<?>> rules) {
        this.rules = rules;
        this.minNodeVersionInCluster = minNodeVersionInCluster;
        this.nodeCtx = nodeCtx;
    }

    public LogicalPlan optimize(LogicalPlan plan, PlanStats planStats, CoordinatorTxnCtx txnCtx, OptimizerTracer tracer, PlannerContext plannerContext) {
        var memo = new Memo(plan);
        var planStatsWithMemo = planStats.withMemo(memo);

        // Memo is used to have a mutable view over the tree so it can change nodes without
        // having to re-build the full tree all the time.`GroupReference` is used as place-holder
        // or proxy that must be resolved to the real plan node
        UnaryOperator<LogicalPlan> groupReferenceResolver = node -> {
            if (node instanceof GroupReference g) {
                return memo.resolve(g.groupId());
            }
            // not a group reference, return same node
            return node;
        };

        tracer.optimizationStarted(plan, planStatsWithMemo);

        var applicableRules = removeExcludedRules(rules, txnCtx.sessionSettings().excludedOptimizerRules());
        exploreGroup(memo.getRootGroup(), new Context(memo, groupReferenceResolver, applicableRules, txnCtx, planStatsWithMemo, tracer, plannerContext));
        return memo.extract();
    }

    /**
     *
     * This processes a group by trying to apply all the rules of the optimizer to the given group and its children.
     * If any children are changed by a rule, the given group will be reprocessed to check if additional rules
     * can be matched until a fixpoint is reached.
     *
     * @param group the id of the group to explore
     * @param context the context of the optimizer
     * @return true if there were any changes of plans on the node or it's children or false if not
     */
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
        var rules = context.rules;
        var resolvePlan = context.groupReferenceResolver;
        var node = context.memo.resolve(group);

        int numIteration = 0;
        int maxIterations = 10_000;
        boolean progress = false;
        boolean done = false;
        var minVersion = minNodeVersionInCluster.get();
        while (!done && numIteration < maxIterations) {
            numIteration++;
            done = true;
            for (Rule<?> rule : rules) {
                if (minVersion.before(rule.requiredVersion())) {
                    continue;
                }
                LogicalPlan transformed = Optimizer.tryMatchAndApply(
                    rule,
                    node,
                    context.planStats,
                    nodeCtx,
                    context.txnCtx,
                    resolvePlan,
                    context.tracer,
                    context.plannerContext
                );
                if (transformed != null) {
                    // the plan changed, update memo to reference to the new plan
                    context.memo.replace(group, transformed);
                    node = transformed;
                    done = false;
                    progress = true;
                    var tracer = context.tracer;
                    if (tracer.isActive()) {
                        tracer.ruleApplied(rule, context.memo.extract(), context.planStats);
                    }
                }
            }
        }
        assert numIteration < maxIterations
            : "Optimizer reached 10_000 iterations safety guard. This is an indication of a broken rule that matches again and again";

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

    private record Context(
        Memo memo,
        UnaryOperator<LogicalPlan> groupReferenceResolver,
        List<Rule<?>> rules,
        CoordinatorTxnCtx txnCtx,
        PlanStats planStats,
        OptimizerTracer tracer,
        PlannerContext plannerContext
    ) {}
}
