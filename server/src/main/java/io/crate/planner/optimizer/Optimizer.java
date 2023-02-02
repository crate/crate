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

package io.crate.planner.optimizer;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.DefaultMatcher;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.statistics.TableStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class Optimizer {

    private static final Logger LOGGER = LogManager.getLogger(Optimizer.class);

    private final List<Rule<?>> rules;
    private final Supplier<Version> minNodeVersionInCluster;
    private final NodeContext nodeCtx;

    public Optimizer(NodeContext nodeCtx,
                     Supplier<Version> minNodeVersionInCluster,
                     List<Rule<?>> rules) {
        this.rules = rules;
        this.minNodeVersionInCluster = minNodeVersionInCluster;
        this.nodeCtx = nodeCtx;
    }

    public LogicalPlan optimize(LogicalPlan plan, TableStats tableStats, CoordinatorTxnCtx txnCtx) {
        var applicableRules = removeExcludedRules(rules, txnCtx.sessionSettings().excludedOptimizerRules());
        LogicalPlan optimizedRoot = tryApplyRules(applicableRules, plan, tableStats, txnCtx);
        var optimizedSources = Lists2.mapIfChange(optimizedRoot.sources(), x -> optimize(x, tableStats, txnCtx));
        return tryApplyRules(
            applicableRules,
            optimizedSources == optimizedRoot.sources() ? optimizedRoot : optimizedRoot.replaceSources(optimizedSources),
            tableStats,
            txnCtx
        );
    }

    @VisibleForTesting
    static List<Rule<?>> removeExcludedRules(List<Rule<?>> rules, Set<Class<? extends Rule<?>>> excludedRules) {
        final boolean isTraceEnabled = LOGGER.isTraceEnabled();
        if (excludedRules.isEmpty()) {
            return rules;
        }
        var result = new ArrayList<Rule<?>>(rules.size());
        for (var rule : rules) {
            if (excludedRules.contains(rule.getClass())) {
                if (isTraceEnabled) {
                    LOGGER.trace("Rule '" + rule.getClass().getSimpleName() + "' excluded from execution");
                }
            } else {
                result.add(rule);
            }
        }
        return result;
    }

    private LogicalPlan tryApplyRules(List<Rule<?>> rules, LogicalPlan plan, TableStats tableStats, TransactionContext txnCtx) {
        final boolean isTraceEnabled = LOGGER.isTraceEnabled();
        LogicalPlan node = plan;
        // Some rules may only become applicable after another rule triggered, so we keep
        // trying to re-apply the rules as long as at least one plan was transformed.
        boolean done = false;
        int numIterations = 0;
        while (!done && numIterations < 10_000) {
            done = true;
            Version minVersion = minNodeVersionInCluster.get();
            for (Rule rule : rules) {
                if (minVersion.before(rule.requiredVersion())) {
                    continue;
                }
                Match<?> match = DefaultMatcher.DEFAULT_MATCHER.match(rule.pattern(), plan, Captures.empty());
                if (match.isPresent()) {
                    if (isTraceEnabled) {
                        LOGGER.trace("Rule '" + rule.getClass().getSimpleName() + "' matched");
                    }
                    @SuppressWarnings("unchecked")
                    LogicalPlan transformedPlan = rule.apply(match.value(), match.captures(), tableStats, txnCtx, nodeCtx);
                    if (transformedPlan != null) {
                        if (isTraceEnabled) {
                            LOGGER.trace("Rule '" + rule.getClass().getSimpleName() + "' transformed the logical plan");
                        }
                        node = transformedPlan;
                        done = false;
                    }
                }
            }
            numIterations++;
        }
        assert numIterations < 10_000
            : "Optimizer reached 10_000 iterations safety guard. This is an indication of a broken rule that matches again and again";
        return node;
    }

}
