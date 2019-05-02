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

package io.crate.planner.optimizer;

import io.crate.collections.Lists2;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;

import java.util.List;

public class Optimizer {

    private List<Rule<?>> rules;

    public Optimizer(List<Rule<?>> rules) {
        this.rules = rules;
    }

    public LogicalPlan optimize(LogicalPlan plan) {
        LogicalPlan optimizedRoot = tryApplyRules(plan);
        return optimizedRoot.replaceSources(Lists2.map(optimizedRoot.sources(), this::optimize));
    }

    private LogicalPlan tryApplyRules(LogicalPlan plan) {
        LogicalPlan node = plan;
        // Some rules may only become applicable after another rule triggered, so we keep
        // trying to re-apply the rules as long as at least one plan was transformed.
        boolean done = false;
        while (!done) {
            done = true;
            for (Rule rule : rules) {
                Match match = rule.pattern().accept(node, Captures.empty());
                if (match.isPresent()) {
                    @SuppressWarnings("unchecked")
                    LogicalPlan transformedPlan = rule.apply(match.value(), match.captures());
                    if (transformedPlan != null) {
                        node = transformedPlan;
                        done = false;
                    }
                }
            }
        }
        return node;
    }
}
