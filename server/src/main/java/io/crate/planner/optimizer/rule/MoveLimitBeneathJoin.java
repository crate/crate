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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;

import io.crate.expression.symbol.Literal;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.AbstractJoinPlan;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public class MoveLimitBeneathJoin implements Rule<Limit> {

    private final Capture<AbstractJoinPlan> joinCapture;
    private final Pattern<Limit> pattern;
    private static final Set<JoinType> SUPPORTED_JOIN_TYPES = EnumSet.of(JoinType.LEFT, JoinType.RIGHT, JoinType.CROSS);

    public MoveLimitBeneathJoin() {
        this.joinCapture = new Capture<>();
        this.pattern = typeOf(Limit.class)
            .with(l -> l.isPushedBeneathJoin() == false)
            .with(source(), typeOf(AbstractJoinPlan.class)
            .with(j -> SUPPORTED_JOIN_TYPES.contains(j.joinType())).capturedAs(joinCapture));
    }

    @Override
    public Pattern<Limit> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Limit limit,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             Function<LogicalPlan, LogicalPlan> resolvePlan) {
        var join = captures.get(joinCapture);
        var joinType = join.joinType();
        var addLimitToLhs = false;
        var addLimitToRhs = false;

        if (joinType == JoinType.LEFT) {
            addLimitToLhs = true;
        } else if (joinType == JoinType.RIGHT) {
            addLimitToRhs = true;
        } else if (joinType == JoinType.CROSS) {
            addLimitToLhs = true;
            addLimitToRhs = true;
        }

        var newLimit = limit.limitAndOffset();
        var newOffset = Literal.of(0);
        var newSource = new ArrayList<LogicalPlan>(2);

        if (addLimitToLhs) {
            newSource.add(new Limit(join.lhs(), newLimit, newOffset, false, false));
        } else {
            newSource.add(join.lhs());
        }
        if (addLimitToRhs) {
            newSource.add(new Limit(join.rhs(), newLimit, newOffset, false, false));
        } else {
            newSource.add(join.rhs());
        }

        return new Limit(
            join.replaceSources(newSource),
            limit.limit(),
            limit.offset(),
            limit.isPushedBeneathUnion(),
            true
        );
    }
}
