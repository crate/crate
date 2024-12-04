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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

import io.crate.planner.operators.EquiJoinDetector;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public class RewriteLeftOuterJoinToHashJoin implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern =
        typeOf(JoinPlan.class)
            .with(j -> j.joinType() == JoinType.LEFT &&
                EquiJoinDetector.isEquiJoin(j.joinCondition()));

    @Override
    public Pattern<JoinPlan> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(JoinPlan join,
                             Captures captures,
                             Rule.Context context) {
        if (context.txnCtx().sessionSettings().hashJoinsEnabled()) {
            return new HashJoin(
                join.lhs(),
                join.rhs(),
                join.joinCondition(),
                join.joinType(),
                join.lookUpJoin()
            );
        }
        return null;
    }
}
