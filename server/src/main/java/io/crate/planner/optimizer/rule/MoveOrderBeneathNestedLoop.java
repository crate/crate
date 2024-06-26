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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import io.crate.analyze.OrderBy;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.AbstractJoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.operators.Order;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

/* Move the orderBy expression to the sub-relation if possible.
 *
 * This is possible because a nested loop preserves the ordering of the input-relation
 * IF:
 *   - the order by expressions only operate using fields from a single relation
 *   - that relation happens to be on the left-side of the join
 *   - there is no outer join involved in the whole join (outer joins may create null rows - breaking the ordering)
 */
public final class MoveOrderBeneathNestedLoop implements Rule<Order> {

    private final Capture<NestedLoopJoin> nlCapture;
    private final Pattern<Order> pattern;

    public MoveOrderBeneathNestedLoop() {
        this.nlCapture = new Capture<>();
        this.pattern = typeOf(Order.class)
            .with(source(),
                  typeOf(NestedLoopJoin.class)
                      .capturedAs(nlCapture)
                      .with(nl -> !nl.joinType().isOuter())
            );
    }

    @Override
    public Pattern<Order> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Order order,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {
        NestedLoopJoin nestedLoop = captures.get(nlCapture);
        Set<RelationName> relationsInOrderBy =
            Collections.newSetFromMap(new IdentityHashMap<>());
        Consumer<ScopedSymbol> gatherRelationsFromField = f -> relationsInOrderBy.add(f.relation());
        Consumer<Reference> gatherRelationsFromRef = r -> relationsInOrderBy.add(r.ident().tableIdent());
        OrderBy orderBy = order.orderBy();
        for (Symbol orderExpr : orderBy.orderBySymbols()) {
            FieldsVisitor.visitFields(orderExpr, gatherRelationsFromField);
            orderExpr.visitRefs(gatherRelationsFromRef);
        }
        if (relationsInOrderBy.size() == 1) {
            var relationInOrderBy = relationsInOrderBy.iterator().next();
            var topMostLeftRelation = nestedLoop.relationNames().get(0);
            if (relationInOrderBy.equals(topMostLeftRelation)) {
                LogicalPlan lhs = nestedLoop.sources().get(0);
                LogicalPlan newLhs = order.replaceSources(List.of(lhs));
                return new NestedLoopJoin(
                    newLhs,
                    nestedLoop.sources().get(1),
                    nestedLoop.joinType(),
                    nestedLoop.joinCondition(),
                    nestedLoop.isFiltered(),
                    true,
                    nestedLoop.isRewriteNestedLoopJoinToHashJoinDone(),
                    AbstractJoinPlan.LookUpJoin.NONE
                );
            }
        }
        return null;
    }
}
