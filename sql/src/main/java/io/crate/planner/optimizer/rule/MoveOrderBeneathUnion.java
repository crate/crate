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

package io.crate.planner.optimizer.rule;

import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.Order;
import io.crate.planner.operators.Union;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

import java.util.List;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

public final class MoveOrderBeneathUnion implements Rule<Order> {

    private final Capture<Union> unionCapture;
    private final Pattern<Order> pattern;

    public MoveOrderBeneathUnion() {
        this.unionCapture = new Capture<>();
        this.pattern = typeOf(Order.class)
            .with(source(), typeOf(Union.class).capturedAs(unionCapture))
            .with(MoveOrderBeneathUnion::orderDoesNotContainAnyLiteral);
    }

    @Override
    public Pattern<Order> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Order order, Captures captures) {
        Union union = captures.get(unionCapture);
        List<LogicalPlan> unionSources = union.sources();
        assert unionSources.size() == 2 : "A UNION must have exactly 2 unionSources";
        Order lhsOrder = updateSources(order, unionSources.get(0));
        Order rhsOrder = updateSources(order, unionSources.get(1));
        return union.replaceSources(List.of(lhsOrder, rhsOrder));
    }

    private static Order updateSources(Order order, LogicalPlan source) {
        List<Symbol> sourceOutputs = order.source().outputs();
        return new Order(source, order.orderBy().map(s -> {
            int idx = sourceOutputs.indexOf(s);
            if (idx < 0) {
                throw new IllegalArgumentException(
                    "The ORDER BY expression " + s + " must be part of the child union: " + sourceOutputs);
            }
            return source.outputs().get(idx);
        }));
    }

    /**
     * An order on a union always references to the position on each sub relations outputs.
     * If one symbol is a literal, it must be applied after the union as each sub relation may return different literals.
     * In such cases, the push down must be prevented.
     * (Pushing down parts of the order by could be done but requires order symbols splitting)
     *
     * Example query:
     *
     *  SELECT * FROM (
     *     SELECT 1 AS x, y FROM t1
     *     UNION ALL
     *     SELECT 2, z FROM t2
     *  ) c
     *  ORDER BY x
     *
     */
    private static boolean orderDoesNotContainAnyLiteral(Order order) {
        for (Symbol symbol : order.orderBy().orderBySymbols()) {
            if (ContainsOrPointsToAnyLiteralSymbol.INSTANCE.process(symbol, null)) {
                return false;
            }
        }
        return true;
    }

    private static class ContainsOrPointsToAnyLiteralSymbol extends DefaultTraversalSymbolVisitor<Void, Boolean> {

        private static final ContainsOrPointsToAnyLiteralSymbol INSTANCE = new ContainsOrPointsToAnyLiteralSymbol();

        @Override
        protected Boolean visitSymbol(Symbol symbol, Void context) {
            return false;
        }

        @Override
        public Boolean visitLiteral(Literal symbol, Void context) {
            return true;
        }

        @Override
        public Boolean visitField(Field field, Void context) {
            return process(field.pointer(), context);
        }
    }
}
