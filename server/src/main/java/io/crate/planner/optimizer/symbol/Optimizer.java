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

package io.crate.planner.optimizer.symbol;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.AliasResolver;
import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.PlannerContext;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.planner.optimizer.symbol.rule.MoveArrayLengthOnReferenceCastToLiteralCastInsideOperators;
import io.crate.planner.optimizer.symbol.rule.MoveReferenceCastToLiteralCastOnAnyOperatorsWhenLeftIsReference;
import io.crate.planner.optimizer.symbol.rule.MoveReferenceCastToLiteralCastOnAnyOperatorsWhenRightIsReference;
import io.crate.planner.optimizer.symbol.rule.MoveSubscriptOnReferenceCastToLiteralCastInsideOperators;
import io.crate.planner.optimizer.symbol.rule.SimplifyEqualsOperationOnIdenticalReferences;
import io.crate.planner.optimizer.symbol.rule.SwapCastsInComparisonOperators;
import io.crate.planner.optimizer.symbol.rule.SwapCastsInLikeOperators;

public class Optimizer {

    private static final Logger LOGGER = LogManager.getLogger(Optimizer.class);
    private static final List<Rule<?>> RULES = List.of(
        new SwapCastsInComparisonOperators(),
        new SwapCastsInLikeOperators(),
        new MoveReferenceCastToLiteralCastOnAnyOperatorsWhenRightIsReference(),
        new MoveReferenceCastToLiteralCastOnAnyOperatorsWhenLeftIsReference(),
        new MoveSubscriptOnReferenceCastToLiteralCastInsideOperators(),
        new MoveArrayLengthOnReferenceCastToLiteralCastInsideOperators(),
        new SimplifyEqualsOperationOnIdenticalReferences()
    );

    public static Symbol optimizeCasts(Symbol query, PlannerContext plannerCtx) {
        return optimizeCasts(query, plannerCtx.transactionContext(), plannerCtx.nodeContext());
    }

    public static Symbol optimizeCasts(Symbol query, TransactionContext txnCtx, NodeContext nodeCtx) {
        Optimizer optimizer = new Optimizer(txnCtx, nodeCtx);
        return optimizer.optimize(query);
    }

    private final NodeContext nodeCtx;
    private final Visitor visitor = new Visitor();
    private FunctionLookup functionLookup;

    public Optimizer(TransactionContext txnCtx, NodeContext nodeCtx) {
        functionLookup = (f, args) -> {
            try {
                return ExpressionAnalyzer.allocateFunction(
                    f,
                    args,
                    null,
                    null,
                    txnCtx,
                    nodeCtx);
            } catch (ConversionException e) {
                return null;
            }
        };
        this.nodeCtx = nodeCtx;
    }

    public Symbol optimize(Symbol node) {
        return node.accept(visitor, null);
    }

    public Symbol tryApplyRules(Symbol node) {
        final boolean isTraceEnabled = LOGGER.isTraceEnabled();
        // Some rules may only become applicable after another rule triggered, so we keep
        // trying to re-apply the rules as long as at least one plan was transformed.
        boolean done = false;
        int numIterations = 0;
        while (!done && numIterations < 10_000) {
            done = true;
            for (Rule<?> rule : RULES) {
                Symbol transformed = tryMatchAndApply(rule, node, nodeCtx);
                if (transformed != null) {
                    if (isTraceEnabled) {
                        LOGGER.trace("Rule '{}' transformed the symbol", rule.getClass().getSimpleName());
                    }
                    node = transformed;
                    done = false;
                }
            }
            numIterations++;
        }
        assert numIterations < 10_000
            : "Optimizer reached 10_000 iterations safety guard. This is an indication of a broken rule that matches again and again";
        return node;
    }

    private <T> Symbol tryMatchAndApply(Rule<T> rule, Symbol node, NodeContext nodeCtx) {
        Match<T> match = rule.pattern().accept(node, Captures.empty());
        if (match.isPresent()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Rule '{}' matched", rule.getClass().getSimpleName());
            }
            return rule.apply(match.value(), match.captures(), nodeCtx, functionLookup, visitor.getParentFunction());
        }
        return null;
    }

    private class Visitor extends FunctionCopyVisitor<Void> {
        private Deque<io.crate.expression.symbol.Function> visitedFunctions = new ArrayDeque<>();

        @Override
        public Symbol visitFunction(io.crate.expression.symbol.Function symbol, Void context) {
            symbol = (io.crate.expression.symbol.Function) symbol.accept(AliasResolver.INSTANCE, null);
            visitedFunctions.push(symbol);
            var maybeTransformedSymbol = tryApplyRules(symbol);
            if (symbol.equals(maybeTransformedSymbol) == false) {
                visitedFunctions.pop();
                return maybeTransformedSymbol;
            }
            var sym = super.visitFunction(symbol, context);
            visitedFunctions.pop();
            return sym;
        }

        public Symbol getParentFunction() {
            if (visitedFunctions.size() < 2) {
                return null;
            }
            var current = visitedFunctions.pop();
            var parent = visitedFunctions.peek();
            visitedFunctions.push(current);
            return parent;
        }
    }
}
