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

import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.common.collections.Lists2;
import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.planner.PlannerContext;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.planner.optimizer.symbol.rule.MoveArrayLengthOnReferenceCastToLiteralCastInsideOperators;
import io.crate.planner.optimizer.symbol.rule.MoveReferenceCastToLiteralCastInsideOperators;
import io.crate.planner.optimizer.symbol.rule.MoveReferenceCastToLiteralCastOnAnyOperatorsWhenLeftIsReference;
import io.crate.planner.optimizer.symbol.rule.MoveReferenceCastToLiteralCastOnAnyOperatorsWhenRightIsReference;
import io.crate.planner.optimizer.symbol.rule.MoveSubscriptOnReferenceCastToLiteralCastInsideOperators;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class Optimizer {

    public static Symbol optimizeCasts(Symbol query, PlannerContext plannerContext) {
        Optimizer optimizer = new Optimizer(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            () -> plannerContext.clusterState().nodes().getMinNodeVersion(),
            List.of(
                MoveReferenceCastToLiteralCastInsideOperators::new,
                MoveReferenceCastToLiteralCastOnAnyOperatorsWhenRightIsReference::new,
                MoveReferenceCastToLiteralCastOnAnyOperatorsWhenLeftIsReference::new,
                MoveSubscriptOnReferenceCastToLiteralCastInsideOperators::new,
                MoveArrayLengthOnReferenceCastToLiteralCastInsideOperators::new
            )
        );
        return optimizer.optimize(query);
    }

    private static final Logger LOGGER = LogManager.getLogger(Optimizer.class);

    private final List<Rule<?>> rules;
    private final Supplier<Version> minNodeVersionInCluster;
    private final NodeContext nodeCtx;
    private final Visitor visitor = new Visitor();

    public Optimizer(CoordinatorTxnCtx coordinatorTxnCtx,
                     NodeContext nodeCtx,
                     Supplier<Version> minNodeVersionInCluster,
                     List<Function<FunctionSymbolResolver, Rule<?>>> rules) {
        FunctionSymbolResolver functionResolver =
            (f, args) -> {
                try {
                    return ExpressionAnalyzer.allocateFunction(
                        f,
                        args,
                        null,
                        null,
                        coordinatorTxnCtx,
                        nodeCtx);
                } catch (ConversionException e) {
                    return null;
                }
            };

        this.rules = Lists2.map(rules, r -> r.apply(functionResolver));
        this.minNodeVersionInCluster = minNodeVersionInCluster;
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
            Version minVersion = minNodeVersionInCluster.get();
            for (Rule rule : rules) {
                if (minVersion.before(rule.requiredVersion())) {
                    continue;
                }
                Match<?> match = rule.pattern().accept(node, Captures.empty());
                if (match.isPresent()) {
                    if (isTraceEnabled) {
                        LOGGER.trace("Rule '" + rule.getClass().getSimpleName() + "' matched");
                    }
                    Symbol transformedNode = rule.apply(match.value(), match.captures(), nodeCtx);
                    if (transformedNode != null) {
                        if (isTraceEnabled) {
                            LOGGER.trace("Rule '" + rule.getClass().getSimpleName() + "' transformed the symbol");
                        }
                        node = transformedNode;
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

    private class Visitor extends FunctionCopyVisitor<Void> {

        @Override
        public Symbol visitFunction(io.crate.expression.symbol.Function symbol, Void context) {
            var maybeTransformedSymbol = tryApplyRules(symbol);
            if (symbol.equals(maybeTransformedSymbol) == false) {
                return maybeTransformedSymbol;
            }
            return super.visitFunction(symbol, context);
        }
    }

}
