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

package io.crate.expression.eval;

import io.crate.expression.operator.Operators;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;

/**
 * Inside a query, NULL values as logical operator arguments can be treated like boolean FALSE.
 * As a result, the query can mostly be optimized and turned into a lucene query.
 * Without this optimization chances are high that a genericFunctionFilter is used instead of a lucene query which
 * results in poor performance (table scan -> filter).
 * <p>
 * <pre>
 * Example:
 *
 *      NULL AND col1 = 1
 *
 *  can be handled as:
 *
 *      FALSE AND col1 = 1
 *
 *  which can be immediately normalized to:
 *
 *      FALSE -> NO-MATCH
 * </pre>
 *
 * @implNote If a NOT predicate is encountered inside the tree, the current boolean to replace a NULL must be inverted
 * for that leaf. Also traversing must be stopped if a conditional function is encountered as they can handle NULL
 * values in a concrete way.
 */
public final class NullEliminator {

    private static final Visitor VISITOR = new Visitor();

    /**
     * Eliminates NULLs inside the given query symbol if possible.
     * Also see {@link NullEliminator} class documentation for details.
     *
     * @param symbol The query symbol to operate on.
     * @param postProcessor A function applied only on function symbols which changed due to NULL replacement.
     */
    public static Symbol eliminateNullsIfPossible(Symbol symbol,
                                                  java.util.function.Function<Symbol, Symbol> postProcessor) {
        return symbol.accept(VISITOR, new Context(postProcessor));
    }

    private static class Context {
        private final java.util.function.Function<Symbol, Symbol> postProcessor;
        boolean insideLogicalOperator = false;
        boolean nullReplacement = false;

        public Context(java.util.function.Function<Symbol, Symbol> postProcessor) {
            this.postProcessor = postProcessor;
        }
    }

    private static class Visitor extends FunctionCopyVisitor<Context> {

        @Override
        public Symbol visitFunction(Function func, Context context) {
            String functionName = func.name();

            // only operate inside logical operators
            if (Operators.LOGICAL_OPERATORS.contains(functionName)) {
                final boolean currentNullReplacement = context.nullReplacement;
                final boolean currentInsideLogicalOperator = context.insideLogicalOperator;
                context.insideLogicalOperator = true;

                if (NotPredicate.NAME.equals(functionName)) {
                    // not(null) -> not(false) would evaluate to true, so replacement boolean must be flipped
                    context.nullReplacement = !currentNullReplacement;
                }
                Symbol newFunc = super.visitFunction(func, context);

                if (newFunc != func) {
                    newFunc = context.postProcessor.apply(newFunc);
                }

                // reset context
                context.insideLogicalOperator = currentInsideLogicalOperator;
                context.nullReplacement = currentNullReplacement;

                return newFunc;
            }

            return func;
        }

        @Override
        public Symbol visitLiteral(Literal<?> symbol, Context context) {
            if (context.insideLogicalOperator && symbol.value() == null) {
                return Literal.of(context.nullReplacement);
            }
            return symbol;
        }
    }

    private NullEliminator() {
    }
}
