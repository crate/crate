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

package io.crate.analyze;

import java.util.ArrayList;
import java.util.List;

import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.Operators;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;

/**
 * <p>
 * Visitor to replace all symbols beside of logical operators and {@code IS NULL} predicate to true or null literals.
 * As a result only logical operators and {@code IS NULL} predicate will remain with all
 * arguments converted to literals.
 * <p>
 * <br><br>
 * <b>WARNING</b>: The function tree that is processed by this visitor should already
 * have been normalized. If there are any literals still unresolved this won't work correctly!
 * </p>
 * <p>
 * <h3>Example</h3>
 * <p>
 * <pre>
 *     true and x = 1       -&gt; true and true
 * </pre>
 * <p>
 * <pre>
 *     true and id is null  -&gt; true and null IS NULL -&gt;  true and true -&gt; true
 * </pre>
 */
public final class ScalarsAndRefsToTrue extends SymbolVisitor<ScalarsAndRefsToTrue.Context, Symbol> {

    private static final ScalarsAndRefsToTrue INSTANCE = new ScalarsAndRefsToTrue();

    private ScalarsAndRefsToTrue() {}

    public static class Context {
        private final EvaluatingNormalizer normalizer;
        private final TransactionContext txnCtx;
        private boolean isNullPredicate;

        private Context(EvaluatingNormalizer normalizer, TransactionContext txnCtx) {
            this.normalizer = normalizer;
            this.txnCtx = txnCtx;
            this.isNullPredicate = false;
        }
    }

    public static Symbol rewrite(EvaluatingNormalizer normalizer, TransactionContext txnCtx, Symbol symbol) {
        return symbol.accept(INSTANCE, new ScalarsAndRefsToTrue.Context(normalizer, txnCtx));
    }

    @Override
    public Symbol visitFunction(Function symbol, ScalarsAndRefsToTrue.Context ctx) {
        String functionName = symbol.name();

        if (functionName.equals(IsNullPredicate.NAME)) {
            ctx.isNullPredicate = true;
        }

        List<Symbol> newArgs = new ArrayList<>(symbol.arguments().size());
        boolean allLiterals = true;
        boolean hasNullArg = false;
        for (Symbol arg : symbol.arguments()) {
            Symbol processedArg = arg.accept(this, ctx);
            newArgs.add(processedArg);
            if (!processedArg.symbolType().isValueSymbol()) {
                allLiterals = false;
            }
            if (processedArg.valueType().id() == DataTypes.UNDEFINED.id()) {
                hasNullArg = true;
            }
        }

        if (allLiterals
            && !Operators.LOGICAL_OPERATORS.contains(functionName)
            && !IsNullPredicate.NAME.equals(functionName)) {
            return hasNullArg ? Literal.NULL : Literal.BOOLEAN_TRUE;
        }
        if (functionName.equals(NotPredicate.NAME)) {
            Symbol arg = newArgs.get(0);
            assert arg instanceof Literal<?> : "argument of NOT should have been normalized to literal";
            return arg;
        }

        // Without normalization, IS NULL inside an expression stays as a function
        // and therefore later on `allLiterals` is false, thus an outer function cannot be resolved
        // to a literal, stays as is, and is normalized, outside of this class in
        // WhereClauseAnalyzer#tieBreakPartitionQueries(), possibly to `false`, which in turn is not
        // inverted, since `NOT` operators are dropped completely.
        return ctx.normalizer.normalize(new Function(symbol.signature(), newArgs, symbol.valueType()), ctx.txnCtx);
    }

    @Override
    public Symbol visitMatchPredicate(MatchPredicate matchPredicate, ScalarsAndRefsToTrue.Context ctx) {
        return Literal.BOOLEAN_TRUE;
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, ScalarsAndRefsToTrue.Context ctx) {
        if (ctx.isNullPredicate || symbol.valueType().id() == DataTypes.UNDEFINED.id()) {
            return Literal.NULL;
        }
        return Literal.BOOLEAN_TRUE;
    }

    @Override
    public Symbol visitLiteral(Literal<?> symbol, ScalarsAndRefsToTrue.Context ctx) {
        return symbol;
    }
}
