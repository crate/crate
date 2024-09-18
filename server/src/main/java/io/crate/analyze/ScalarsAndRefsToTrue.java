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

import io.crate.expression.operator.Operators;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
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
 *     true and id is null  -&gt; true and null IS NULL
 * </pre>
 */
public final class ScalarsAndRefsToTrue extends SymbolVisitor<Boolean, Symbol> {

    private static final ScalarsAndRefsToTrue INSTANCE = new ScalarsAndRefsToTrue();

    private ScalarsAndRefsToTrue() {}

    public static Symbol rewrite(Symbol symbol) {
        return symbol.accept(INSTANCE, Boolean.FALSE);
    }

    @Override
    public Symbol visitFunction(Function symbol, Boolean isNullPredicate) {
        String functionName = symbol.name();

        if (functionName.equals(IsNullPredicate.NAME)) {
            isNullPredicate = Boolean.TRUE;
        }
        if (functionName.equals(NotPredicate.NAME)) {
            Symbol argument = symbol.arguments().getFirst();
            if (argument instanceof Reference) {
                return argument.accept(this, isNullPredicate);
            } else if (argument instanceof Function fn) {
                if (!Operators.LOGICAL_OPERATORS.contains(fn.name())) {
                    Symbol processedOperand = argument.accept(this, isNullPredicate);
                    if (processedOperand.symbolType().isValueSymbol()) {
                        // Function could be replaced with Literal.BOOLEAN_TRUE to indicate a possible match.
                        // We don't apply NOT as we don't want 'true' to be negated.
                        // It's just a marker, so we return marker.
                        // I.e. if expression can match, then NOT(expression) can match as well.
                        return processedOperand;
                    } else {
                        // We don't have a marker, we got a Function which will be actually normalized and evaluated.
                        // It's important to apply NOT as it's part of the actual expression.
                        return new Function(symbol.signature(), List.of(processedOperand), symbol.valueType());
                    }
                }
            }
        }

        List<Symbol> newArgs = new ArrayList<>(symbol.arguments().size());
        boolean allLiterals = true;
        boolean hasNullArg = false;
        for (Symbol arg : symbol.arguments()) {
            Symbol processedArg = arg.accept(this, isNullPredicate);
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
        return new Function(symbol.signature(), newArgs, symbol.valueType());
    }

    @Override
    public Symbol visitMatchPredicate(MatchPredicate matchPredicate, Boolean isNullPredicate) {
        return Literal.BOOLEAN_TRUE;
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Boolean isNullPredicate) {
        if (isNullPredicate || symbol.valueType().id() == DataTypes.UNDEFINED.id()) {
            return Literal.NULL;
        }
        return Literal.BOOLEAN_TRUE;
    }

    @Override
    public Symbol visitLiteral(Literal<?> symbol, Boolean isNullPredicate) {
        return symbol;
    }
}
