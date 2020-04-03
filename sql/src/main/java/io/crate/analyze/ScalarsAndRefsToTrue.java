/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.expression.operator.Operators;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Visitor to replace all symbols beside of logical operators to true or null literals.
 * As a result only logical operators will remain with all arguments converted to literals.
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
 *     true and id is null  -&gt; true and true
 * </pre>
 */
public final class ScalarsAndRefsToTrue extends SymbolVisitor<Void, Symbol> {

    private static final ScalarsAndRefsToTrue INSTANCE = new ScalarsAndRefsToTrue();

    private ScalarsAndRefsToTrue() {
    }

    public static Symbol rewrite(Symbol symbol) {
        return symbol.accept(INSTANCE, null);
    }

    @Override
    public Symbol visitFunction(Function symbol, Void context) {
        String functionName = symbol.info().ident().name();

        if (functionName.equals(NotPredicate.NAME)) {
            Symbol argument = symbol.arguments().get(0);
            if (argument instanceof Reference) {
                return argument.accept(this, context);
            } else if (argument instanceof Function) {
                if (!Operators.LOGICAL_OPERATORS.contains(((Function) argument).info().ident().name())) {
                    return argument.accept(this, context);
                }
            }
        }

        List<Symbol> newArgs = new ArrayList<>(symbol.arguments().size());
        boolean allLiterals = true;
        boolean isNull = false;
        for (Symbol arg : symbol.arguments()) {
            Symbol processedArg = arg.accept(this, context);
            newArgs.add(processedArg);
            if (!processedArg.symbolType().isValueSymbol()) {
                allLiterals = false;
            }
            if (processedArg.valueType().id() == DataTypes.UNDEFINED.id()) {
                isNull = true;
            }
        }
        if (allLiterals && !Operators.LOGICAL_OPERATORS.contains(functionName)) {
            return isNull ? Literal.NULL : Literal.BOOLEAN_TRUE;
        }
        return new Function(symbol.info(), symbol.signature(), newArgs);
    }

    @Override
    public Symbol visitMatchPredicate(MatchPredicate matchPredicate, Void context) {
        return Literal.BOOLEAN_TRUE;
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Void context) {
        if (symbol.valueType().id() == DataTypes.UNDEFINED.id()) {
            return Literal.NULL;
        }
        return Literal.BOOLEAN_TRUE;
    }

    @Override
    public Symbol visitLiteral(Literal symbol, Void context) {
        return symbol;
    }
}
