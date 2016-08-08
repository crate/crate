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

import io.crate.analyze.symbol.*;
import io.crate.metadata.Reference;
import io.crate.operation.operator.Operators;
import io.crate.operation.predicate.NotPredicate;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *     Visitor to replace function symbols containing references with true literals.
 *
 * <br><br>
 *     <b>WARNING</b>: The function tree that is processed by this visitor should already
 *     have been normalized. If there are any literals still unresolved this won't work correctly!
 * </p>
 *
 * <h3>Example</h3>
 *
 * <pre>
 *     true and x = 1       -&gt; true and true
 * </pre>
 *
 * <pre>
 *     true and id is null  -&gt; true and true
 * </pre>
 */
public class ReferenceToTrueVisitor extends SymbolVisitor<Void, Symbol> {

    @Override
    public Symbol visitFunction(Function symbol, Void context) {
        String functionName = symbol.info().ident().name();

        if (functionName.equals(NotPredicate.NAME)) {
            Symbol argument = symbol.arguments().get(0);
            if (argument instanceof Reference) {
                return Literal.newLiteral(true);
            } else if (argument instanceof Function) {
                if (!Operators.LOGICAL_OPERATORS.contains(((Function) argument).info().ident().name())) {
                    return Literal.newLiteral(true);
                }
            }
        }
        if (Operators.LOGICAL_OPERATORS.contains(functionName)) {
            List<Symbol> newArgs = new ArrayList<>(symbol.arguments().size());
            for (Symbol arg : symbol.arguments()) {
                newArgs.add(process(arg, context));
            }
            return new Function(symbol.info(), newArgs);
        } else {
            return Literal.newLiteral(true);
        }
    }

    @Override
    public Symbol visitMatchPredicate(MatchPredicate matchPredicate, Void context) {
        return Literal.newLiteral(true);
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Void context) {
        return symbol;
    }
}
