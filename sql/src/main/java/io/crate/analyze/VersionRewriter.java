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

import io.crate.metadata.ColumnIdent;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.Operators;
import io.crate.planner.symbol.*;

/**
 * Visitor which traverses functions to find _version columns and rewrites
 * the function to a `True` Literal.
 */
public class VersionRewriter {

    private static final Visitor visitor = new Visitor();

    public Symbol rewrite(Symbol query) {
        return visitor.process(query, null);
    }

    private static class Visitor extends SymbolVisitor<Void, Symbol> {

        @Override
        public Symbol visitFunction(Function function, Void context){
            String functionName = function.info().ident().name();
            if (Operators.LOGICAL_OPERATORS.contains(functionName)) {
                function = continueTraversal(function);
                return function;
            }
            if (functionName.equals(EqOperator.NAME)) {
                assert function.arguments().size() == 2;
                Symbol left = function.arguments().get(0);
                Symbol right = function.arguments().get(1);

                if (left.symbolType() != SymbolType.REFERENCE || !right.symbolType().isValueSymbol()) {
                    return function;
                }

                Reference reference = (Reference) left;
                ColumnIdent columnIdent = reference.info().ident().columnIdent();

                if (columnIdent.name().equals("_version")) {
                    return Literal.newLiteral(true);
                }
            }
            return function;
        }

        private Function continueTraversal(Function symbol) {
            int argumentsProcessed = 0;
            for (Symbol argument : symbol.arguments()) {
                Symbol argumentNew = process(argument, null);
                if (!argument.equals(argumentNew)) {
                    symbol.setArgument(argumentsProcessed, argumentNew);
                }
                argumentsProcessed++;
            }
            return symbol;
        }
    }
}
