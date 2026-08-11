/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.optimizer.rule;

import java.util.ArrayList;
import java.util.List;

import io.crate.data.Input;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.OuterColumn;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.VoidReference;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;

final class NullSymbolEvaluator extends SymbolVisitor<Void, Symbol> {

    private final TransactionContext txnCtx;
    private final NodeContext nodeCtx;

    public NullSymbolEvaluator(TransactionContext txnCtx, NodeContext nodeCtx) {
        this.txnCtx = txnCtx;
        this.nodeCtx = nodeCtx;
    }

    @Override
    public Symbol visitField(ScopedSymbol field, Void context) {
        return Literal.NULL;
    }

    @Override
    public Symbol visitReference(Reference symbol, Void context) {
        return Literal.NULL;
    }

    @Override
    public Symbol visitDynamicReference(DynamicReference symbol, Void context) {
        return visitReference(symbol, context);
    }

    @Override
    public Symbol visitVoidReference(VoidReference symbol, Void context) {
        return visitReference(symbol, context);
    }

    @Override
    public Symbol visitOuterColumn(OuterColumn outerColumn, Void context) {
        return Literal.NULL;
    }

    @Override
    public Symbol visitAlias(AliasSymbol aliasSymbol, Void context) {
        return aliasSymbol.symbol().accept(this, context);
    }

    @Override
    public Symbol visitLiteral(Literal<?> symbol, Void context) {
        return symbol;
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Void context) {
        // ParameterSymbol or SelectSymbol can have a different value than NULL,
        return symbol;
    }

    @Override
    public Symbol visitFunction(Function function, Void context) {
        List<Symbol> arguments = function.arguments();
        List<Symbol> newArguments = new ArrayList<>(arguments.size());
        boolean allResolved = true;
        boolean atLeastOneNull = false;
        for (int i = 0; i < arguments.size(); i++) {
            var newArg = arguments.get(i).accept(this, context);
            if (newArg == Literal.NULL) {
                atLeastOneNull = true;
            }
            if (!(newArg instanceof Input<?>)) {
                allResolved = false;
            }
            newArguments.add(newArg);
        }
        if (!allResolved) {
            if (atLeastOneNull) {
                if (function.signature().hasFeature(Scalar.Feature.STRICTNULL)) {
                    // Function is unresolved, but having at least one null argument is enough to make it NULL
                    return Literal.NULL;
                }
                if (AnyOperator.OPERATOR_NAMES.contains(function.name())) {
                    // ANY` isn't STRICTNULL, because it doesn't fit equivalence: arg null <==> f null
                    // But here we need only weaker implication  arg null ==> f null to resolve function to null.
                    return Literal.NULL;
                }
            }
            return new Function(function.signature(), newArguments, function.valueType(), function.filter());
        }

        // Fallback to BaseImplementationSymbolVisitor.visitFunction
        FunctionImplementation implementation = nodeCtx.functions().getQualified(function);
        if (!(implementation instanceof Scalar<?, ?> scalar)) {
            return function;
        }
        Scalar<?, ?> compiled = scalar.compile(arguments, txnCtx.sessionSettings().userName(), nodeCtx.roles());
        Input<?>[] inputs = new Input<?>[newArguments.size()];
        for (int i = 0; i < newArguments.size(); i++) {
            inputs[i] = (Input<?>) newArguments.get(i);
        }
        Object value = ((Scalar) compiled).evaluate(txnCtx, nodeCtx, inputs);
        return Literal.ofUnchecked(function.valueType(), value);
    }
}
