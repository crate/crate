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

package io.crate.expression;

import com.google.common.base.Joiner;
import io.crate.data.Input;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;

import java.util.List;
import java.util.Locale;

public class BaseImplementationSymbolVisitor<C> extends SymbolVisitor<C, Input<?>> {

    private final TransactionContext txnCtx;
    protected final Functions functions;

    public BaseImplementationSymbolVisitor(TransactionContext txnCtx, Functions functions) {
        this.txnCtx = txnCtx;
        this.functions = functions;
    }

    @Override
    public Input<?> visitFunction(Function function, C context) {
        FunctionIdent ident = function.info().ident();
        Signature signature = function.signature();
        FunctionImplementation functionImplementation;
        if (signature == null) {
            functionImplementation = functions.getQualified(ident);
        } else {
            functionImplementation = functions.getQualified(signature, ident.argumentTypes());
        }
        assert functionImplementation != null : "Function implementation not found using full qualified lookup";

        if (functionImplementation instanceof Scalar<?, ?>) {
            List<Symbol> arguments = function.arguments();
            Scalar<?, ?> scalarImpl = ((Scalar) functionImplementation).compile(arguments);
            Input[] argumentInputs = new Input[arguments.size()];
            int i = 0;
            for (Symbol argument : function.arguments()) {
                argumentInputs[i++] = argument.accept(this, context);
            }
            return new FunctionExpression<>(txnCtx, scalarImpl, argumentInputs);
        } else {
            throw new UnsupportedFeatureException(
                String.format(
                    Locale.ENGLISH,
                    "Function %s(%s) is not a scalar function.",
                    ident.name(),
                    Joiner.on(", ").join(ident.argumentTypes())
                )
            );
        }
    }

    @Override
    public Input<?> visitLiteral(Literal symbol, C context) {
        return symbol;
    }

    @Override
    public Input<?> visitDynamicReference(DynamicReference symbol, C context) {
        return visitReference(symbol, context);
    }

    @Override
    public Input<?> visitAlias(AliasSymbol aliasSymbol, C context) {
        return aliasSymbol.symbol().accept(this, context);
    }

    @Override
    protected Input<?> visitSymbol(Symbol symbol, C context) {
        throw new UnsupportedOperationException(
            String.format(Locale.ENGLISH, "Can't handle Symbol [%s: %s]", symbol.getClass().getSimpleName(), symbol));
    }
}
