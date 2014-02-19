/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operator.operations;

import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.operator.Input;
import io.crate.operator.aggregation.FunctionExpression;
import io.crate.planner.node.CollectNode;
import io.crate.planner.symbol.*;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractImplementationSymbolVisitor<C extends AbstractImplementationSymbolVisitor.Context>
        extends SymbolVisitor<C, Input<?>> {

    protected final Functions functions;

    protected abstract static class Context {

        protected ArrayList<Input<?>> topLevelInputs = new ArrayList<>();

        protected void add(Input<?> input) {
            topLevelInputs.add(input);
        }

        public ArrayList<Input<?>> topLevelInputs() {
            return topLevelInputs;
        }

    }

    protected abstract C newContext();

    public C process(CollectNode node) {
        C context = newContext();
        if (node.toCollect() != null) {
            for (Symbol symbol : node.toCollect()) {
                context.add(process(symbol, context));
            }
        }
        return context;
    }

    public C process(Symbol... symbols) {
        C context = newContext();
        for (Symbol symbol : symbols) {
            context.add(process(symbol, context));
        }
        return context;
    }

    public C process(List<Symbol> symbols) {
        C context = newContext();
        for (Symbol symbol : symbols) {
            context.add(process(symbol, context));
        }
        return context;
    }

    public AbstractImplementationSymbolVisitor(Functions functions) {
        this.functions = functions;
    }

    @Override
    public Input<?> visitFunction(Function function, C context) {
        final FunctionImplementation functionImplementation = functions.get(function.info().ident());
        if (functionImplementation != null && functionImplementation instanceof Scalar<?>) {

            List<Symbol> arguments = function.arguments();
            Input[] argumentInputs = new Input[arguments.size()];
            int i = 0;
            for (Symbol argument : function.arguments()) {
                argumentInputs[i++] = process(argument, context);
            }
            return new FunctionExpression<>((Scalar<?>) functionImplementation, argumentInputs);
        } else {
            throw new IllegalArgumentException(
                    String.format("Cannot find implementation for function %s", function));
        }
    }

    @Override
    public Input<?> visitStringLiteral(StringLiteral symbol, C context) {
        return symbol;
    }

    @Override
    public Input<?> visitDoubleLiteral(DoubleLiteral symbol, C context) {
        return symbol;
    }

    @Override
    public Input<?> visitBooleanLiteral(BooleanLiteral symbol, C context) {
        return symbol;
    }

    @Override
    public Input<?> visitIntegerLiteral(IntegerLiteral symbol, C context) {
        return symbol;
    }

    @Override
    public Input<?> visitNullLiteral(Null symbol, C context) {
        return symbol;
    }

    @Override
    public Input<?> visitLongLiteral(LongLiteral symbol, C context) {
        return symbol;
    }

    @Override
    public Input<?> visitFloatLiteral(FloatLiteral symbol, C context) {
        return symbol;
    }

    @Override
    protected Input<?> visitSymbol(Symbol symbol, C context) {
        throw new UnsupportedOperationException(String.format("Can't handle Symbol %s", symbol));
    }
}
