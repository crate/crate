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

import io.crate.metadata.*;
import io.crate.operator.Input;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.aggregation.FunctionExpression;
import io.crate.operator.reference.doc.CollectorExpression;
import io.crate.operator.reference.doc.DocLevelExpressions;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.CollectNode;
import io.crate.planner.symbol.*;
import org.cratedb.sql.CrateException;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * convert Symbols into Inputs for evaluation and CollectExpressions
 * that might be treated in a special way
 */
public class ImplementationSymbolVisitor extends SymbolVisitor<ImplementationSymbolVisitor.Context, Input<?>> {
    public static class Context {
        protected Set<CollectExpression<?>> collectExpressions = new LinkedHashSet<>(); // to keep insertion order
        protected List<Input<?>> topLevelInputs = new ArrayList<>();
        protected List<CollectorExpression<?>> docLevelExpressions = new ArrayList<>();
        protected RowGranularity maxGranularity = RowGranularity.CLUSTER;

        public RowGranularity maxGranularity() {
            return maxGranularity;
        }

        protected void setMaxGranularity(RowGranularity granularity) {
            if (granularity.ordinal() > this.maxGranularity.ordinal()) {
                this.maxGranularity = granularity;
            }
        }

        public void add(Input<?> input) {
            topLevelInputs.add(input);
        }

        public CollectorExpression<?>[] docLevelExpressions() {
            return docLevelExpressions.toArray(new CollectorExpression<?>[docLevelExpressions.size()]);
        }

        public Set<CollectExpression<?>> collectExpressions() {
            return collectExpressions;
        }

        public Input<?>[] topLevelInputs() {
            return topLevelInputs.toArray(new Input<?>[topLevelInputs.size()]);
        }
    }
    protected final ReferenceResolver referenceResolver;
    protected final Functions functions;
    protected final RowGranularity rowGranularity;


    public ImplementationSymbolVisitor(ReferenceResolver referenceResolver, Functions functions, RowGranularity rowGranularity) {
        this.referenceResolver = referenceResolver;
        this.functions = functions;
        this.rowGranularity = rowGranularity;
    }

    public Context process(CollectNode node) {
        Context context = new Context();
        if (node.toCollect() != null) {
            for (Symbol symbol : node.toCollect()) {
                context.add(process(symbol, context));
            }
        }
        return context;
    }

    public Context process(Symbol... symbols) {
        Context context = new Context();
        for (Symbol symbol : symbols) {
            context.add(process(symbol, context));
        }
        return context;
    }

    public Context process(List<Symbol> symbols) {
        Context context = new Context();
        for (Symbol symbol : symbols) {
            context.add(process(symbol, context));
        }
        return context;
    }

    @Override
    public Input<?> visitFunction(Function function, Context context) {
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
            throw new CrateException("Unknown Function");
        }
    }

    @Override
    public Input<?> visitReference(Reference symbol, Context context) {
        Input<?> result;

        if (symbol.info().granularity().ordinal() <= rowGranularity.ordinal()) {
            if (symbol.info().granularity() == RowGranularity.DOC) {
                CollectorExpression<?> docLevelExpression = DocLevelExpressions.getExpression(symbol.info());
                context.docLevelExpressions.add(docLevelExpression);
                result = docLevelExpression;
            } else {
                ReferenceImplementation impl = referenceResolver.getImplementation(symbol.info().ident());
                if (impl != null && impl instanceof Input<?>) {
                    // collect collectExpressions separately
                    if (impl instanceof CollectExpression<?>) {
                        context.collectExpressions.add((CollectExpression<?>) impl);
                    }
                    result = (Input<?>)impl;
                } else {
                    // same or lower granularity and not found means unknown
                    throw new CrateException("Unknown Reference");
                }
            }
            context.setMaxGranularity(symbol.info().granularity());
        } else {
            throw new CrateException(String.format("Cannot handle Reference %s", symbol.toString()));
        }
        return result;
    }

    @Override
    protected Input<?> visitSymbol(Symbol symbol, Context context) {
        if (symbol instanceof Literal<?, ?>) {
            return (Literal<?, ?>)symbol;
        }
        return super.visitSymbol(symbol, context);
    }
}
