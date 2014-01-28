/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.operator.InputCollectExpression;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.aggregation.FunctionExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.plan.PlanNode;
import io.crate.planner.symbol.*;
import org.cratedb.sql.CrateException;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * convert Symbols into Inputs to simply evaluate and CollectExpressions
 * that might be treated in a special way (TODO how)
 */
public class ImplementationSymbolVisitor extends SymbolVisitor<ImplementationSymbolVisitor.Context, Input<?>>{

    public static class Context {
        private Set<CollectExpression<?>> collectExpressions = new LinkedHashSet<>(); // to keep insertion order
        private List<Input<?>> topLevelInputs = new ArrayList<>();
        private RowGranularity maxGranularity = RowGranularity.CLUSTER;
        private List<Reference> higherGranularityReferences = new ArrayList<>();

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

        public Set<CollectExpression<?>> collectExpressions() {
            return collectExpressions;
        }

        public Input<?>[] topLevelInputs() {
            return topLevelInputs.toArray(new Input<?>[topLevelInputs.size()]);
        }

        public CollectExpression<?> handleHigherGranularityReference(Reference shardLevelReference) {
            InputCollectExpression<?> expr = new InputCollectExpression<>(higherGranularityReferences.size());
            collectExpressions.add(expr);
            // order matters, this has to be after creating expr
            higherGranularityReferences.add(shardLevelReference);
            return expr;
        }

        /**
         * list of references that
         * @return
         */
        public Reference[] higherGranularityReferences() {
            return higherGranularityReferences.toArray(new Reference[higherGranularityReferences.size()]);
        }
    }

    private final ReferenceResolver referenceResolver;
    private final Functions functions;
    private final RowGranularity rowGranularity;


    public ImplementationSymbolVisitor(ReferenceResolver referenceResolver, Functions functions, RowGranularity rowGranularity) {
        this.referenceResolver = referenceResolver;
        this.functions = functions;
        this.rowGranularity = rowGranularity;
    }

    public Context process(PlanNode node) {
        Context context = new Context();
        if (node.outputs() != null) {
            for (Symbol symbol : node.outputs()) {
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
        ReferenceImplementation impl = referenceResolver.getImplementation(symbol.info().ident());
        if (rowGranularity.compareTo(symbol.info().granularity()) >= 0) {
            if (impl != null && impl instanceof Input<?>) {
                context.setMaxGranularity(symbol.info().granularity());
                // collect collectExpressions separately
                if (impl instanceof CollectExpression<?>) {
                    context.collectExpressions.add((CollectExpression<?>) impl);
                }
                return (Input<?>)impl;
            } else {
                // same or lower granularity and not found means unknown
                throw new CrateException("Unknown Reference");
            }
        } else {
            // higher granularity
            context.setMaxGranularity(symbol.info().granularity());
            return context.handleHigherGranularityReference(symbol);
        }
    }

    @Override
    protected Input<?> visitSymbol(Symbol symbol, Context context) {
        if (symbol instanceof Literal<?, ?>) {
            return (Literal<?, ?>)symbol;
        }
        return super.visitSymbol(symbol, context);
    }
}
