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
import io.crate.operator.aggregation.NestedCollectExpression;
import io.crate.planner.plan.PlanNode;
import io.crate.planner.symbol.*;
import org.cratedb.sql.CrateException;
import org.elasticsearch.common.inject.Inject;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ImplementationSymbolVisitor extends SymbolVisitor<ImplementationSymbolVisitor.Context, Void>{

    public static class Context {
        private Set<Input<?>> leafs = new LinkedHashSet<>(); // to keep insertion order
        private List<CollectExpression<?>> topLevelOutputs = new ArrayList<>();

        private NestedCollectExpression<?> currentParentExpression = null;

        public void addLeaf(Input<?> inputLeaf) {
            leafs.add(inputLeaf);
        }

        public Input<?>[] leafs() {
            return leafs.toArray(new Input<?>[leafs.size()]);
        }

        public void add(CollectExpression<?> collectExpression) {
            if (currentParentExpression == null) {
                topLevelOutputs.add(collectExpression);
            } else {
                currentParentExpression.nestedExpressions.add(collectExpression);
            }
        }

        public NestedCollectExpression<?> currentParentExpression() {
            return currentParentExpression;
        }

        public void currentParentExpression(NestedCollectExpression<?> newParent) {
            currentParentExpression = newParent;
        }

        public CollectExpression<?>[] topLevelOutputs() {
            return topLevelOutputs.toArray(new CollectExpression<?>[topLevelOutputs.size()]);
        }

    }

    private final ReferenceResolver referenceResolver;
    private final Functions functions;


    @Inject
    public ImplementationSymbolVisitor(ReferenceResolver referenceResolver, Functions functions) {
        this.referenceResolver = referenceResolver;
        this.functions = functions;
    }

    public Context process(PlanNode node) {
        Context context = new Context();
        if (node.outputs() != null) {
            for (Symbol symbol : node.outputs()) {
                process(symbol, context);
            }
        }
        return context;
    }

    @Override
    public Void visitFunction(Function function, Context context) {
        final FunctionImplementation functionImplementation = functions.get(function.info().ident());
        if (functionImplementation != null && functionImplementation instanceof Scalar<?>) {

            NestedCollectExpression<Object> functionCollectExpression = new NestedCollectExpression<Object>() {
                @Override
                public boolean doSetNextRow(Object... args) {
                    return true;
                }

                @Override
                public Object value() {
                    return null;
                }
            };

            NestedCollectExpression<?> currentParent = context.currentParentExpression();
            context.currentParentExpression(functionCollectExpression);
            for (ValueSymbol argument : function.arguments()) {
                process(argument, context);
            }
            context.currentParentExpression(currentParent);

            // TODO: how to determine level?
            context.topLevelOutputs.add(new CollectExpression<Object>() {
                private Object[] args = new Object[0];

                @Override
                public Object value() {
                    return ((Scalar<?>) functionImplementation).evaluate(args);
                }

                @Override
                public boolean setNextRow(Object... args) {
                    this.args = args;
                    return true;
                }
            });
        } else {
            throw new CrateException("Unknown Function");
        }
        return null;
    }

    @Override
    public Void visitReference(Reference symbol, Context context) {
        ReferenceImplementation impl = referenceResolver.getImplementation(symbol.info().ident());
        if (impl != null && impl instanceof Input<?>) {
            context.add(new InputCollectExpression<>(context.leafs.size()));
            // references are always leafs
            context.addLeaf((Input<?>) impl);
        } else {
            throw new CrateException("Unknown Reference");
        }
        return null;
    }

    @Override
    public Void visitStringLiteral(StringLiteral literal, Context context) {
        final String literalValue = literal.value();
        context.add(new CollectExpression<Object>() {
            @Override
            public boolean setNextRow(Object... args) {
                return true;
            }

            @Override
            public Object value() {
                return literalValue;
            }
        });
        return null;
    }
}
