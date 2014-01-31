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

import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.Input;
import io.crate.operator.InputCollectExpression;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.inject.Inject;

import java.util.ArrayList;
import java.util.List;

/**
 * able to handle inputs of higher granularity - creates {@link io.crate.operator.InputCollectExpression}s from them
 */
public class LenientImplementationSymbolVisitor extends ImplementationSymbolVisitor {

    public static class Context extends ImplementationSymbolVisitor.Context {

        private List<Reference> higherGranularityReferences = new ArrayList<>();

        public CollectExpression<?> handleHigherGranularityReference(Reference higherGranularityReference) {
            InputCollectExpression<?> expr = new InputCollectExpression<>(higherGranularityReferences.size());
            collectExpressions.add(expr);
            // order matters, this has to be after creating expr
            higherGranularityReferences.add(higherGranularityReference);
            return expr;
        }

        /**
         * array of references of higher granularity
         * @return
         */
        public Reference[] higherGranularityReferences() {
            return higherGranularityReferences.toArray(new Reference[higherGranularityReferences.size()]);
        }
    }

    @Override
    public Context process(CollectNode node) {
        return (Context)super.process(node);
    }

    @Override
    public Context process(Symbol... symbols) {
        return (Context)super.process(symbols);
    }

    @Inject
    public LenientImplementationSymbolVisitor(ReferenceResolver referenceResolver, Functions functions, RowGranularity rowGranularity) {
        super(referenceResolver, functions, rowGranularity);
    }

    @Override
    protected ImplementationSymbolVisitor.Context getContext() {
        // make sure we have the right kind of context
        return new Context();
    }

    /**
     * create InputCollectExpression from references with higher granularity,
     * expect them to be filled by collector using {@link io.crate.operator.InputCollectExpression#setNextRow(Object...)}.
     *
     * @param reference the reference of higher granularity
     * @param context the current context
     * @return the resulting {@link io.crate.operator.Input}
     */
    @Override
    protected Input<?> visitHigherGranularityReference(Reference reference, ImplementationSymbolVisitor.Context context) {
        context.setMaxGranularity(reference.info().granularity());
        return ((Context)context).handleHigherGranularityReference(reference);
    }
}
