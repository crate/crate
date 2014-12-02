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

package io.crate.operation;

import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;

import java.util.*;

/**
 * convert Symbols into Inputs for evaluation and CollectExpressions
 * that might be treated in a special way
 */
public class ImplementationSymbolVisitor extends
        AbstractImplementationSymbolVisitor<ImplementationSymbolVisitor.Context> {

    public static class Context extends AbstractImplementationSymbolVisitor.Context {

        protected Set<CollectExpression<?>> collectExpressions = new LinkedHashSet<>(); // to keep insertion order
        protected RowGranularity maxGranularity = RowGranularity.CLUSTER;
        protected List<AggregationContext> aggregations = new ArrayList<>();
        protected Map<InputColumn, InputCollectExpression> allocatedInputCollectionExpressions = new HashMap<>();

        public RowGranularity maxGranularity() {
            return maxGranularity;
        }

        private void setMaxGranularity(RowGranularity granularity) {
            if (granularity.ordinal() > this.maxGranularity.ordinal()) {
                this.maxGranularity = granularity;
            }
        }

        public Set<CollectExpression<?>> collectExpressions() {
            return collectExpressions;
        }

        public AggregationContext[] aggregations() {
            return aggregations.toArray(new AggregationContext[aggregations.size()]);
        }

        public Input<?> collectExpressionFor(InputColumn inputColumn) {
            InputCollectExpression<?> collectExpression = allocatedInputCollectionExpressions.get(inputColumn);
            if (collectExpression == null) {
                collectExpression = new InputCollectExpression<>(inputColumn.index());
                allocatedInputCollectionExpressions.put(inputColumn, collectExpression);
            }
            collectExpressions.add(collectExpression);
            return collectExpression;
        }
    }

    protected final ReferenceResolver referenceResolver;
    protected final RowGranularity rowGranularity;


    public ImplementationSymbolVisitor(ReferenceResolver referenceResolver, Functions functions,
                                       RowGranularity rowGranularity) {
        super(functions);
        this.referenceResolver = referenceResolver;
        this.rowGranularity = rowGranularity;
    }

    public ReferenceResolver referenceResolver() {
        return referenceResolver;
    }

    public RowGranularity rowGranularity() {
        return rowGranularity;
    }

    @Override
    protected Context newContext() {
        return new Context();
    }

    @Override
    public Input<?> visitInputColumn(InputColumn inputColumn, Context context) {
        return context.collectExpressionFor(inputColumn);
    }

    @Override
    public Input<?> visitReference(Reference symbol, Context context) {
        Input<?> result;
        if (!symbol.info().granularity().finerThan(rowGranularity)) {
            ReferenceImplementation impl = referenceResolver.getImplementation(symbol.info().ident());
            if (impl != null && impl instanceof Input<?>) {
                // collect collectExpressions separately
                if (impl instanceof CollectExpression<?>) {
                    context.collectExpressions.add((CollectExpression<?>) impl);
                }
                result = (Input<?>) impl;
            } else {
                // same or lower granularity and not found means unknown
                throw new UnhandledServerException(SymbolFormatter.format("Unknown Reference %s", symbol));
            }
            context.setMaxGranularity(symbol.info().granularity());
        } else {
            throw new IllegalArgumentException(SymbolFormatter.format("Cannot handle Reference %s", symbol));
        }
        return result;
    }

    @Override
    public Input<?> visitAggregation(Aggregation symbol, Context context) {
        FunctionImplementation impl = functions.get(symbol.functionIdent());
        if (impl == null) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("Can't load aggregation impl for symbol %s", symbol));
        }

        AggregationContext aggregationContext = new AggregationContext((AggregationFunction) impl, symbol);
        for (Symbol aggInput : symbol.inputs()) {
            aggregationContext.addInput(process(aggInput, context));
        }
        context.aggregations.add(aggregationContext);

        // can't generate an input from an aggregation.
        // since they cannot/shouldn't be nested this should be okay.
        return null;
    }
}
