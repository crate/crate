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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.Aggregation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ProjectionToProjectorVisitor extends ProjectionVisitor<ProjectionToProjectorVisitor.Context, Projector> {

    public static class Context {
        List<Projector> projectors = new ArrayList<>();

        /**
         * add the projector to the list of projectors
         * and connect this projector to the last gathered one by setting it as downStream.
         *
         * @param projector
         */
        public void add(Projector projector) {
            if (!projectors.isEmpty()) {
                projectors.get(projectors.size() - 1).setDownStream(projector);
            }
            projectors.add(projector);
        }

        public List<Projector> projectors() {
            return projectors;
        }
    }

    private final ImplementationSymbolVisitor symbolVisitor;

    public List<Projector> process(Collection<Projection> projections) {
        Context ctx = new Context();
        process(projections, ctx);
        return ctx.projectors();
    }

    public ProjectionToProjectorVisitor(ImplementationSymbolVisitor symbolVisitor) {
        this.symbolVisitor = symbolVisitor;
    }

    @Override
    public Projector visitColumnProjection(ColumnProjection projection, Context context) {
        return super.visitColumnProjection(projection, context);
    }

    @Override
    public Projector visitTopNProjection(TopNProjection projection, Context context) {
        Projector projector;
        List<Input<?>> inputs = new ArrayList<>();
        List<CollectExpression<?>> collectExpressions = new ArrayList<>();

        ImplementationSymbolVisitor.Context ctx = symbolVisitor.process(projection.outputs());
        inputs.addAll(ctx.topLevelInputs());
        collectExpressions.addAll(ctx.collectExpressions());

        if (projection.isOrdered()) {
            int numOutputs = inputs.size();
            ImplementationSymbolVisitor.Context orderByCtx = symbolVisitor.process(projection.orderBy());

            // append orderby inputs to row, needed for sorting on them
            inputs.addAll(orderByCtx.topLevelInputs());
            collectExpressions.addAll(orderByCtx.collectExpressions());

            int[] orderByIndices = new int[inputs.size() - numOutputs];
            int idx = 0;
            for (int i = numOutputs; i < inputs.size(); i++) {
                orderByIndices[idx++] = i;
            }

            projector = new SortingTopNProjector(
                    inputs.toArray(new Input<?>[inputs.size()]),
                    collectExpressions.toArray(new CollectExpression[collectExpressions.size()]),
                    numOutputs,
                    orderByIndices,
                    projection.reverseFlags(),
                    projection.limit(),
                    projection.offset());
        } else {
            projector = new SimpleTopNProjector(
                    inputs.toArray(new Input<?>[inputs.size()]),
                    collectExpressions.toArray(new CollectExpression[collectExpressions.size()]),
                    projection.limit(),
                    projection.offset());
        }
        context.add(projector);
        return projector;
    }

    @Override
    public Projector visitGroupProjection(GroupProjection projection, Context context) {
        ImplementationSymbolVisitor.Context symbolContext = symbolVisitor.process(projection.keys());
        List<Input<?>> keyInputs = symbolContext.topLevelInputs();

        for (Aggregation aggregation : projection.values()) {
            symbolVisitor.process(aggregation, symbolContext);
        }
        Projector groupProjector = new GroupingProjector(
                keyInputs,
                ImmutableList.copyOf(symbolContext.collectExpressions()),
                symbolContext.aggregations()
        );
        context.add(groupProjector);
        return groupProjector;
    }

    @Override
    public Projector visitAggregationProjection(AggregationProjection projection, Context context) {
        ImplementationSymbolVisitor.Context symbolContext = new ImplementationSymbolVisitor.Context();
        for (Aggregation aggregation : projection.aggregations()) {
            symbolVisitor.process(aggregation, symbolContext);
        }
        Projector aggregationProjector = new AggregationProjector(
                symbolContext.collectExpressions(),
                symbolContext.aggregations());
        context.add(aggregationProjector);
        return aggregationProjector;
    }
}
