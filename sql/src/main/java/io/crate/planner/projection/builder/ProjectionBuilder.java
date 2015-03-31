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

package io.crate.planner.projection.builder;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import io.crate.Constants;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ProjectionBuilder {

    private static final InputCreatingVisitor inputVisitor = InputCreatingVisitor.INSTANCE;

    private final QuerySpec querySpec;

    public ProjectionBuilder(QuerySpec querySpec) {
        this.querySpec = querySpec;
    }

    public SplitPoints getSplitPoints() {
        // TODO: orderBy for none groups
        SplitPoints context = new SplitPoints(querySpec);
        SplitPointVisitor.INSTANCE.process(context);
        LeafVisitor.INSTANCE.process(context);
        return context;
    }


    public AggregationProjection aggregationProjection(
            Collection<? extends Symbol> inputs,
            Collection<Function> aggregates,
            Aggregation.Step fromStep,
            Aggregation.Step toStep){
        InputCreatingVisitor.Context context = new InputCreatingVisitor.Context(inputs);
        ArrayList<Aggregation> aggregations = getAggregations(aggregates, fromStep,
                toStep, context);
        return new AggregationProjection(aggregations);
    }

    public GroupProjection groupProjection(
            Collection<Symbol> inputs,
            Collection<Symbol> keys,
            Collection<Function> values,
            Aggregation.Step fromStep,
            Aggregation.Step toStep){

        InputCreatingVisitor.Context context = new InputCreatingVisitor.Context(inputs);
        ArrayList<Aggregation> aggregations = getAggregations(values, fromStep, toStep, context);
        return new GroupProjection(inputVisitor.process(keys, context), aggregations);
    }

    private ArrayList<Aggregation> getAggregations(Collection<Function> functions,
                                                   Aggregation.Step fromStep,
                                                   Aggregation.Step toStep,
                                                   InputCreatingVisitor.Context context) {
        ArrayList<Aggregation> aggregations = new ArrayList(functions.size());
        for (Function function : functions) {
            assert function.info().type() == FunctionInfo.Type.AGGREGATE;
            Aggregation aggregation;
            if (fromStep == Aggregation.Step.PARTIAL){
                InputColumn ic = context.inputs.get(function);
                assert ic != null;
                aggregation = new Aggregation(
                        function.info(),
                        ImmutableList.<Symbol>of(ic),
                        fromStep, toStep);
            } else {
                // ITER means that there is no aggregation part upfront, therefore the input
                // symbols need to be in arguments
                aggregation = new Aggregation(
                        function.info(),
                        inputVisitor.process(function.arguments(), context),
                        fromStep, toStep);
            }
            aggregations.add(aggregation);

        }
        return aggregations;
    }

    public FilterProjection filterProjection(
            Collection<? extends Symbol> inputs,
            Symbol query) {
        InputCreatingVisitor.Context context = new InputCreatingVisitor.Context(inputs);
        query = inputVisitor.process(query, context);
        List<Symbol> outputs = inputVisitor.process(inputs, context);
        return new FilterProjection(query, outputs);
    }

    public TopNProjection topNProjection(
            Collection<? extends Symbol> inputs,
            @Nullable OrderBy orderBy,
            int offset,
            @Nullable Integer limit,
            @Nullable Collection<Symbol> outputs) {
        limit = MoreObjects.firstNonNull(limit, Constants.DEFAULT_SELECT_LIMIT);

        InputCreatingVisitor.Context context = new InputCreatingVisitor.Context(inputs);
        List<Symbol> inputsProcessed = inputVisitor.process(inputs, context);
        List<Symbol> outputsProcessed;
        if (outputs == null){
            outputsProcessed = inputsProcessed;
        } else {
            outputsProcessed = inputVisitor.process(outputs, context);
        }

        TopNProjection result;
        if (orderBy == null) {
            result = new TopNProjection(limit, offset);
        } else {
            result = new TopNProjection(limit, offset,
                    inputVisitor.process(orderBy.orderBySymbols(), context),
                    orderBy.reverseFlags(),
                    orderBy.nullsFirst());
        }
        result.outputs(outputsProcessed);
        return result;
    }

    public MergeProjection mergeProjection(
            List<Symbol> inputs,
            OrderBy orderBy
    ) {
        InputCreatingVisitor.Context context = new InputCreatingVisitor.Context(inputs);
        return new MergeProjection(inputs,
                inputVisitor.process(orderBy.orderBySymbols(), context),
                orderBy.reverseFlags(),
                orderBy.nullsFirst());
    }

}
