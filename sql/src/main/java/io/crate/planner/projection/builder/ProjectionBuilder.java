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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueryClause;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.symbol.*;
import io.crate.metadata.*;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.projectors.TopN;
import io.crate.planner.projection.*;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ProjectionBuilder {

    private static final InputCreatingVisitor inputVisitor = InputCreatingVisitor.INSTANCE;

    private final Functions functions;
    private final QuerySpec querySpec;

    public ProjectionBuilder(Functions functions, QuerySpec querySpec) {
        this.functions = functions;
        this.querySpec = querySpec;
    }

    public SplitPoints getSplitPoints() {
        // TODO: orderBy for none groups
        SplitPoints context = new SplitPoints(querySpec);
        SplitPointVisitor.INSTANCE.process(context);
        LeafVisitor.INSTANCE.process(context);
        return context;
    }


    public AggregationProjection aggregationProjection(Collection<? extends Symbol> inputs,
                                                       Collection<Function> aggregates,
                                                       Aggregation.Step fromStep,
                                                       Aggregation.Step toStep,
                                                       RowGranularity granularity) {
        InputCreatingVisitor.Context context = new InputCreatingVisitor.Context(inputs);
        ArrayList<Aggregation> aggregations = getAggregations(aggregates, fromStep, toStep, context);
        return new AggregationProjection(aggregations, granularity);
    }

    public GroupProjection groupProjection(
        Collection<Symbol> inputs,
        Collection<Symbol> keys,
        Collection<Function> values,
        Aggregation.Step fromStep,
        Aggregation.Step toStep,
        RowGranularity requiredGranularity) {

        InputCreatingVisitor.Context context = new InputCreatingVisitor.Context(inputs);
        ArrayList<Aggregation> aggregations = getAggregations(values, fromStep, toStep, context);
        return new GroupProjection(inputVisitor.process(keys, context), aggregations, requiredGranularity);
    }

    private ArrayList<Aggregation> getAggregations(Collection<Function> functions,
                                                   Aggregation.Step fromStep,
                                                   Aggregation.Step toStep,
                                                   InputCreatingVisitor.Context context) {
        ArrayList<Aggregation> aggregations = new ArrayList<>(functions.size());
        for (Function function : functions) {
            assert function.info().type() == FunctionInfo.Type.AGGREGATE :
                "function type must be " + FunctionInfo.Type.AGGREGATE;
            Aggregation aggregation;
            List<Symbol> aggregationInputs;
            if (fromStep == Aggregation.Step.PARTIAL) {
                aggregationInputs = ImmutableList.<Symbol>of(context.inputs.get(function));
            } else {
                // ITER means that there is no aggregation part upfront, therefore the input
                // symbols need to be in arguments
                aggregationInputs = inputVisitor.process(function.arguments(), context);
            }

            if (toStep == Aggregation.Step.PARTIAL) {
                FunctionIdent ident = function.info().ident();
                aggregation = Aggregation.partialAggregation(
                    function.info(),
                    ((AggregationFunction) this.functions.get(ident.schema(), ident.name(), ident.argumentTypes())).partialType(),
                    aggregationInputs
                );
            } else {
                aggregation = Aggregation.finalAggregation(function.info(), aggregationInputs, fromStep);
            }
            aggregations.add(aggregation);
        }
        return aggregations;
    }

    public static FilterProjection filterProjection(Collection<? extends Symbol> inputs, QueryClause queryClause) {
        InputCreatingVisitor.Context context = new InputCreatingVisitor.Context(inputs);
        Symbol query;
        if (queryClause.hasQuery()) {
            query = inputVisitor.process(queryClause.query(), context);
        } else if (queryClause.noMatch()) {
            query = Literal.BOOLEAN_FALSE;
        } else {
            query = Literal.BOOLEAN_TRUE;
        }
        List<Symbol> outputs = inputVisitor.process(inputs, context);
        return new FilterProjection(query, outputs);
    }

    /**
     * Create a {@link OrderedTopNProjection}, {@link TopNProjection} or {@link EvalProjection}.
     *
     * @param inputs Symbols which describe the inputs the projection will receive
     * @param outputs Symbols which describe the outputs.
     *                If these symbols differ from the inputs the projection will evaluate the rows to produce
     *                the desired outputs. (That is, evaluate functions or re-order the columns)
     */
    public static Projection topNOrEval(
        Collection<? extends Symbol> inputs,
        @Nullable OrderBy orderBy,
        int offset,
        int limit,
        @Nullable Collection<Symbol> outputs) {

        InputCreatingVisitor.Context context = new InputCreatingVisitor.Context(inputs);
        List<Symbol> inputsProcessed = inputVisitor.process(inputs, context);
        List<Symbol> outputsProcessed;
        if (outputs == null) {
            outputsProcessed = inputsProcessed;
        } else {
            outputsProcessed = inputVisitor.process(outputs, context);
        }

        if (orderBy == null) {
            if ( limit == TopN.NO_LIMIT && offset == 0) {
                return new EvalProjection(outputsProcessed);
            }
            return new TopNProjection(limit, offset, outputsProcessed);
        }
        return new OrderedTopNProjection(limit, offset, outputsProcessed,
            inputVisitor.process(orderBy.orderBySymbols(), context),
            orderBy.reverseFlags(),
            orderBy.nullsFirst());
    }

    /**
     * Create a {@link TopNProjection} or {@link EvalProjection} if required, otherwise null is returned.
     * <p>
     * The output symbols will consist of InputColumns.
     * </p>
     * @param numOutputs number of outputs this projection should have.
     *                   If inputTypes is longer this projection will cut off superfluous columns
     */
    @Nullable
    public static Projection topNOrEvalIfNeeded(Integer limit,
                                                int offset,
                                                int numOutputs,
                                                List<DataType> inputTypes) {
        if (limit == null) {
            limit = TopN.NO_LIMIT;
        }
        int numInputTypes = inputTypes.size();
        List<DataType> strippedInputs = inputTypes;
        if (numOutputs < numInputTypes) {
            strippedInputs = inputTypes.subList(0, numOutputs);
        }
        if (limit == TopN.NO_LIMIT && offset == 0) {
            if (numOutputs >= numInputTypes) {
                return null;
            }
            return new EvalProjection(InputColumn.fromTypes(strippedInputs));
        }
        return new TopNProjection(limit, offset, InputColumn.fromTypes(strippedInputs));
    }

    public static WriterProjection writerProjection(Collection<? extends Symbol> inputs,
                                                    Symbol uri,
                                                    @Nullable WriterProjection.CompressionType compressionType,
                                                    Map<ColumnIdent, Symbol> overwrites,
                                                    @Nullable List<String> outputNames,
                                                    WriterProjection.OutputFormat outputFormat) {
        InputCreatingVisitor.Context context = new InputCreatingVisitor.Context(inputs);

        return new WriterProjection(
            inputVisitor.process(inputs, context), uri, compressionType, overwrites, outputNames, outputFormat);
    }
}
