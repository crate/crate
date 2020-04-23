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

package io.crate.execution.dsl.projection.builder;

import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.TopNProjection;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ProjectionBuilder {

    private final Functions functions;

    public ProjectionBuilder(Functions functions) {
        this.functions = functions;
    }

    public AggregationProjection aggregationProjection(Collection<? extends Symbol> inputs,
                                                       Collection<Function> aggregates,
                                                       AggregateMode mode,
                                                       RowGranularity granularity) {
        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(inputs);
        ArrayList<Aggregation> aggregations = getAggregations(aggregates, mode, sourceSymbols);
        return new AggregationProjection(aggregations, granularity, mode);
    }

    public GroupProjection groupProjection(
        Collection<? extends Symbol> inputs,
        Collection<? extends Symbol> keys,
        Collection<Function> values,
        AggregateMode mode,
        RowGranularity requiredGranularity) {

        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(inputs);
        ArrayList<Aggregation> aggregations = getAggregations(values, mode, sourceSymbols);
        return new GroupProjection(InputColumns.create(keys, sourceSymbols), aggregations, mode, requiredGranularity);
    }

    private ArrayList<Aggregation> getAggregations(Collection<Function> functions,
                                                   AggregateMode mode,
                                                   InputColumns.SourceSymbols sourceSymbols) {
        ArrayList<Aggregation> aggregations = new ArrayList<>(functions.size());
        for (Function function : functions) {
            assert function.info().type() == FunctionInfo.Type.AGGREGATE :
                "function type must be " + FunctionInfo.Type.AGGREGATE;
            List<Symbol> aggregationInputs;
            Symbol filterInput;
            switch (mode) {
                case ITER_FINAL:
                case ITER_PARTIAL:
                    // ITER means that there is no aggregation part upfront, therefore the input
                    // symbols need to be in arguments
                    aggregationInputs = InputColumns.create(function.arguments(), sourceSymbols);
                    Symbol filter = function.filter();
                    if (filter != null) {
                        filterInput = InputColumns.create(filter, sourceSymbols);
                    } else {
                        filterInput = Literal.BOOLEAN_TRUE;
                    }
                    break;

                case PARTIAL_FINAL:
                    aggregationInputs = List.of(sourceSymbols.getICForSource(function));
                    filterInput = Literal.BOOLEAN_TRUE;
                    break;

                default:
                    throw new AssertionError("Invalid mode: " + mode.name());
            }

            FunctionIdent ident = function.info().ident();
            Signature signature = function.signature();
            AggregationFunction<?, ?> aggregationFunction;
            if (signature == null) {
                aggregationFunction = (AggregationFunction<?, ?>) this.functions.getQualified(ident);
            } else {
                aggregationFunction = (AggregationFunction<?, ?>) this.functions.getQualified(signature, ident.argumentTypes());
            }
            assert aggregationFunction != null :
                "Aggregation function implementation not found using full qualified lookup: " + function;

            Aggregation aggregation = new Aggregation(
                function.info(),
                signature,
                mode.returnType(aggregationFunction),
                aggregationInputs,
                filterInput
            );
            aggregations.add(aggregation);
        }
        return aggregations;
    }

    public static FilterProjection filterProjection(Collection<? extends Symbol> inputs, Symbol query) {
        // FilterProjection can only pass-through rows as is; create inputColumns which preserve the type:
        return new FilterProjection(InputColumns.create(query, inputs), InputColumn.mapToInputColumns(inputs));
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
            return new EvalProjection(InputColumn.mapToInputColumns(strippedInputs));
        }
        return new TopNProjection(limit, offset, strippedInputs);
    }

    public static WriterProjection writerProjection(Collection<? extends Symbol> inputs,
                                                    Symbol uri,
                                                    @Nullable WriterProjection.CompressionType compressionType,
                                                    Map<ColumnIdent, Symbol> overwrites,
                                                    @Nullable List<String> outputNames,
                                                    WriterProjection.OutputFormat outputFormat) {
        return new WriterProjection(
            InputColumn.mapToInputColumns(inputs), uri, compressionType, overwrites, outputNames, outputFormat);
    }
}
