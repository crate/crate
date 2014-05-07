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

package io.crate.planner.node;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.Functions;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.planner.node.dql.AbstractDQLPlanNode;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;
import io.crate.DataType;
import io.crate.Streamer;
import io.crate.exceptions.CrateException;
import org.elasticsearch.common.inject.Inject;

import java.util.*;

/**
 * get input and output {@link io.crate.Streamer}s for {@link io.crate.planner.node.PlanNode}s
 */
public class PlanNodeStreamerVisitor extends PlanVisitor<PlanNodeStreamerVisitor.Context, Void> {

    public static class Context {
        private List<Streamer<?>> inputStreamers = new ArrayList<>();
        private List<Streamer<?>> outputStreamers = new ArrayList<>();

        public Streamer<?>[] inputStreamers() {
            return inputStreamers.toArray(new Streamer<?>[inputStreamers.size()]);
        }

        public Streamer<?>[] outputStreamers() {
            return outputStreamers.toArray(new Streamer<?>[outputStreamers.size()]);
        }
    }

    private final Functions functions;

    @Inject
    public PlanNodeStreamerVisitor(Functions functions) {
        this.functions = functions;
    }

    private AggregationStateStreamer getStreamer(AggregationFunction aggregationFunction) {
        return new AggregationStateStreamer(aggregationFunction);
    }

    private Streamer<?> resolveStreamer(Aggregation aggregation, Aggregation.Step step) {
        Streamer<?> streamer;
        AggregationFunction<?> aggFunction = (AggregationFunction<?>)functions.get(aggregation.functionIdent());
        if (aggFunction == null) {
            throw new CrateException("unknown aggregation function");
        }
        switch (step) {
            case ITER:
                assert aggFunction.info().ident().argumentTypes().size() == 1;
                streamer = aggFunction.info().ident().argumentTypes().get(0).streamer();
                break;
            case PARTIAL:
                streamer = getStreamer(aggFunction);
                break;
            case FINAL:
                streamer = aggFunction.info().returnType().streamer();
                break;
            default:
                throw new UnsupportedOperationException("step not supported");
        }
        return streamer;
    }

    public Context process(AbstractDQLPlanNode planNode) {
        Context ctx = new Context();
        process(planNode, ctx);
        return ctx;
    }

    @Override
    public Void visitCollectNode(CollectNode node, Context context) {
        // get aggregations, if any
        Optional<Projection> finalProjection = node.finalProjection();
        List<Aggregation> aggregations = ImmutableList.of();
        if (finalProjection.isPresent()) {
            if (finalProjection.get().projectionType() == ProjectionType.AGGREGATION) {
                aggregations = ((AggregationProjection)finalProjection.get()).aggregations();
            } else if (finalProjection.get().projectionType() == ProjectionType.GROUP) {
                aggregations = ((GroupProjection)finalProjection.get()).values();
            }
        }

        int aggIdx = 0;
        Aggregation aggregation;
        for (DataType outputType : node.outputTypes()) {
            if (outputType == null || outputType == DataType.NULL) {
                // get streamer for aggregation result
                try {
                    aggregation = aggregations.get(aggIdx);
                    if (aggregation != null) {
                        context.outputStreamers.add(resolveStreamer(aggregation, aggregation.toStep()));
                    }
                } catch (IndexOutOfBoundsException e) {
                    // assume this is an unknown column
                    context.outputStreamers.add(DataType.NULL.streamer());
                }
                aggIdx++;
            } else {
                // get streamer for outputType
                context.outputStreamers.add(outputType.streamer());
            }
        }
        return null;
    }

    @Override
    public Void visitMergeNode(MergeNode node, Context context) {
        if (node.projections().isEmpty()) {
            for (DataType dataType : node.inputTypes()) {
                if (dataType != null && dataType != DataType.NULL) {
                    context.inputStreamers.add(dataType.streamer());
                } else {
                    throw new IllegalStateException("Can't resolve Streamer from null dataType");
                }
            }
            return null;
        }

        Projection firstProjection = node.projections().get(0);
        setInputStreamers(node.inputTypes(), firstProjection, context);
        setOutputStreamers(node.outputTypes(), node.inputTypes(), node.projections(), context);

        return null;
    }

    private void setOutputStreamers(List<DataType> outputTypes,
                                    List<DataType> inputTypes,
                                    List<Projection> projections, Context context) {
        final Streamer<?>[] streamers = new Streamer[outputTypes.size()];

        int idx = 0;
        for (DataType outputType : outputTypes) {
            if (outputType == DataType.NULL) {
                resolveStreamer(streamers, projections, idx, projections.size() - 1, inputTypes);
            } else {
                streamers[idx] = outputType.streamer();
            }
            idx++;
        }

        for (Streamer<?> streamer : streamers) {
            if (streamer == null) {
                throw new IllegalStateException("Could not resolve all output streamers");
            }
        }
        Collections.addAll(context.outputStreamers, streamers);
    }

    /**
     * traverse the projections backward until a output type/streamer is found for each symbol in the last projection
     */
    private void resolveStreamer(Streamer<?>[] streamers,
                                 List<Projection> projections,
                                 int columnIdx,
                                 int projectionIdx,
                                 List<DataType> inputTypes) {
        final Projection projection = projections.get(projectionIdx);
        final Symbol symbol = projection.outputs().get(columnIdx);

        if (symbol instanceof ValueSymbol) {
            streamers[columnIdx] = ((ValueSymbol) symbol).valueType().streamer();
        } else if (symbol.symbolType() == SymbolType.AGGREGATION) {
            Aggregation aggregation = (Aggregation)symbol;
            streamers[columnIdx] = resolveStreamer(aggregation, aggregation.toStep());
        } else if (symbol.symbolType() == SymbolType.INPUT_COLUMN) {
            columnIdx = ((InputColumn)symbol).index();
            if (projectionIdx > 0) {
                projectionIdx--;
                resolveStreamer(streamers, projections, columnIdx, projectionIdx, inputTypes);
            } else {
                streamers[columnIdx] = inputTypes.get(((InputColumn)symbol).index()).streamer();
            }
        }
    }

    private void setInputStreamers(List<DataType> inputTypes, Projection projection, Context context) {
        List<Aggregation> aggregations;
        switch (projection.projectionType()) {
            case TOPN:
                aggregations = ImmutableList.of();
                break;
            case GROUP:
                aggregations = ((GroupProjection)projection).values();
                break;
            case AGGREGATION:
                aggregations = ((AggregationProjection)projection).aggregations();
                break;
            default:
                throw new UnsupportedOperationException("projectionType not supported");
        }

        int idx = 0;
        for (DataType inputType : inputTypes) {
            if (inputType != null && inputType != DataType.NULL) {
                context.inputStreamers.add(inputType.streamer());
            } else {
                Aggregation aggregation = aggregations.get(idx);
                context.inputStreamers.add(resolveStreamer(aggregation, aggregation.fromStep()));
                idx++;
            }
        }
    }
}
