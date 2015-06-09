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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.Streamer;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.metadata.Functions;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.ProjectionType;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.UndefinedType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * get input and output {@link io.crate.Streamer}s for {@link io.crate.planner.node.PlanNode}s
 */
@Singleton
public class StreamerVisitor {

    public static class Context {
        private List<Streamer<?>> inputStreamers = new ArrayList<>();
        private List<Streamer<?>> outputStreamers = new ArrayList<>();

        public Context() {
        }

        public Streamer<?>[] inputStreamers() {
            return inputStreamers.toArray(new Streamer<?>[inputStreamers.size()]);
        }

        public Streamer<?>[] outputStreamers() {
            return outputStreamers.toArray(new Streamer<?>[outputStreamers.size()]);
        }
    }

    private final Functions functions;
    private final PlanNodeStreamerVisitor planNodeStreamerVisitor;
    private final ExecutionNodeStreamerVisitor executionNodeStreamerVisitor;

    @Inject
    public StreamerVisitor(Functions functions) {
        this.functions = functions;
        this.planNodeStreamerVisitor = new PlanNodeStreamerVisitor();
        this.executionNodeStreamerVisitor = new ExecutionNodeStreamerVisitor();
    }

    public Context processPlanNode(PlanNode node) {
        Context context = new Context();
        planNodeStreamerVisitor.process(node, context);
        return context;
    }

    public Context processExecutionNode(ExecutionNode executionNode) {
        Context context = new Context();
        executionNodeStreamerVisitor.process(executionNode, context);
        return context;
    }

    private class PlanNodeStreamerVisitor extends PlanNodeVisitor<Context, Void> {

        @Override
        public Void visitCollectNode(CollectNode node, Context context) {
            extractFromCollectNode(node, context);
            return null;
        }

        @Override
        public Void visitMergeNode(MergeNode node, Context context) {
            extractFromMergeNode(node, context);
            return null;
        }
    }

    private class ExecutionNodeStreamerVisitor extends ExecutionNodeVisitor<Context, Void> {

        @Override
        public Void visitMergeNode(MergeNode node, Context context) {
            extractFromMergeNode(node, context);
            return null;
        }

        @Override
        public Void visitCollectNode(CollectNode collectNode, Context context) {
            extractFromCollectNode(collectNode, context);
            return null;
        }

        @Override
        public Void visitNestedLoopNode(NestedLoopNode node, Context context) {
            setOutputStreamers(node.outputTypes(), ImmutableList.<DataType>of(), node.projections(), context);
            return null;
        }

        @Override
        protected Void visitExecutionNode(ExecutionNode node, Context context) {
            throw new UnsupportedOperationException(String.format("Got unsupported ExecutionNode %s", node.getClass().getName()));
        }
    }


    private Streamer<?> resolveStreamer(Aggregation aggregation, Aggregation.Step step) {
        Streamer<?> streamer;
        AggregationFunction<?, ?> aggFunction = (AggregationFunction<?, ?>)functions.get(aggregation.functionIdent());
        if (aggFunction == null) {
            throw new ResourceUnknownException("unknown aggregation function");
        }
        switch (step) {
            case ITER:
                assert aggFunction.info().ident().argumentTypes().size() == 1;
                streamer = aggFunction.info().ident().argumentTypes().get(0).streamer();
                break;
            case PARTIAL:
                streamer = aggFunction.partialType().streamer();
                break;
            case FINAL:
                streamer = aggFunction.info().returnType().streamer();
                break;
            default:
                throw new UnsupportedOperationException("step not supported");
        }
        return streamer;
    }

    private void extractFromCollectNode(CollectNode node, Context context) {
        // get aggregations, if any
        List<Aggregation> aggregations = ImmutableList.of();
        List<Projection> projections = Lists.reverse(node.projections());
        for(Projection projection : projections){
            if (projection.projectionType() == ProjectionType.AGGREGATION) {
                aggregations = ((AggregationProjection)projection).aggregations();
                break;
            } else if (projection.projectionType() == ProjectionType.GROUP) {
                aggregations = ((GroupProjection)projection).values();
                break;
            }
        }


        int aggIdx = 0;
        Aggregation aggregation;
        for (DataType outputType : node.outputTypes()) {
            if (outputType == null || outputType == UndefinedType.INSTANCE) {
                // get streamer for aggregation result
                try {
                    aggregation = aggregations.get(aggIdx);
                    if (aggregation != null) {
                        context.outputStreamers.add(resolveStreamer(aggregation, aggregation.toStep()));
                    }
                } catch (IndexOutOfBoundsException e) {
                    // assume this is an unknown column
                    context.outputStreamers.add(UndefinedType.INSTANCE.streamer());
                }
                aggIdx++;
            } else {
                // get streamer for outputType
                context.outputStreamers.add(outputType.streamer());
            }
        }
    }

    private void extractFromMergeNode(MergeNode node, Context context) {
        if (node.projections().isEmpty()) {
            for (DataType dataType : node.inputTypes()) {
                if (dataType != null) {
                    context.inputStreamers.add(dataType.streamer());
                } else {
                    throw new IllegalStateException("Can't resolve Streamer from null dataType");
                }
            }
            return;
        }

        Projection firstProjection = node.projections().get(0);
        setInputStreamers(node.inputTypes(), firstProjection, context);
        setOutputStreamers(node.outputTypes(), node.inputTypes(), node.projections(), context);
    }

    private void setOutputStreamers(List<DataType> outputTypes,
                                    List<DataType> inputTypes,
                                    List<Projection> projections, Context context) {
        final Streamer<?>[] streamers = new Streamer[outputTypes.size()];

        int idx = 0;
        for (DataType outputType : outputTypes) {
            if (outputType == UndefinedType.INSTANCE) {
                resolveStreamer(streamers, projections, idx, projections.size() - 1, inputTypes);
                if (streamers[idx] == null) {
                    streamers[idx] = outputType.streamer();
                }
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

        if (!symbol.valueType().equals(DataTypes.UNDEFINED)) {
            streamers[columnIdx] = symbol.valueType().streamer();
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
            case FETCH:
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
            if (inputType != null && inputType != UndefinedType.INSTANCE) {
                context.inputStreamers.add(inputType.streamer());
            } else {
                Aggregation aggregation = aggregations.get(idx);
                context.inputStreamers.add(resolveStreamer(aggregation, aggregation.fromStep()));
                idx++;
            }
        }
    }
}
