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
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.operator.aggregation.AggregationState;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.ProjectionType;
import io.crate.planner.symbol.Aggregation;
import org.cratedb.DataType;
import org.cratedb.sql.CrateException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * get input and output {@link org.cratedb.DataType.Streamer}s for {@link io.crate.planner.node.PlanNode}s
 */
public class PlanNodeStreamerVisitor extends PlanVisitor<PlanNodeStreamerVisitor.Context, Void> {

    public static class Context {
        private List<DataType.Streamer<?>> inputStreamers = new ArrayList<>();
        private List<DataType.Streamer<?>> outputStreamers = new ArrayList<>();

        public DataType.Streamer<?>[] inputStreamers() {
            return inputStreamers.toArray(new DataType.Streamer<?>[inputStreamers.size()]);
        }

        public DataType.Streamer<?>[] outputStreamers() {
            return outputStreamers.toArray(new DataType.Streamer<?>[outputStreamers.size()]);
        }
    }

    private final Functions functions;

    @Inject
    public PlanNodeStreamerVisitor(Functions functions) {
        this.functions = functions;
    }

    /**
     * Streamer used for {@link io.crate.operator.aggregation.AggregationState}s
     */
    public class AggStateStreamer implements DataType.Streamer<AggregationState>{

        private final AggregationFunction aggregationFunction;

        public AggStateStreamer(AggregationFunction aggregationFunction) {
            this.aggregationFunction = aggregationFunction;
        }

        @Override
        public AggregationState<?> readFrom(StreamInput in) throws IOException {
            AggregationState<?> aggState = this.aggregationFunction.newState();
            aggState.readFrom(in);
            return aggState;
        }

        @Override
        public void writeTo(StreamOutput out, Object v) throws IOException {
            ((AggregationState<?>)v).writeTo(out);
        }
    }

    private AggStateStreamer getStreamer(AggregationFunction aggregationFunction) {
        return new AggStateStreamer(aggregationFunction);
    }

    private DataType.Streamer<?> resolveStreamer(Aggregation aggregation) {
        DataType.Streamer<?> streamer;
        AggregationFunction<?> aggFunction = (AggregationFunction<?>)functions.get(aggregation.functionIdent());
        if (aggFunction == null) {
            throw new CrateException("unknown aggregation function");
        }
        switch(aggregation.toStep()) {
            case PARTIAL:
                streamer = getStreamer(aggFunction);
                break;
            case FINAL:
                streamer = aggFunction.info().returnType().streamer();
                break;
            default:
                throw new CrateException(String.format("not supported aggregation step %s", aggregation.toStep().name()));
        }
        return streamer;
    }

    public Context process(PlanNode planNode) {
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
            if (outputType == null) {
                // get streamer for aggregation result
                try {
                    aggregation = aggregations.get(aggIdx);
                } catch (IndexOutOfBoundsException e) {
                    throw new CrateException("invalid output types on CollectNode");
                }
                if (aggregation != null) {
                    context.outputStreamers.add(resolveStreamer(aggregation));
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
        Optional<Projection> projection = Optional.absent();
        if (node.projections().size() > 0) {
            projection = Optional.of(node.projections().get(0));
        }
        List<Aggregation> aggregations = ImmutableList.of();
        if (projection.isPresent()) {
            if (projection.get().projectionType() == ProjectionType.AGGREGATION) {
                aggregations = ((AggregationProjection)projection.get()).aggregations();
            } else if (projection.get().projectionType() == ProjectionType.GROUP) {
                aggregations = ((GroupProjection)projection.get()).values();
            }
        }
        // switch on first projection
        int aggIdx = 0;
        Aggregation aggregation;
        for (DataType inputType : node.inputTypes()) {
            if (inputType == null) {
                try {
                    aggregation = aggregations.get(aggIdx);
                } catch(IndexOutOfBoundsException e) {
                    throw new CrateException("invalid input types on MergeNode");
                }
                if (aggregation != null) {
                    context.inputStreamers.add(resolveStreamer(aggregation));
                }
                aggIdx++;
            } else {
                context.inputStreamers.add(inputType.streamer());
            }
        }
        return null;
    }
}
