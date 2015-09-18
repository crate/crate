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

package io.crate.planner.node.dql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.Planner;
import io.crate.planner.RowGranularity;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Symbols;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * A plan node which collects data.
 */
public class CollectPhase extends AbstractDQLPlanPhase implements UpstreamPhase {

    public static final ExecutionPhaseFactory<CollectPhase> FACTORY = new ExecutionPhaseFactory<CollectPhase>() {
        @Override
        public CollectPhase create() {
            return new CollectPhase();
        }
    };

    private Routing routing;
    private List<Symbol> toCollect;
    private DistributionType distributionType;
    private WhereClause whereClause = WhereClause.MATCH_ALL;
    private RowGranularity maxRowGranularity = RowGranularity.CLUSTER;

    private boolean isPartitioned = false;
    private boolean keepContextForFetcher = false;

    private @Nullable Integer limit = null;
    private @Nullable OrderBy orderBy = null;

    protected CollectPhase() {
        super();
    }

    public CollectPhase(UUID jobId,
                        int executionNodeId,
                        String name,
                        Routing routing,
                        RowGranularity maxRowGranularity,
                        List<Symbol> toCollect,
                        List<Projection> projections,
                        WhereClause whereClause,
                        DistributionType distributionType) {
        super(jobId, executionNodeId, name, projections);
        this.whereClause = whereClause;
        this.routing = routing;
        this.maxRowGranularity = maxRowGranularity;
        this.toCollect = toCollect;
        this.distributionType = distributionType;
        Projection lastProjection = Iterables.getLast(projections, null);
        if (lastProjection == null) {
            outputTypes = Symbols.extractTypes(toCollect);
        } else {
            outputTypes = Symbols.extractTypes(lastProjection.outputs());
        }
    }

    @Override
    public Type type() {
        return Type.COLLECT;
    }

    /**
     * @return a set of node ids where this collect operation is executed,
     *         excluding the NULL_NODE_ID for special collect purposes
     */
    @Override
    public Set<String> executionNodes() {
        if (routing == null) {
            return ImmutableSet.of();
        }
        return routing.nodes();
    }

    @Override
    public DistributionType distributionType() {
        return distributionType;
    }

    @Override
    public void distributionType(DistributionType distributionType) {
        this.distributionType = distributionType;
    }

    public @Nullable Integer limit() {
        return limit;
    }

    public void limit(Integer limit) {
        this.limit = limit;
    }

    public @Nullable OrderBy orderBy() {
        return orderBy;
    }

    public void orderBy(@Nullable OrderBy orderBy) {
        this.orderBy = orderBy;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    public Routing routing() {
        return routing;
    }

    public List<Symbol> toCollect() {
        return toCollect;
    }

    public boolean isRouted() {
        return routing != null && routing.hasLocations();
    }

    public RowGranularity maxRowGranularity() {
        return maxRowGranularity;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitCollectNode(this, context);
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitCollectPhase(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        distributionType = DistributionType.values()[in.readVInt()];

        int numCols = in.readVInt();
        if (numCols > 0) {
            toCollect = new ArrayList<>(numCols);
            for (int i = 0; i < numCols; i++) {
                toCollect.add(Symbol.fromStream(in));
            }
        } else {
            toCollect = ImmutableList.of();
        }

        maxRowGranularity = RowGranularity.fromStream(in);

        if (in.readBoolean()) {
            routing = new Routing();
            routing.readFrom(in);
        }

        whereClause = new WhereClause(in);
        keepContextForFetcher = in.readBoolean();

        if( in.readBoolean()) {
            limit = in.readVInt();
        }

        if (in.readBoolean()) {
            orderBy = OrderBy.fromStream(in);
        }
        isPartitioned = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeVInt(distributionType.ordinal());

        int numCols = toCollect.size();
        out.writeVInt(numCols);
        for (Symbol aToCollect : toCollect) {
            Symbol.toStream(aToCollect, out);
        }

        RowGranularity.toStream(maxRowGranularity, out);

        if (routing != null) {
            out.writeBoolean(true);
            routing.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        whereClause.writeTo(out);

        out.writeBoolean(keepContextForFetcher);
        if (limit != null ) {
            out.writeBoolean(true);
            out.writeVInt(limit);
        } else {
            out.writeBoolean(false);
        }
        if (orderBy != null) {
            out.writeBoolean(true);
            OrderBy.toStream(orderBy, out);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(isPartitioned);
    }

    /**
     * normalizes the symbols of this node with the given normalizer
     *
     * @return a normalized node, if no changes occurred returns this
     */
    public CollectPhase normalize(EvaluatingNormalizer normalizer) {
        assert whereClause() != null;
        CollectPhase result = this;
        List<Symbol> newToCollect = normalizer.normalize(toCollect());
        boolean changed = newToCollect != toCollect();
        WhereClause newWhereClause = whereClause().normalize(normalizer);
        if (newWhereClause != whereClause()) {
            changed = changed || newWhereClause != whereClause();
        }
        if (changed) {
            result = new CollectPhase(
                    jobId(),
                    executionPhaseId(),
                    name(),
                    routing,
                    maxRowGranularity,
                    newToCollect,
                    projections,
                    newWhereClause,
                    distributionType
            );
            result.keepContextForFetcher = keepContextForFetcher;
        }
        return result;
    }

    public void keepContextForFetcher(boolean keepContextForFetcher) {
        this.keepContextForFetcher = keepContextForFetcher;
    }

    public boolean keepContextForFetcher() {
        return keepContextForFetcher;
    }

    public static CollectPhase forQueriedTable(Planner.Context plannerContext,
                                               QueriedTableRelation table,
                                               List<Symbol> toCollect,
                                               List<Projection> projections) {
        TableInfo tableInfo = table.tableRelation().tableInfo();
        WhereClause where = table.querySpec().where();
        Routing routing = tableInfo.getRouting(where, null);
        plannerContext.allocateJobSearchContextIds(routing);
        return new CollectPhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                "collect",
                routing,
                tableInfo.rowGranularity(),
                toCollect,
                projections,
                where,
                DistributionType.BROADCAST
        );
    }
}