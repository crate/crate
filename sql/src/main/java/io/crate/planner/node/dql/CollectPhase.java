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
import com.google.common.collect.Sets;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
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
public class CollectPhase extends AbstractDQLPlanPhase {

    public static final ExecutionPhaseFactory<CollectPhase> FACTORY = new ExecutionPhaseFactory<CollectPhase>() {
        @Override
        public CollectPhase create() {
            return new CollectPhase();
        }
    };
    private Routing routing;
    private List<Symbol> toCollect;
    private WhereClause whereClause = WhereClause.MATCH_ALL;
    private RowGranularity maxRowGranularity = RowGranularity.CLUSTER;

    private boolean isPartitioned = false;
    private boolean keepContextForFetcher = false;
    private @Nullable String handlerSideCollect = null;

    private @Nullable Integer limit = null;
    private @Nullable OrderBy orderBy = null;

    protected CollectPhase() {
        super();
    }

    public CollectPhase(UUID jobId, int executionNodeId, String name, Routing routing, List<Symbol> toCollect, List<Projection> projections) {
        super(jobId, executionNodeId, name, projections);
        this.routing = routing;
        this.toCollect = toCollect;
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
        if (routing != null) {
            if (routing.isNullRouting()) {
                return routing.nodes();
            } else {
                return Sets.filter(routing.nodes(), TableInfo.IS_NOT_NULL_NODE_ID);
            }
        } else {
            return ImmutableSet.of();
        }
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

    public void whereClause(WhereClause whereClause) {
        assert whereClause != null;
        this.whereClause = whereClause;
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

    public void maxRowGranularity(RowGranularity newRowGranularity) {
        if (maxRowGranularity.compareTo(newRowGranularity) < 0) {
            maxRowGranularity = newRowGranularity;
        }
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
        handlerSideCollect = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        int numCols = toCollect.size();
        out.writeVInt(numCols);
        for (int i = 0; i < numCols; i++) {
            Symbol.toStream(toCollect.get(i), out);
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
        out.writeOptionalString(handlerSideCollect);
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
            result = new CollectPhase(jobId(), executionPhaseId(), name(), routing, newToCollect, projections);
            result.maxRowGranularity = maxRowGranularity;
            result.keepContextForFetcher = keepContextForFetcher;
            result.handlerSideCollect = handlerSideCollect;
            result.whereClause(newWhereClause);
        }
        return result;
    }

    public void keepContextForFetcher(boolean keepContextForFetcher) {
        this.keepContextForFetcher = keepContextForFetcher;
    }

    public boolean keepContextForFetcher() {
        return keepContextForFetcher;
    }

    public void handlerSideCollect(String handlerSideCollect) {
        this.handlerSideCollect = handlerSideCollect;
    }

    @Nullable
    public String handlerSideCollect() {
        return handlerSideCollect;
    }
}