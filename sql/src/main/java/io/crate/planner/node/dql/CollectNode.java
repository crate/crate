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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;
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
public class CollectNode extends AbstractDQLPlanNode {

    private Optional<UUID> jobId = Optional.absent();
    private Routing routing;
    private List<Symbol> toCollect;
    private WhereClause whereClause = WhereClause.MATCH_ALL;
    private RowGranularity maxRowGranularity = RowGranularity.CLUSTER;
    private List<String> downStreamNodes;
    private boolean isPartitioned = false;
    private boolean keepContextForFetcher = false;

    private @Nullable Integer limit = null;
    private @Nullable OrderBy orderBy = null;

    public CollectNode(String id) {
        super(id);
    }

    public CollectNode() {
        super();
    }

    public CollectNode(String id, Routing routing) {
        this(id, routing, ImmutableList.<Symbol>of(), ImmutableList.<Projection>of());
    }

    public CollectNode(String id, Routing routing, List<Symbol> toCollect, List<Projection> projections) {
        super(id);
        this.routing = routing;
        this.toCollect = toCollect;
        this.projections = projections;
    }

    /**
     * This method returns true if downstreams are defined, which means that results of this collect
     * operation should be sent to other nodes instead of being returned directly.
     */
    public boolean hasDownstreams() {
        return downStreamNodes != null && downStreamNodes.size() > 0;
    }

    public List<String> downStreamNodes() {
        return downStreamNodes;
    }

    public void downStreamNodes(List<String> downStreamNodes) {
        this.downStreamNodes = downStreamNodes;
    }

    @Override
    public Set<String> executionNodes() {
        if (routing != null && routing.hasLocations()) {
            return routing.locations().keySet();
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

    public void toCollect(List<Symbol> toCollect) {
        assert toCollect != null;
        this.toCollect = toCollect;
    }

    public boolean isRouted() {
        return routing != null && routing.hasLocations();
    }

    /**
     * Whether collect operates on a partitioned table.
     * Only used on {@link io.crate.operation.collect.HandlerSideDataCollectOperation},
     * so no serialization is needed.
     *
     * @return
     */
    public boolean isPartitioned() {
        return isPartitioned;
    }

    public void isPartitioned(boolean isPartitioned) {
        this.isPartitioned = isPartitioned;
    }

    public RowGranularity maxRowGranularity() {
        return maxRowGranularity;
    }

    public void maxRowGranularity(RowGranularity newRowGranularity) {
        if (maxRowGranularity.compareTo(newRowGranularity) < 0) {
            maxRowGranularity = newRowGranularity;
        }
    }

    public Optional<UUID> jobId() {
        return jobId;
    }

    public void jobId(@Nullable UUID jobId) {
        this.jobId = Optional.fromNullable(jobId);
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitCollectNode(this, context);
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

        int numDownStreams = in.readVInt();
        downStreamNodes = new ArrayList<>(numDownStreams);
        for (int i = 0; i < numDownStreams; i++) {
            downStreamNodes.add(in.readString());
        }
        if (in.readBoolean()) {
            jobId = Optional.of(new UUID(in.readLong(), in.readLong()));
        }
        keepContextForFetcher = in.readBoolean();

        if( in.readBoolean()) {
            limit = in.readVInt();
        }

        if (in.readBoolean()) {
            orderBy = OrderBy.fromStream(in);
        }
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

        if (hasDownstreams()) {
            out.writeVInt(downStreamNodes.size());
            for (String node : downStreamNodes) {
                out.writeString(node);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeBoolean(jobId.isPresent());
        if (jobId.isPresent()) {
            out.writeLong(jobId.get().getMostSignificantBits());
            out.writeLong(jobId.get().getLeastSignificantBits());
        }
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
    }

    /**
     * normalizes the symbols of this node with the given normalizer
     *
     * @return a normalized node, if no changes occurred returns this
     */
    public CollectNode normalize(EvaluatingNormalizer normalizer) {
        assert whereClause() != null;
        CollectNode result = this;
        List<Symbol> newToCollect = normalizer.normalize(toCollect());
        boolean changed = newToCollect != toCollect();
        WhereClause newWhereClause = whereClause().normalize(normalizer);
        if (newWhereClause != whereClause()) {
            changed = changed || newWhereClause != whereClause();
        }
        if (changed) {
            result = new CollectNode(id(), routing, newToCollect, projections);
            result.downStreamNodes = downStreamNodes;
            result.maxRowGranularity = maxRowGranularity;
            result.jobId = jobId;
            result.keepContextForFetcher = keepContextForFetcher;
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
}