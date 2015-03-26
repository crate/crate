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
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.ExecutionNodeVisitor;
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
public class CollectNode extends AbstractDQLPlanNode implements ExecutionNode {

    private Optional<UUID> jobId = Optional.absent();
    private int jobLocalId = -1;
    private Routing routing;
    private List<Symbol> toCollect;
    private WhereClause whereClause = WhereClause.MATCH_ALL;
    private RowGranularity maxRowgranularity = RowGranularity.CLUSTER;
    @Nullable
    private List<String> downstreamNodes;
    private boolean isPartitioned = false;

    public CollectNode() {
        super();
    }

    public CollectNode(String name) {
        super(name);
    }

    public CollectNode(String name, Routing routing) {
        this(name, routing, ImmutableList.<Symbol>of(), ImmutableList.<Projection>of());
    }

    public CollectNode(@Nullable UUID jobId, int jobLocalId, String name) {
        super(name);
        this.jobId = Optional.fromNullable(jobId);
        this.jobLocalId = jobLocalId;
    }

    public CollectNode(String name, Routing routing, List<Symbol> toCollect, List<Projection> projections) {
        super(name);
        this.routing = routing;
        this.toCollect = toCollect;
        this.projections = projections;
    }


    @Override
    public Set<String> executionNodes() {
        if (routing != null && routing.hasLocations()) {
            return routing.locations().keySet();
        } else {
            return ImmutableSet.of();
        }
    }

    @Nullable
    public List<String> downstreamNodes() {
        return downstreamNodes;
    }

    public boolean hasDownstreams() {
        return downstreamNodes != null && downstreamNodes.size() > 0;
    }

    public void downstreamNodes(List<String> downStreamNodes) {
        this.downstreamNodes = downStreamNodes;
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
        return maxRowgranularity;
    }

    public void maxRowGranularity(RowGranularity newRowGranularity) {
        if (maxRowgranularity.compareTo(newRowGranularity) < 0) {
            maxRowgranularity = newRowGranularity;
        }
    }

    public Optional<UUID> jobId() {
        return jobId;
    }

    public void jobId(@Nullable UUID jobId) {
        this.jobId = Optional.fromNullable(jobId);
    }

    public int jobLocalId() {
        return jobLocalId;
    }

    public void jobLocalId(int jobLocalId) {
        this.jobLocalId = jobLocalId;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitCollectNode(this, context);
    }

    @Override
    public <C, R> R accept(ExecutionNodeVisitor<C, R> visitor, C context) {
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

        maxRowgranularity = RowGranularity.fromStream(in);

        if (in.readBoolean()) {
            routing = new Routing();
            routing.readFrom(in);
        }

        whereClause = new WhereClause(in);

        int numDownStreams = in.readVInt();
        downstreamNodes = new ArrayList<>(numDownStreams);
        for (int i = 0; i < numDownStreams; i++) {
            downstreamNodes.add(in.readString());
        }
        if (in.readBoolean()) {
            jobId = Optional.of(new UUID(in.readLong(), in.readLong()));
        }
        jobLocalId = in.readInt();
        isPartitioned = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        int numCols = toCollect.size();
        out.writeVInt(numCols);
        for (int i = 0; i < numCols; i++) {
            Symbol.toStream(toCollect.get(i), out);
        }

        RowGranularity.toStream(maxRowgranularity, out);

        if (routing != null) {
            out.writeBoolean(true);
            routing.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        whereClause.writeTo(out);

        if (downstreamNodes != null) {
            out.writeVInt(downstreamNodes.size());
            for (String downstreamNode : downstreamNodes) {
                out.writeString(downstreamNode);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeBoolean(jobId.isPresent());
        if (jobId.isPresent()) {
            out.writeLong(jobId.get().getMostSignificantBits());
            out.writeLong(jobId.get().getLeastSignificantBits());
        }
        out.writeInt(jobLocalId);
        out.writeBoolean(isPartitioned);
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
            result = new CollectNode(name(), routing, newToCollect, projections);
            result.downstreamNodes = downstreamNodes;
            result.maxRowgranularity = maxRowgranularity;
            result.jobId = jobId;
            result.jobLocalId = jobLocalId;
            result.isPartitioned(isPartitioned);
            result.whereClause(newWhereClause);
        }
        return result;
    }
}
