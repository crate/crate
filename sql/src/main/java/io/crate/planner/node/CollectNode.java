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
import com.google.common.collect.ImmutableSet;
import io.crate.metadata.Routing;
import io.crate.planner.RowGranularity;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A plan node which collects data.
 */
public class CollectNode extends PlanNode {

    private Routing routing;
    private List<Symbol> toCollect;
    private Function whereClause;
    private RowGranularity maxRowgranularity = RowGranularity.CLUSTER;
    private List<String> downStreamNodes;

    public CollectNode(String id) {
        super(id);
    }

    public CollectNode() {
        super();
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
        if (routing.hasLocations()) {
            return routing.locations().keySet();
        } else {
            return ImmutableSet.of();
        }
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

    public Function whereClause() {
        return whereClause;
    }

    public void whereClause(Function whereClause) {
        this.whereClause = whereClause;
    }

    public Routing routing() {
        return routing;
    }

    public void routing(Routing routing) {
        this.routing = routing;
    }

    public List<Symbol> toCollect() {
        return toCollect;
    }

    public void toCollect(List<Symbol> toCollect) {
        this.toCollect = toCollect;
    }

    public boolean isRouted() {
        return routing != null && routing.hasLocations();
    }

    public RowGranularity maxRowGranularity() {
        return maxRowgranularity;
    }

    public void maxRowGranularity(RowGranularity newRowGranularity) {
        if (maxRowgranularity.compareTo(newRowGranularity) < 0) {
            maxRowgranularity = newRowGranularity;
        }
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
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
        }

        if (in.readBoolean()) {
            routing = new Routing();
            routing.readFrom(in);
        }
        if (in.readBoolean()) {
            whereClause = new Function();
            whereClause.readFrom(in);
        }
        int numDownStreams = in.readVInt();
        downStreamNodes = new ArrayList<>(numDownStreams);
        for (int i = 0; i < numDownStreams; i++) {
            downStreamNodes.add(in.readString());
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

        if (routing != null) {
            out.writeBoolean(true);
            routing.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (whereClause != null) {
            out.writeBoolean(true);
            whereClause.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        if (hasDownstreams()) {
            out.writeVInt(downStreamNodes.size());
            for (String node : downStreamNodes) {
                out.writeString(node);
            }
        } else {
            out.writeVInt(0);
        }
    }

}
