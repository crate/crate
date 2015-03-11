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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.planner.RowGranularity;
import io.crate.planner.projection.Projection;

import java.util.ArrayList;
import java.util.List;

/**
 * a chain of connected projectors data is flowing through
 * with separate chains, zero or more chains to be executed on data shards
 * connected to the final chain of which there can be only one,
 * executed on a node.
 *
 * Currently only one projector can be executed on a shard
 *
 * <pre>
 * Data -> Shard chain 0 \
 *                        \
 * Data -> Shard chain 1 ----> Node chain --> Result
 *                        /
 * Data -> Shard chain 2 /
 * </pre>
 *
 * If there is no projector to be executed on a shard,
 * all data will go directly to the node chain.
 * <p>
 * Usage:
 *
 * <ul>
 * <li> construct one from a list of projections
 * <li> get a shard projector by calling {@linkplain #newShardDownstreamProjector(io.crate.operation.projectors.ProjectionToProjectorVisitor)}
 * from a shard context. do this for every shard you have
 * <li> call {@linkplain #startProjections()}
 * <li> feed data to the shard projectors
 */
public class ShardProjectorChain {

    private final List<Projection> projections;
    private final RamAccountingContext ramAccountingContext;
    protected final List<Projector> shardProjectors;
    protected final List<Projector> nodeProjectors;
    private Projector firstNodeProjector;
    private int shardProjectionsIndex = -1;


    public ShardProjectorChain(int numShards,
                               List<Projection> projections,
                               ProjectionToProjectorVisitor nodeProjectorVisitor,
                               RamAccountingContext ramAccountingContext) {
        this.projections = projections;
        this.ramAccountingContext = ramAccountingContext;
        assert projections.size() >0;
        nodeProjectors = new ArrayList<>();
        int idx = 0;
        for (Projection projection : projections) {
            if (projection.requiredGranularity() == RowGranularity.SHARD) {
                shardProjectionsIndex = idx;
                break; // we can quit here since currently
                // there can be only 1 projection on the shard
            }
            idx++;
        }

        Projector previousUpstream = null;
        // create the node level projectors
        for (int i = shardProjectionsIndex + 1; i < projections.size(); i++) {
            Projector projector = nodeProjectorVisitor.process(projections.get(i), ramAccountingContext);
            nodeProjectors.add(projector);
            if (previousUpstream != null) {
                previousUpstream.downstream(projector);
            } else {
                firstNodeProjector = projector;
            }
            previousUpstream = projector;
        }

        if (shardProjectionsIndex >= 0) {
            shardProjectors = new ArrayList<>((shardProjectionsIndex + 1) * numShards);
            // shardProjector will be created later
            if (firstNodeProjector == null) {
                firstNodeProjector = nodeProjectors.get(0);
            }
        } else {
            shardProjectors = ImmutableList.of();
        }
    }


    /**
     * Creates a new shard downstream chain if needed and returns a projector to be used as downstream
     * this method also calls startProjection on newly created shard level projectors.
     *
     * @param projectorVisitor the visitor to create projections out of a projection
     * @return a new projector connected to the internal chain
     */
    public Projector newShardDownstreamProjector(ProjectionToProjectorVisitor projectorVisitor) {
        if (shardProjectionsIndex < 0) {
            return firstNodeProjector;
        }
        Projector previousProjector = firstNodeProjector;
        Projector projector = null;
        for (int i = shardProjectionsIndex; i >= 0; i--) {
            projector = projectorVisitor.process(projections.get(i), ramAccountingContext);
            projector.downstream(previousProjector);
            shardProjectors.add(projector);
            previousProjector = projector;
        }
        return projector;
    }

    public void startProjections() {
        for (Projector projector : Lists.reverse(nodeProjectors)) {
            projector.startProjection();
        }
        if (shardProjectionsIndex >= 0) {
            for (Projector p : shardProjectors) {
                p.startProjection();
            }
        }
    }

    public Projector lastProjector() {
        return nodeProjectors.get(nodeProjectors.size());
    }

}
