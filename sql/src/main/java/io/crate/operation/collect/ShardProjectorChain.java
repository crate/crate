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

import com.google.common.collect.Lists;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.planner.RowGranularity;
import io.crate.planner.projection.Projection;

import java.util.ArrayList;
import java.util.List;

public class ShardProjectorChain {

    private final List<Projection> projections;
    private final List<Projector> shardProjectors;
    private final List<Projector> nodeProjectors;
    private Projector firstNodeProjector;
    private Projector lastProjector;
    private int shardProjectionsIndex = -1;


    public ShardProjectorChain(int numShards, List<Projection> projections, ProjectionToProjectorVisitor nodeProjectorVisitor) {
        this.projections = projections;
        nodeProjectors = new ArrayList<>();

        if (projections.size() == 0) {
            firstNodeProjector = lastProjector = new CollectingProjector();
            nodeProjectors.add(firstNodeProjector);
            shardProjectors = null;
            return;
        }

        int idx = 0;
        for (Projection projection : projections) {
            if (projection.requiredGranularity().ordinal() == RowGranularity.SHARD.ordinal()) {
                shardProjectionsIndex = idx;
                break;
            }
        }

        Projector previousProjector = null;
        for (int i = shardProjectionsIndex + 1; i < projections.size(); i++) {
            Projector projector = nodeProjectorVisitor.process(projections.get(i), null);
            nodeProjectors.add(projector);
            if (previousProjector != null) {
                previousProjector.downstream(projector);
            } else {
                firstNodeProjector = projector;
            }
            previousProjector = projector;
        }
        if (shardProjectionsIndex >= 0) {
            shardProjectors = new ArrayList<>((shardProjectionsIndex + 1) * numShards);
        } else {
            shardProjectors = null;
        }
        lastProjector = previousProjector;

        // TODO: once there is a plan to do a shard only projection chain, this case needs to be handled
        assert firstNodeProjector != null;
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
        for (int i = 0; i <= shardProjectionsIndex; i++) {
            projector = projectorVisitor.process(projections.get(i), null);
            previousProjector.downstream(projector);
            shardProjectors.add(projector);
            previousProjector = projector;
        }
        return projector;
    }

    public Object[][] result() {
        return lastProjector.getRows();
    }

    public Projector lastProjector() {
        return lastProjector;
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
}
