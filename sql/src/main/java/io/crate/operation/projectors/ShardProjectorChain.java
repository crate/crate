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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.breaker.RamAccountingContext;
import io.crate.metadata.RowGranularity;
import io.crate.operation.RowDownstream;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * a chain of connected projectors data is flowing through
 * with separate chains, zero or more chains to be executed on data shards
 * connected to the final chain of which there can be only one,
 * executed on a node.
 *
 * Currently only one projector can be executed on a shard
 *
 *
 * <pre>
 * Data -&gt; Shard chain 0 \
 *                        \
 * Data -&gt; Shard chain 1 ----&gt; Node chain --&gt; Result
 *                        /
 * Data -&gt; Shard chain 2 /
 * </pre>
 *
 * If there is no projector to be executed on a shard,
 * all data will go directly to the node chain.
 * <p>
 * Usage:
 *
 * <ul>
 *  <li> construct one from a list of projections</li>
 *  <li> get a shard projector by calling {@linkplain #newShardDownstreamProjector(ProjectorFactory)}
 *       from a shard context. do this for every shard you have
 *  </li>
 *  <li> call {@linkplain #prepare()}</li>
 *  <li> feed data to the shard projectors</li>
 * </ul>
 */
public class ShardProjectorChain {

    private final static ESLogger LOGGER = Loggers.getLogger(ShardProjectorChain.class);

    private final UUID jobId;
    private final RowDownstream.Factory rowDownstreamFactory;
    private final RamAccountingContext ramAccountingContext;
    protected final List<Projector> shardProjectors;
    protected final List<Projector> nodeProjectors;
    private final RowReceiver finalDownstream;
    private final List<? extends Projection> projections;
    private RowReceiver firstNodeProjector;
    private int shardProjectionsIndex = -1;

    private final RowDownstream rowDownstream;

    public ShardProjectorChain(UUID jobId,
                               List<? extends Projection> projections,
                               RowDownstream.Factory rowDownstreamFactory,
                               RowReceiver finalDownstream,
                               ProjectorFactory projectorFactory,
                               RamAccountingContext ramAccountingContext) {
        this.jobId = jobId;
        this.rowDownstreamFactory = rowDownstreamFactory;
        this.ramAccountingContext = ramAccountingContext;
        this.projections = projections;
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
            Projector projector = projectorFactory.create(projections.get(i), ramAccountingContext, jobId);
            nodeProjectors.add(projector);
            if (previousUpstream != null) {
                previousUpstream.downstream(projector);
            } else {
                firstNodeProjector = projector;
            }
            previousUpstream = projector;
        }

        // register final downstream
        this.finalDownstream = finalDownstream;
        if (nodeProjectors.isEmpty()) {
            firstNodeProjector = finalDownstream;
        } else {
            nodeProjectors.get(nodeProjectors.size()-1).downstream(finalDownstream);
        }

        rowDownstream = rowDownstreamFactory.create(firstNodeProjector);

        if (shardProjectionsIndex >= 0) {
            // shardProjector will be created later
            shardProjectors = new ArrayList<>();
        } else {
            shardProjectors = ImmutableList.of();
        }
    }

    /**
     * Creates a new shard downstream chain if needed and returns a projector to be used as downstream
     * this method also calls startProjection on newly created shard level projectors.
     *
     * @return a new projector connected to the internal chain
     */
    public RowReceiver newShardDownstreamProjector(ProjectorFactory projectorFactory) {
        if (shardProjectionsIndex < 0) {
            return rowDownstream.newRowReceiver();
        }

        RowReceiver previousProjector = rowDownstream.newRowReceiver();
        Projector projector = null;
        for (int i = shardProjectionsIndex; i >= 0; i--) {
            projector = projectorFactory.create(projections.get(i), ramAccountingContext, jobId);
            projector.downstream(previousProjector);

            shardProjectors.add(projector);
            previousProjector = projector;
        }

        return projector;
    }

    public void prepare() {
        this.finalDownstream.prepare();
        for (Projector projector : Lists.reverse(nodeProjectors)) {
            projector.prepare();
        }
        if (shardProjectionsIndex >= 0) {
            for (Projector p : shardProjectors) {
                p.prepare();
            }
        }
    }

    public void fail(Throwable t) {
        if (firstNodeProjector == null) {
            return;
        }
        firstNodeProjector.fail(t);
    }
}
