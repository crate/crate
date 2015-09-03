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
import io.crate.analyze.OrderBy;
import io.crate.breaker.RamAccountingContext;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.planner.RowGranularity;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;

import javax.annotation.Nullable;
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
 *  <li> call {@linkplain #startProjections(ExecutionState)}</li>
 *  <li> feed data to the shard projectors</li>
 * </ul>
 */
public class ShardProjectorChain {

    private final UUID jobId;
    private final RamAccountingContext ramAccountingContext;
    protected final List<Projector> shardProjectors;
    protected final List<Projector> nodeProjectors;
    private final List<? extends Projection> projections;
    private RowReceiver firstNodeProjector;
    private int shardProjectionsIndex = -1;

    private final RowDownstream rowDownstream;


    public static ShardProjectorChain sortedMerge(UUID jobId,
                                                  List<? extends Projection> projections,
                                                  int maxNumShards,
                                                  RowReceiver finalDownstream,
                                                  ProjectorFactory projectorFactory,
                                                  RamAccountingContext ramAccountingContext,
                                                  List<? extends Symbol> outputs,
                                                  OrderBy orderBy) {
        return new ShardProjectorChain(
                jobId,
                projections,
                maxNumShards,
                finalDownstream,
                projectorFactory,
                ramAccountingContext,
                outputs,
                orderBy
        );
    }

    public static ShardProjectorChain passThroughMerge(UUID jobId,
                                                       int maxNumShards,
                                                       List<? extends Projection> projections,
                                                       RowReceiver finalDownstream,
                                                       ProjectorFactory projectorFactory,
                                                       RamAccountingContext ramAccountingContext) {
        return new ShardProjectorChain(
                jobId,
                projections,
                maxNumShards,
                finalDownstream,
                projectorFactory,
                ramAccountingContext,
                null,
                null
        );
    }


    private ShardProjectorChain(UUID jobId,
                                List<? extends Projection> projections,
                                int maxNumShards,
                                RowReceiver finalDownstream,
                                ProjectorFactory projectorFactory,
                                RamAccountingContext ramAccountingContext,
                                @Nullable List<? extends Symbol>  outputs,
                                @Nullable OrderBy orderBy) {
        this.jobId = jobId;
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
        if (nodeProjectors.isEmpty()) {
            firstNodeProjector = finalDownstream;
        } else {
            nodeProjectors.get(nodeProjectors.size()-1).downstream(finalDownstream);
        }

        if (maxNumShards == 1) {
            rowDownstream = new SingleUpstreamRowDownstream(firstNodeProjector);
        } else if (orderBy == null || !orderBy.isSorted()) {
            rowDownstream = new SynchronizingPassThroughRowMerger(firstNodeProjector);
        } else {
            assert outputs != null : "must have outputs if orderBy is present";
            int[] orderByPositions = OrderByPositionVisitor.orderByPositions(orderBy.orderBySymbols(), outputs);
            int maxOrderByIndex = 0;
            for (int i = 0; i < orderByPositions.length; i++) {
                int pos = orderByPositions[i];
                if (pos > maxOrderByIndex) {
                    maxOrderByIndex = pos;
                }
            }
            int rowSize = Math.max(outputs.size(), maxOrderByIndex+1);
            rowDownstream = new BlockingSortingQueuedRowDownstream(firstNodeProjector, rowSize, orderByPositions, orderBy.reverseFlags(), orderBy.nullsFirst());
            //rowDownstream = new SortingRowMerger(firstNodeProjector, orderByPositions, orderBy.reverseFlags(), orderBy.nullsFirst());
        }

        if (shardProjectionsIndex >= 0) {
            // shardProjector will be created later
            shardProjectors = new ArrayList<>((shardProjectionsIndex + 1) * maxNumShards);
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

    public void startProjections(ExecutionState executionState) {
        for (Projector projector : Lists.reverse(nodeProjectors)) {
            projector.prepare(executionState);
        }
        if (shardProjectionsIndex >= 0) {
            for (Projector p : shardProjectors) {
                p.prepare(executionState);
            }
        }
    }

    public void fail(Throwable t) {
        if (firstNodeProjector == null) {
            return;
        }
        firstNodeProjector.fail(t);
    }

    public void finish() {
        if (firstNodeProjector != null) {
            firstNodeProjector.finish();
        }
    }
}
