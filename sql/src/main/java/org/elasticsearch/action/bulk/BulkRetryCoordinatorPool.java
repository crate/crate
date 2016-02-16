/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.HashMap;
import java.util.Map;

@Singleton
public class BulkRetryCoordinatorPool extends AbstractLifecycleComponent<BulkRetryCoordinatorPool> implements ClusterStateListener {

    private static final ESLogger LOGGER = Loggers.getLogger(BulkRetryCoordinatorPool.class);

    private final Map<String, BulkRetryCoordinator> coordinatorsByNodeId;
    private final Map<ShardId, BulkRetryCoordinator> coordinatorsByShardId;
    private final ClusterService clusterService;

    @Inject
    public BulkRetryCoordinatorPool(Settings settings,
                                    ClusterService clusterService) {
        super(settings);
        this.coordinatorsByShardId = new HashMap<>();
        this.coordinatorsByNodeId = new HashMap<>();
        this.clusterService = clusterService;
    }

    public BulkRetryCoordinator coordinator(ShardId shardId) throws IndexNotFoundException, ShardNotFoundException {
        synchronized (coordinatorsByShardId) {
            BulkRetryCoordinator coordinator = coordinatorsByShardId.get(shardId);
            if (coordinator == null) {
                IndexRoutingTable indexRoutingTable = clusterService.state().routingTable()
                        .index(shardId.getIndex());
                if (indexRoutingTable == null) {
                    throw new IndexNotFoundException("cannot find index " + shardId.index());
                }
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId.id());
                if (shardRoutingTable == null) {
                    throw new ShardNotFoundException(shardId);
                }
                String nodeId = shardRoutingTable.primaryShard().currentNodeId();
                // for currently unassigned shards the nodeId will be null
                // so we at some point we will always keep one coordinator around
                // for all the orphaned unassigned shards that might end up here
                // The requests for unassigned shards will be retried at the transport action level
                // so no problem here :)
                // wow, that is a long comment!
                synchronized (coordinatorsByNodeId) {
                    coordinator = coordinatorsByNodeId.get(nodeId);
                    if (coordinator == null) {
                        LOGGER.trace("create new coordinator for node {} and shard {}", nodeId, shardId);
                        coordinator = new BulkRetryCoordinator(settings);
                        coordinatorsByNodeId.put(nodeId, coordinator);
                    }
                }
                coordinatorsByShardId.put(shardId, coordinator);
            }
            return coordinator;
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        clusterService.addLast(this);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        clusterService.remove(this);
        synchronized (coordinatorsByNodeId) {
            for (BulkRetryCoordinator bulkRetryCoordinator : coordinatorsByNodeId.values()) {
                bulkRetryCoordinator.close();
            }
        }
        coordinatorsByShardId.clear();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        doStop();
        synchronized (coordinatorsByNodeId) {
            for (BulkRetryCoordinator bulkRetryCoordinator : coordinatorsByNodeId.values()) {
                bulkRetryCoordinator.shutdown();
            }
        }
        coordinatorsByNodeId.clear();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.routingTableChanged()) {
            coordinatorsByShardId.clear();
        }
        if (event.nodesRemoved()) {
            // stop retrycoordinators of removed nodes
            synchronized (coordinatorsByNodeId) {
                for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
                    BulkRetryCoordinator oldCoordinator = coordinatorsByNodeId.remove(node.id());
                    if (oldCoordinator != null) {
                        oldCoordinator.close();
                    }
                }
            }
        }
    }
}
