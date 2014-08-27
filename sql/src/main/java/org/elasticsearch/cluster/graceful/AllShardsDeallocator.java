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

package org.elasticsearch.cluster.graceful;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;

public class AllShardsDeallocator extends AbstractLifecycleComponent<AllShardsDeallocator> implements Deallocator, ClusterStateListener {

    public static final String CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID = "cluster.routing.allocation.exclude._id";

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final TransportClusterUpdateSettingsAction clusterUpdateSettingsAction;
    private final ClusterService clusterService;

    private final Object futureLock = new Object();
    private final Object excludeNodesLock = new Object();

    private String localNodeId;
    private volatile Set<String> deallocatingNodes;
    private volatile SettableFuture<DeallocationResult> waitForFullDeallocation = null;


    @Inject
    public AllShardsDeallocator(Settings settings, ClusterService clusterService, TransportClusterUpdateSettingsAction clusterUpdateSettingsAction) {
        super(settings);
        this.clusterService = clusterService;
        this.clusterUpdateSettingsAction = clusterUpdateSettingsAction;
        this.deallocatingNodes = Sets.newConcurrentHashSet();
    }

    public String localNodeId() {
        if (localNodeId == null) {
            localNodeId = clusterService.localNode().id();
        }
        return localNodeId;
    }

    /**
     * @see org.elasticsearch.cluster.graceful.Deallocator
     *
     * @return a future that is set when the node is fully decommissioned
     */
    @Override
    public ListenableFuture<DeallocationResult> deallocate() {
        RoutingNode node = clusterService.state().routingNodes().node(localNodeId());
        if (isDeallocating()) {
            throw new IllegalStateException("node already waiting for complete deallocation");
        }
        logger.debug("[{}] deallocating node ...", localNodeId());

        if (node.size() == 0) {
            return Futures.immediateFuture(DeallocationResult.SUCCESS_NOTHING_HAPPENED);
        }

        final SettableFuture<DeallocationResult> future = waitForFullDeallocation = SettableFuture.create();
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        synchronized (excludeNodesLock) {
            deallocatingNodes.add(localNodeId());
            request.transientSettings(ImmutableSettings.builder()
                    .put(CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID, StringUtils.COMMA_JOINER.join(deallocatingNodes))
                    .build());
        }
        clusterUpdateSettingsAction.execute(request, new ActionListener<ClusterUpdateSettingsResponse>() {
            @Override
            public void onResponse(ClusterUpdateSettingsResponse response) {
                logExcludedNodes(response.getTransientSettings());
                // future will be set when node has no shards
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("[{}] error disabling allocation", e, localNodeId());
                canceWithExceptionIfPresent(e);
            }
        });


        return future;
    }

    private void canceWithExceptionIfPresent(Throwable e) {
        synchronized (futureLock) {
            SettableFuture<DeallocationResult> future = waitForFullDeallocation;
            if (future != null) {
                future.setException(e);
            }
            waitForFullDeallocation = null;
        }
    }

    @Override
    public boolean cancel() {
        boolean cancelled = removeExclusion(localNodeId());
        canceWithExceptionIfPresent(new DeallocationCancelledException(localNodeId()));
        if (cancelled) {
            logger.debug("[{}] deallocation cancelled", localNodeId());
        } else {
            logger.debug("[{}] node not deallocating", localNodeId());
        }
        return cancelled;
    }

    @Override
    public boolean isDeallocating() {
        return waitForFullDeallocation != null || deallocatingNodes.contains(localNodeId());
    }

    /**
     * <ul>
     *     <li>remove exclusion for previously excluded nodeId that has been removed from the cluster
     *     <li>wait for excluded nodes to have all their shards moved
     * </ul>
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // apply new settings
        if (event.metaDataChanged()) {
            synchronized (excludeNodesLock) {
                deallocatingNodes = Strings.splitStringByCommaToSet(event.state().metaData().settings().get(CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID, ""));
            }
        }

        // remove removed nodes from deallocatingNodes list if we are master
        if (event.state().nodes().localNodeMaster()) {
            for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
                if (removeExclusion(node.id())) {
                    logger.trace("[{}] removed removed node {}", localNodeId(), node.id());
                }
            }
        }

        // set future for fully deallocated local node
        synchronized (futureLock) {
            SettableFuture<DeallocationResult> future = waitForFullDeallocation;
            if (future != null) {
                RoutingNode node = event.state().routingNodes().node(localNodeId());
                if (node.numberOfShardsWithState(ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING, ShardRoutingState.RELOCATING) == 0) {
                    logger.debug("[{}] deallocation successful.", localNodeId());
                    waitForFullDeallocation = null;
                    future.set(DeallocationResult.SUCCESS);
                } else if (logger.isTraceEnabled()) {
                    logger.trace("[{}] still {} started, {} initializing and {} relocating shards remaining",
                            localNodeId(),
                            node.numberOfShardsWithState(ShardRoutingState.STARTED),
                            node.numberOfShardsWithState(ShardRoutingState.INITIALIZING),
                            node.numberOfShardsWithState(ShardRoutingState.RELOCATING));
                }
            }
        }
    }

    /**
     * asynchronously remove exclusion for a node with id <code>nodeId</code> if it exists
     * @return true if the exclusion existed and will be removed
     */
    private boolean removeExclusion(final String nodeId) {
        synchronized (excludeNodesLock) {
            boolean removed = deallocatingNodes.remove(nodeId);
            if (removed) {
                ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
                request.transientSettings(ImmutableSettings.builder()
                        .put(CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID, StringUtils.COMMA_JOINER.join(deallocatingNodes))
                        .build());
                clusterUpdateSettingsAction.execute(request, new ActionListener<ClusterUpdateSettingsResponse>() {
                    @Override
                    public void onResponse(ClusterUpdateSettingsResponse response) {
                        logExcludedNodes(response.getTransientSettings());
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.error("[{}] error removing node '{}' from exclusion list", localNodeId(), nodeId, e);
                    }
                });
            }
            return removed;
        }
    }

    private void logExcludedNodes(Settings transientSettings) {
        logger.debug("[{}] excluded nodes now set to: {}", localNodeId(), transientSettings.get(CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID));
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        this.clusterService.add(this);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        this.clusterService.remove(this);
    }

    @Override
    protected void doClose() throws ElasticsearchException {

    }
}
