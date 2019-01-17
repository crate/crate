/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.store;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IndicesStore extends AbstractComponent implements ClusterStateListener, Closeable {

    // TODO this class can be foled into either IndicesService and partially into IndicesClusterStateService there is no need for a separate public service
    public static final Setting<TimeValue> INDICES_STORE_DELETE_SHARD_TIMEOUT =
        Setting.positiveTimeSetting("indices.store.delete.shard.timeout", new TimeValue(30, TimeUnit.SECONDS),
            Property.NodeScope);
    public static final String ACTION_SHARD_EXISTS = "internal:index/shard/exists";
    private static final EnumSet<IndexShardState> ACTIVE_STATES = EnumSet.of(IndexShardState.STARTED);
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final ThreadPool threadPool;

    // Cache successful shard deletion checks to prevent unnecessary file system lookups
    private final Set<ShardId> folderNotFoundCache = new HashSet<>();

    private TimeValue deleteShardTimeout;

    @Inject
    public IndicesStore(Settings settings, IndicesService indicesService,
                        ClusterService clusterService, TransportService transportService, ThreadPool threadPool) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;
        transportService.registerRequestHandler(ACTION_SHARD_EXISTS, ShardActiveRequest::new, ThreadPool.Names.SAME, new ShardActiveRequestHandler());
        this.deleteShardTimeout = INDICES_STORE_DELETE_SHARD_TIMEOUT.get(settings);
        // Doesn't make sense to delete shards on non-data nodes
        if (DiscoveryNode.isDataNode(settings)) {
            // we double check nothing has changed when responses come back from other nodes.
            // it's easier to do that check when the current cluster state is visible.
            // also it's good in general to let things settle down
            clusterService.addListener(this);
        }
    }

    @Override
    public void close() {
        if (DiscoveryNode.isDataNode(settings)) {
            clusterService.removeListener(this);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.routingTableChanged()) {
            return;
        }

        if (event.state().blocks().disableStatePersistence()) {
            return;
        }

        RoutingTable routingTable = event.state().routingTable();

        // remove entries from cache that don't exist in the routing table anymore (either closed or deleted indices)
        // - removing shard data of deleted indices is handled by IndicesClusterStateService
        // - closed indices don't need to be removed from the cache but we do it anyway for code simplicity
        for (Iterator<ShardId> it = folderNotFoundCache.iterator(); it.hasNext(); ) {
            ShardId shardId = it.next();
            if (routingTable.hasIndex(shardId.getIndex()) == false) {
                it.remove();
            }
        }
        // remove entries from cache which are allocated to this node
        final String localNodeId = event.state().nodes().getLocalNodeId();
        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(localNodeId);
        if (localRoutingNode != null) {
            for (ShardRouting routing : localRoutingNode) {
                folderNotFoundCache.remove(routing.shardId());
            }
        }

        for (IndexRoutingTable indexRoutingTable : routingTable) {
            // Note, closed indices will not have any routing information, so won't be deleted
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                ShardId shardId = indexShardRoutingTable.shardId();
                if (folderNotFoundCache.contains(shardId) == false && shardCanBeDeleted(localNodeId, indexShardRoutingTable)) {
                    IndexService indexService = indicesService.indexService(indexRoutingTable.getIndex());
                    final IndexSettings indexSettings;
                    if (indexService == null) {
                        IndexMetaData indexMetaData = event.state().getMetaData().getIndexSafe(indexRoutingTable.getIndex());
                        indexSettings = new IndexSettings(indexMetaData, settings);
                    } else {
                        indexSettings = indexService.getIndexSettings();
                    }
                    IndicesService.ShardDeletionCheckResult shardDeletionCheckResult = indicesService.canDeleteShardContent(shardId, indexSettings);
                    switch (shardDeletionCheckResult) {
                        case FOLDER_FOUND_CAN_DELETE:
                            deleteShardIfExistElseWhere(event.state(), indexShardRoutingTable);
                            break;
                        case NO_FOLDER_FOUND:
                            folderNotFoundCache.add(shardId);
                            break;
                        case NO_LOCAL_STORAGE:
                            assert false : "shard deletion only runs on data nodes which always have local storage";
                            // nothing to do
                            break;
                        case STILL_ALLOCATED:
                            // nothing to do
                            break;
                        default:
                            assert false : "unknown shard deletion check result: " + shardDeletionCheckResult;
                    }
                }
            }
        }
    }

    static boolean shardCanBeDeleted(String localNodeId, IndexShardRoutingTable indexShardRoutingTable) {
        // a shard can be deleted if all its copies are active, and its not allocated on this node
        if (indexShardRoutingTable.size() == 0) {
            // should not really happen, there should always be at least 1 (primary) shard in a
            // shard replication group, in any case, protected from deleting something by mistake
            return false;
        }

        for (ShardRouting shardRouting : indexShardRoutingTable) {
            // be conservative here, check on started, not even active
            if (shardRouting.started() == false) {
                return false;
            }

            // check if shard is active on the current node
            if (localNodeId.equals(shardRouting.currentNodeId())) {
                return false;
            }
        }

        return true;
    }

    private void deleteShardIfExistElseWhere(ClusterState state, IndexShardRoutingTable indexShardRoutingTable) {
        List<Tuple<DiscoveryNode, ShardActiveRequest>> requests = new ArrayList<>(indexShardRoutingTable.size());
        String indexUUID = indexShardRoutingTable.shardId().getIndex().getUUID();
        ClusterName clusterName = state.getClusterName();
        for (ShardRouting shardRouting : indexShardRoutingTable) {
            assert shardRouting.started() : "expected started shard but was " + shardRouting;
            DiscoveryNode currentNode = state.nodes().get(shardRouting.currentNodeId());
            requests.add(new Tuple<>(currentNode, new ShardActiveRequest(clusterName, indexUUID, shardRouting.shardId(), deleteShardTimeout)));
        }

        ShardActiveResponseHandler responseHandler = new ShardActiveResponseHandler(indexShardRoutingTable.shardId(), state.getVersion(),
            requests.size());
        for (Tuple<DiscoveryNode, ShardActiveRequest> request : requests) {
            logger.trace("{} sending shard active check to {}", request.v2().shardId, request.v1());
            transportService.sendRequest(request.v1(), ACTION_SHARD_EXISTS, request.v2(), responseHandler);
        }
    }

    private class ShardActiveResponseHandler implements TransportResponseHandler<ShardActiveResponse> {

        private final ShardId shardId;
        private final int expectedActiveCopies;
        private final long clusterStateVersion;
        private final AtomicInteger awaitingResponses;
        private final AtomicInteger activeCopies;

        ShardActiveResponseHandler(ShardId shardId, long clusterStateVersion, int expectedActiveCopies) {
            this.shardId = shardId;
            this.expectedActiveCopies = expectedActiveCopies;
            this.clusterStateVersion = clusterStateVersion;
            this.awaitingResponses = new AtomicInteger(expectedActiveCopies);
            this.activeCopies = new AtomicInteger();
        }

        @Override
        public ShardActiveResponse read(StreamInput in) throws IOException {
            return new ShardActiveResponse(in);
        }

        @Override
        public void handleResponse(ShardActiveResponse response) {
            logger.trace("{} is {}active on node {}", shardId, response.shardActive ? "" : "not ", response.node);
            if (response.shardActive) {
                activeCopies.incrementAndGet();
            }

            if (awaitingResponses.decrementAndGet() == 0) {
                allNodesResponded();
            }
        }

        @Override
        public void handleException(TransportException exp) {
            logger.debug(() -> new ParameterizedMessage("shards active request failed for {}", shardId), exp);
            if (awaitingResponses.decrementAndGet() == 0) {
                allNodesResponded();
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        private void allNodesResponded() {
            if (activeCopies.get() != expectedActiveCopies) {
                logger.trace("not deleting shard {}, expected {} active copies, but only {} found active copies", shardId, expectedActiveCopies, activeCopies.get());
                return;
            }

            ClusterState latestClusterState = clusterService.state();
            if (clusterStateVersion != latestClusterState.getVersion()) {
                logger.trace("not deleting shard {}, the latest cluster state version[{}] is not equal to cluster state before shard active api call [{}]", shardId, latestClusterState.getVersion(), clusterStateVersion);
                return;
            }

            clusterService.getClusterApplierService().runOnApplierThread("indices_store ([" + shardId + "] active fully on other nodes)",
                currentState -> {
                    if (clusterStateVersion != currentState.getVersion()) {
                        logger.trace("not deleting shard {}, the update task state version[{}] is not equal to cluster state before shard active api call [{}]", shardId, currentState.getVersion(), clusterStateVersion);
                        return;
                    }
                    try {
                        indicesService.deleteShardStore("no longer used", shardId, currentState);
                    } catch (Exception ex) {
                        logger.debug(() -> new ParameterizedMessage("{} failed to delete unallocated shard, ignoring", shardId), ex);
                    }
                },
                (source, e) -> logger.error(() -> new ParameterizedMessage("{} unexpected error during deletion of unallocated shard", shardId), e)
            );
        }

    }

    private class ShardActiveRequestHandler implements TransportRequestHandler<ShardActiveRequest> {

        @Override
        public void messageReceived(final ShardActiveRequest request, final TransportChannel channel) throws Exception {
            IndexShard indexShard = getShard(request);

            // make sure shard is really there before register cluster state observer
            if (indexShard == null) {
                channel.sendResponse(new ShardActiveResponse(false, clusterService.localNode()));
            } else {
                // create observer here. we need to register it here because we need to capture the current cluster state
                // which will then be compared to the one that is applied when we call waitForNextChange(). if we create it
                // later we might miss an update and wait forever in case no new cluster state comes in.
                // in general, using a cluster state observer here is a workaround for the fact that we cannot listen on shard state changes explicitly.
                // instead we wait for the cluster state changes because we know any shard state change will trigger or be
                // triggered by a cluster state change.
                ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.timeout, logger, threadPool.getThreadContext());
                // check if shard is active. if so, all is good
                boolean shardActive = shardActive(indexShard);
                if (shardActive) {
                    channel.sendResponse(new ShardActiveResponse(true, clusterService.localNode()));
                } else {
                    // shard is not active, might be POST_RECOVERY so check if cluster state changed inbetween or wait for next change
                    observer.waitForNextChange(new ClusterStateObserver.Listener() {
                        @Override
                        public void onNewClusterState(ClusterState state) {
                            sendResult(shardActive(getShard(request)));
                        }

                        @Override
                        public void onClusterServiceClose() {
                            sendResult(false);
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            sendResult(shardActive(getShard(request)));
                        }

                        public void sendResult(boolean shardActive) {
                            try {
                                channel.sendResponse(new ShardActiveResponse(shardActive, clusterService.localNode()));
                            } catch (IOException e) {
                                logger.error(() -> new ParameterizedMessage("failed send response for shard active while trying to delete shard {} - shard will probably not be removed", request.shardId), e);
                            } catch (EsRejectedExecutionException e) {
                                logger.error(() -> new ParameterizedMessage("failed send response for shard active while trying to delete shard {} - shard will probably not be removed", request.shardId), e);
                            }
                        }
                    }, newState -> {
                        // the shard is not there in which case we want to send back a false (shard is not active), so the cluster state listener must be notified
                        // or the shard is active in which case we want to send back that the shard is active
                        // here we could also evaluate the cluster state and get the information from there. we
                        // don't do it because we would have to write another method for this that would have the same effect
                        IndexShard currentShard = getShard(request);
                        return currentShard == null || shardActive(currentShard);
                    });
                }
            }
        }

        private boolean shardActive(IndexShard indexShard) {
            if (indexShard != null) {
                return ACTIVE_STATES.contains(indexShard.state());
            }
            return false;
        }

        private IndexShard getShard(ShardActiveRequest request) {
            ClusterName thisClusterName = clusterService.getClusterName();
            if (!thisClusterName.equals(request.clusterName)) {
                logger.trace("shard exists request meant for cluster[{}], but this is cluster[{}], ignoring request", request.clusterName, thisClusterName);
                return null;
            }
            ShardId shardId = request.shardId;
            IndexService indexService = indicesService.indexService(shardId.getIndex());
            if (indexService != null && indexService.indexUUID().equals(request.indexUUID)) {
                return indexService.getShardOrNull(shardId.id());
            }
            return null;
        }

    }

    private static class ShardActiveRequest extends TransportRequest {
        protected TimeValue timeout = null;
        private ClusterName clusterName;
        private String indexUUID;
        private ShardId shardId;

        ShardActiveRequest() {
        }

        ShardActiveRequest(ClusterName clusterName, String indexUUID, ShardId shardId, TimeValue timeout) {
            this.shardId = shardId;
            this.indexUUID = indexUUID;
            this.clusterName = clusterName;
            this.timeout = timeout;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            clusterName = new ClusterName(in);
            indexUUID = in.readString();
            shardId = ShardId.readShardId(in);
            timeout = new TimeValue(in.readLong(), TimeUnit.MILLISECONDS);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            clusterName.writeTo(out);
            out.writeString(indexUUID);
            shardId.writeTo(out);
            out.writeLong(timeout.millis());
        }
    }

    private static class ShardActiveResponse extends TransportResponse {

        private final boolean shardActive;
        private final DiscoveryNode node;

        ShardActiveResponse(boolean shardActive, DiscoveryNode node) {
            this.shardActive = shardActive;
            this.node = node;
        }

        ShardActiveResponse(StreamInput in) throws IOException {
            shardActive = in.readBoolean();
            node = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(shardActive);
            node.writeTo(out);
        }
    }
}
