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
package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;

/**
 * Allows to asynchronously fetch shard related data from other nodes for allocation, without blocking
 * the cluster update thread.
 * <p>
 * The async fetch logic maintains a map of which nodes are being fetched from in an async manner,
 * and once the results are back, it makes sure to schedule a reroute to make sure those results will
 * be taken into account.
 */
public abstract class AsyncShardFetch<T extends BaseNodeResponse> implements Releasable {

    /**
     * An action that lists the relevant shard data that needs to be fetched.
     */
    public interface Lister<NodesResponse extends BaseNodesResponse<NodeResponse>, NodeResponse extends BaseNodeResponse> {
        void list(ShardId shardId, DiscoveryNode[] nodes, ActionListener<NodesResponse> listener);
    }

    protected final Logger logger;
    protected final String type;
    protected final ShardId shardId;
    private final Lister<BaseNodesResponse<T>, T> action;
    private final Map<String, NodeEntry<T>> cache = new HashMap<>();
    private final Set<String> nodesToIgnore = new HashSet<>();
    private final AtomicLong round = new AtomicLong();
    private boolean closed;

    @SuppressWarnings("unchecked")
    protected AsyncShardFetch(Logger logger, String type, ShardId shardId, Lister<? extends BaseNodesResponse<T>, T> action) {
        this.logger = logger;
        this.type = type;
        this.shardId = shardId;
        this.action = (Lister<BaseNodesResponse<T>, T>) action;
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }

    /**
     * Returns the number of async fetches that are currently ongoing.
     */
    public synchronized int getNumberOfInFlightFetches() {
        int count = 0;
        for (NodeEntry<T> nodeEntry : cache.values()) {
            if (nodeEntry.isFetching()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Fetches the data for the relevant shard. If there any ongoing async fetches going on, or new ones have
     * been initiated by this call, the result will have no data.
     * <p>
     * The ignoreNodes are nodes that are supposed to be ignored for this round, since fetching is async, we need
     * to keep them around and make sure we add them back when all the responses are fetched and returned.
     */
    public synchronized FetchResult<T> fetchData(DiscoveryNodes nodes, Set<String> ignoreNodes) {
        if (closed) {
            throw new IllegalStateException(shardId + ": can't fetch data on closed async fetch");
        }
        nodesToIgnore.addAll(ignoreNodes);
        fillShardCacheWithDataNodes(cache, nodes);
        List<NodeEntry<T>> nodesToFetch = findNodesToFetch(cache);
        if (nodesToFetch.isEmpty() == false) {
            // mark all node as fetching and go ahead and async fetch them
            // use a unique round id to detect stale responses in processAsyncFetch
            final long fetchingRound = round.incrementAndGet();
            for (NodeEntry<T> nodeEntry : nodesToFetch) {
                nodeEntry.markAsFetching(fetchingRound);
            }
            DiscoveryNode[] discoNodesToFetch = nodesToFetch.stream().map(NodeEntry::getNodeId).map(nodes::get)
                .toArray(DiscoveryNode[]::new);
            asyncFetch(discoNodesToFetch, fetchingRound);
        }

        // if we are still fetching, return null to indicate it
        if (hasAnyNodeFetching(cache)) {
            return new FetchResult<>(shardId, null, emptySet());
        } else {
            // nothing to fetch, yay, build the return value
            Map<DiscoveryNode, T> fetchData = new HashMap<>();
            Set<String> failedNodes = new HashSet<>();
            for (Iterator<Map.Entry<String, NodeEntry<T>>> it = cache.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, NodeEntry<T>> entry = it.next();
                String nodeId = entry.getKey();
                NodeEntry<T> nodeEntry = entry.getValue();

                DiscoveryNode node = nodes.get(nodeId);
                if (node != null) {
                    if (nodeEntry.isFailed()) {
                        // if its failed, remove it from the list of nodes, so if this run doesn't work
                        // we try again next round to fetch it again
                        it.remove();
                        failedNodes.add(nodeEntry.getNodeId());
                    } else {
                        if (nodeEntry.getValue() != null) {
                            fetchData.put(node, nodeEntry.getValue());
                        }
                    }
                }
            }
            Set<String> allIgnoreNodes = unmodifiableSet(new HashSet<>(nodesToIgnore));
            // clear the nodes to ignore, we had a successful run in fetching everything we can
            // we need to try them if another full run is needed
            nodesToIgnore.clear();
            // if at least one node failed, make sure to have a protective reroute
            // here, just case this round won't find anything, and we need to retry fetching data
            if (failedNodes.isEmpty() == false || allIgnoreNodes.isEmpty() == false) {
                reroute(shardId, "nodes failed [" + failedNodes.size() + "], ignored [" + allIgnoreNodes.size() + "]");
            }
            return new FetchResult<>(shardId, fetchData, allIgnoreNodes);
        }
    }

    /**
     * Called by the response handler of the async action to fetch data. Verifies that its still working
     * on the same cache generation, otherwise the results are discarded. It then goes and fills the relevant data for
     * the shard (response + failures), issuing a reroute at the end of it to make sure there will be another round
     * of allocations taking this new data into account.
     */
    protected synchronized void processAsyncFetch(List<T> responses, List<FailedNodeException> failures, long fetchingRound) {
        if (closed) {
            // we are closed, no need to process this async fetch at all
            logger.trace("{} ignoring fetched [{}] results, already closed", shardId, type);
            return;
        }
        logger.trace("{} processing fetched [{}] results", shardId, type);

        if (responses != null) {
            for (T response : responses) {
                NodeEntry<T> nodeEntry = cache.get(response.getNode().getId());
                if (nodeEntry != null) {
                    if (nodeEntry.getFetchingRound() != fetchingRound) {
                        assert nodeEntry.getFetchingRound() > fetchingRound : "node entries only replaced by newer rounds";
                        logger.trace("{} received response for [{}] from node {} for an older fetching round (expected: {} but was: {})",
                            shardId, nodeEntry.getNodeId(), type, nodeEntry.getFetchingRound(), fetchingRound);
                    } else if (nodeEntry.isFailed()) {
                        logger.trace("{} node {} has failed for [{}] (failure [{}])", shardId, nodeEntry.getNodeId(), type,
                            nodeEntry.getFailure());
                    } else {
                        // if the entry is there, for the right fetching round and not marked as failed already, process it
                        logger.trace("{} marking {} as done for [{}], result is [{}]", shardId, nodeEntry.getNodeId(), type, response);
                        nodeEntry.doneFetching(response);
                    }
                }
            }
        }
        if (failures != null) {
            for (FailedNodeException failure : failures) {
                logger.trace("{} processing failure {} for [{}]", shardId, failure, type);
                NodeEntry<T> nodeEntry = cache.get(failure.nodeId());
                if (nodeEntry != null) {
                    if (nodeEntry.getFetchingRound() != fetchingRound) {
                        assert nodeEntry.getFetchingRound() > fetchingRound : "node entries only replaced by newer rounds";
                        logger.trace("{} received failure for [{}] from node {} for an older fetching round (expected: {} but was: {})",
                            shardId, nodeEntry.getNodeId(), type, nodeEntry.getFetchingRound(), fetchingRound);
                    } else if (nodeEntry.isFailed() == false) {
                        // if the entry is there, for the right fetching round and not marked as failed already, process it
                        Throwable unwrappedCause = ExceptionsHelper.unwrapCause(failure.getCause());
                        // if the request got rejected or timed out, we need to try it again next time...
                        if (unwrappedCause instanceof EsRejectedExecutionException ||
                            unwrappedCause instanceof ReceiveTimeoutTransportException ||
                            unwrappedCause instanceof ElasticsearchTimeoutException) {
                            nodeEntry.restartFetching();
                        } else {
                            logger.warn(() -> new ParameterizedMessage("{}: failed to list shard for {} on node [{}]",
                                shardId, type, failure.nodeId()), failure);
                            nodeEntry.doneFetching(failure.getCause());
                        }
                    }
                }
            }
        }
        reroute(shardId, "post_response");
    }

    /**
     * Implement this in order to scheduled another round that causes a call to fetch data.
     */
    protected abstract void reroute(ShardId shardId, String reason);

    /**
     * Fills the shard fetched data with new (data) nodes and a fresh NodeEntry, and removes from
     * it nodes that are no longer part of the state.
     */
    private void fillShardCacheWithDataNodes(Map<String, NodeEntry<T>> shardCache, DiscoveryNodes nodes) {
        // verify that all current data nodes are there
        for (ObjectObjectCursor<String, DiscoveryNode> cursor : nodes.getDataNodes()) {
            DiscoveryNode node = cursor.value;
            if (shardCache.containsKey(node.getId()) == false) {
                shardCache.put(node.getId(), new NodeEntry<T>(node.getId()));
            }
        }
        // remove nodes that are not longer part of the data nodes set
        shardCache.keySet().removeIf(nodeId -> !nodes.nodeExists(nodeId));
    }

    /**
     * Finds all the nodes that need to be fetched. Those are nodes that have no
     * data, and are not in fetch mode.
     */
    private List<NodeEntry<T>> findNodesToFetch(Map<String, NodeEntry<T>> shardCache) {
        List<NodeEntry<T>> nodesToFetch = new ArrayList<>();
        for (NodeEntry<T> nodeEntry : shardCache.values()) {
            if (nodeEntry.hasData() == false && nodeEntry.isFetching() == false) {
                nodesToFetch.add(nodeEntry);
            }
        }
        return nodesToFetch;
    }

    /**
     * Are there any nodes that are fetching data?
     */
    private boolean hasAnyNodeFetching(Map<String, NodeEntry<T>> shardCache) {
        for (NodeEntry<T> nodeEntry : shardCache.values()) {
            if (nodeEntry.isFetching()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Async fetches data for the provided shard with the set of nodes that need to be fetched from.
     */
    // visible for testing
    void asyncFetch(final DiscoveryNode[] nodes, long fetchingRound) {
        logger.trace("{} fetching [{}] from {}", shardId, type, nodes);
        action.list(shardId, nodes, new ActionListener<BaseNodesResponse<T>>() {
            @Override
            public void onResponse(BaseNodesResponse<T> response) {
                processAsyncFetch(response.getNodes(), response.failures(), fetchingRound);
            }

            @Override
            public void onFailure(Exception e) {
                List<FailedNodeException> failures = new ArrayList<>(nodes.length);
                for (final DiscoveryNode node: nodes) {
                    failures.add(new FailedNodeException(node.getId(), "total failure in fetching", e));
                }
                processAsyncFetch(null, failures, fetchingRound);
            }
        });
    }

    /**
     * The result of a fetch operation. Make sure to first check {@link #hasData()} before
     * fetching the actual data.
     */
    public static class FetchResult<T extends BaseNodeResponse> {

        private final ShardId shardId;
        private final Map<DiscoveryNode, T> data;
        private final Set<String> ignoreNodes;

        public FetchResult(ShardId shardId, Map<DiscoveryNode, T> data, Set<String> ignoreNodes) {
            this.shardId = shardId;
            this.data = data;
            this.ignoreNodes = ignoreNodes;
        }

        /**
         * Does the result actually contain data? If not, then there are on going fetch
         * operations happening, and it should wait for it.
         */
        public boolean hasData() {
            return data != null;
        }

        /**
         * Returns the actual data, note, make sure to check {@link #hasData()} first and
         * only use this when there is an actual data.
         */
        public Map<DiscoveryNode, T> getData() {
            assert data != null : "getData should only be called if there is data to be fetched, please check hasData first";
            return this.data;
        }

        /**
         * Process any changes needed to the allocation based on this fetch result.
         */
        public void processAllocation(RoutingAllocation allocation) {
            for (String ignoreNode : ignoreNodes) {
                allocation.addIgnoreShardForNode(shardId, ignoreNode);
            }
        }
    }

    /**
     * A node entry, holding the state of the fetched data for a specific shard
     * for a giving node.
     */
    static class NodeEntry<T> {
        private final String nodeId;
        private boolean fetching;
        @Nullable
        private T value;
        private boolean valueSet;
        private Throwable failure;
        private long fetchingRound;

        NodeEntry(String nodeId) {
            this.nodeId = nodeId;
        }

        String getNodeId() {
            return this.nodeId;
        }

        boolean isFetching() {
            return fetching;
        }

        void markAsFetching(long fetchingRound) {
            assert fetching == false : "double marking a node as fetching";
            this.fetching = true;
            this.fetchingRound = fetchingRound;
        }

        void doneFetching(T value) {
            assert fetching : "setting value but not in fetching mode";
            assert failure == null : "setting value when failure already set";
            this.valueSet = true;
            this.value = value;
            this.fetching = false;
        }

        void doneFetching(Throwable failure) {
            assert fetching : "setting value but not in fetching mode";
            assert valueSet == false : "setting failure when already set value";
            assert failure != null : "setting failure can't be null";
            this.failure = failure;
            this.fetching = false;
        }

        void restartFetching() {
            assert fetching : "restarting fetching, but not in fetching mode";
            assert valueSet == false : "value can't be set when restarting fetching";
            assert failure == null : "failure can't be set when restarting fetching";
            this.fetching = false;
        }

        boolean isFailed() {
            return failure != null;
        }

        boolean hasData() {
            return valueSet || failure != null;
        }

        Throwable getFailure() {
            assert hasData() : "getting failure when data has not been fetched";
            return failure;
        }

        @Nullable
        T getValue() {
            assert failure == null : "trying to fetch value, but its marked as failed, check isFailed";
            assert valueSet : "value is not set, hasn't been fetched yet";
            return value;
        }

        long getFetchingRound() {
            return fetchingRound;
        }
    }
}
