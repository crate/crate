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

import io.crate.common.unit.TimeValue;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TransportNodesListShardStoreMetadata extends TransportNodesAction<TransportNodesListShardStoreMetadata.Request,
    TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata,
    TransportNodesListShardStoreMetadata.NodeRequest,
    TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> {

    public static final String ACTION_NAME = "internal:cluster/nodes/indices/shard/store";
    public static final ActionType<NodesStoreFilesMetadata> TYPE = new ActionType<>(ACTION_NAME);

    private final IndicesService indicesService;
    private final NodeEnvironment nodeEnv;
    private final NamedXContentRegistry namedXContentRegistry;
    private final Settings settings;

    @Inject
    public TransportNodesListShardStoreMetadata(Settings settings,
                                                ThreadPool threadPool,
                                                ClusterService clusterService,
                                                TransportService transportService,
                                                IndicesService indicesService,
                                                NodeEnvironment nodeEnv,
                                                NamedXContentRegistry namedXContentRegistry) {
        super(ACTION_NAME, threadPool, clusterService, transportService,
            Request::new, NodeRequest::new, ThreadPool.Names.FETCH_SHARD_STORE, NodeStoreFilesMetadata.class);
        this.settings = settings;
        this.indicesService = indicesService;
        this.nodeEnv = nodeEnv;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeStoreFilesMetadata read(StreamInput in) throws IOException {
        return new NodeStoreFilesMetadata(in);
    }

    @Override
    protected NodesStoreFilesMetadata newResponse(Request request,
                                                  List<NodeStoreFilesMetadata> responses, List<FailedNodeException> failures) {
        return new NodesStoreFilesMetadata(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeStoreFilesMetadata nodeOperation(NodeRequest request, Task task) {
        try {
            return new NodeStoreFilesMetadata(clusterService.localNode(), listStoreMetadata(request.shardId));
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to list store metadata for shard [" + request.shardId + "]", e);
        }
    }

    private StoreFilesMetadata listStoreMetadata(ShardId shardId) throws IOException {
        logger.trace("listing store meta data for {}", shardId);
        long startTimeNS = System.nanoTime();
        boolean exists = false;
        try {
            IndexService indexService = indicesService.indexService(shardId.getIndex());
            if (indexService != null) {
                IndexShard indexShard = indexService.getShardOrNull(shardId.id());
                if (indexShard != null) {
                    try {
                        final StoreFilesMetadata storeFilesMetadata = new StoreFilesMetadata(
                            shardId,
                            indexShard.snapshotStoreMetadata(),
                            indexShard.getPeerRecoveryRetentionLeases()
                        );
                        exists = true;
                        return storeFilesMetadata;
                    } catch (org.apache.lucene.index.IndexNotFoundException e) {
                        logger.trace(new ParameterizedMessage("[{}] node is missing index, responding with empty", shardId), e);
                        return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
                    } catch (IOException e) {
                        logger.warn(new ParameterizedMessage("[{}] can't read metadata from store, responding with empty", shardId), e);
                        return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
                    }
                }
            }
            // try and see if we an list unallocated
            IndexMetadata metadata = clusterService.state().metadata().index(shardId.getIndex());
            if (metadata == null) {
                // we may send this requests while processing the cluster state that recovered the index
                // sometimes the request comes in before the local node processed that cluster state
                // in such cases we can load it from disk
                metadata = IndexMetadata.FORMAT.loadLatestState(logger, namedXContentRegistry,
                    nodeEnv.indexPaths(shardId.getIndex()));
            }
            if (metadata == null) {
                logger.trace("{} node doesn't have meta data for the requests index, responding with empty", shardId);
                return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
            }
            final IndexSettings indexSettings = indexService != null ? indexService.getIndexSettings() : new IndexSettings(metadata, settings);
            final ShardPath shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, indexSettings);
            if (shardPath == null) {
                return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
            }
            // note that this may fail if it can't get access to the shard lock. Since we check above there is an active shard, this means:
            // 1) a shard is being constructed, which means the master will not use a copy of this replica
            // 2) A shard is shutting down and has not cleared it's content within lock timeout. In this case the master may not
            //    reuse local resources.
            final Store.MetadataSnapshot metadataSnapshot =
                Store.readMetadataSnapshot(shardPath.resolveIndex(), shardId, nodeEnv::shardLock, logger);
            // We use peer recovery retention leases from the primary for allocating replicas. We should always have retention leases when
            // we refresh shard info after the primary has started. Hence, we can ignore retention leases if there is no active shard.
            return new StoreFilesMetadata(shardId, metadataSnapshot, Collections.emptyList());
        } finally {
            TimeValue took = new TimeValue(System.nanoTime() - startTimeNS, TimeUnit.NANOSECONDS);
            if (exists) {
                logger.debug("{} loaded store meta data (took [{}])", shardId, took);
            } else {
                logger.trace("{} didn't find any store meta data to load (took [{}])", shardId, took);
            }
        }
    }

    public static class StoreFilesMetadata implements Iterable<StoreFileMetadata>, Writeable {
        private final ShardId shardId;
        private final Store.MetadataSnapshot metadataSnapshot;
        private final List<RetentionLease> peerRecoveryRetentionLeases;

        public StoreFilesMetadata(ShardId shardId,
                                  Store.MetadataSnapshot metadataSnapshot,
                                  List<RetentionLease> peerRecoveryRetentionLeases) {
            this.shardId = shardId;
            this.metadataSnapshot = metadataSnapshot;
            this.peerRecoveryRetentionLeases = peerRecoveryRetentionLeases;
        }

        public StoreFilesMetadata(StreamInput in) throws IOException {
            this.shardId = new ShardId(in);
            this.metadataSnapshot = new Store.MetadataSnapshot(in);
            if (in.getVersion().onOrAfter(Version.V_4_3_0)) {
                this.peerRecoveryRetentionLeases = in.readList(RetentionLease::new);
            } else {
                this.peerRecoveryRetentionLeases = Collections.emptyList();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            metadataSnapshot.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_4_3_0)) {
                out.writeList(peerRecoveryRetentionLeases);
            }
        }

        public ShardId shardId() {
            return this.shardId;
        }

        public boolean isEmpty() {
            return metadataSnapshot.size() == 0;
        }

        @Override
        public Iterator<StoreFileMetadata> iterator() {
            return metadataSnapshot.iterator();
        }

        public boolean fileExists(String name) {
            return metadataSnapshot.asMap().containsKey(name);
        }

        public StoreFileMetadata file(String name) {
            return metadataSnapshot.asMap().get(name);
        }

        /**
         * Returns the retaining sequence number of the peer recovery retention lease for a given node if exists; otherwise, returns -1.
         */
        public long getPeerRecoveryRetentionLeaseRetainingSeqNo(DiscoveryNode node) {
            assert node != null;
            final String retentionLeaseId = ReplicationTracker.getPeerRecoveryRetentionLeaseId(node.getId());
            return peerRecoveryRetentionLeases.stream().filter(lease -> lease.id().equals(retentionLeaseId))
                .mapToLong(RetentionLease::retainingSequenceNumber).findFirst().orElse(-1L);
        }

        public List<RetentionLease> peerRecoveryRetentionLeases() {
            return peerRecoveryRetentionLeases;
        }

        /**
         * @return commit sync id if exists, else null
         */
        public String syncId() {
            return metadataSnapshot.getSyncId();
        }

        @Override
        public String toString() {
            return "StoreFilesMetadata{" +
                ", shardId=" + shardId +
                ", metadataSnapshot{size=" + metadataSnapshot.size() + ", syncId=" + metadataSnapshot.getSyncId() + "}" +
                '}';
        }
    }


    public static class Request extends BaseNodesRequest<Request> {

        private final ShardId shardId;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
        }

        public Request(ShardId shardId, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardId = shardId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
        }
    }

    public static class NodesStoreFilesMetadata extends BaseNodesResponse<NodeStoreFilesMetadata> {

        public NodesStoreFilesMetadata(ClusterName clusterName, List<NodeStoreFilesMetadata> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeStoreFilesMetadata> nodes) throws IOException {
            out.writeList(nodes);
        }
    }


    public static class NodeRequest extends BaseNodeRequest {

        private final ShardId shardId;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
        }

        NodeRequest(TransportNodesListShardStoreMetadata.Request request) {
            this.shardId = request.shardId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
        }
    }

    public static class NodeStoreFilesMetadata extends BaseNodeResponse {

        private final StoreFilesMetadata storeFilesMetadata;

        public NodeStoreFilesMetadata(DiscoveryNode node, StoreFilesMetadata storeFilesMetadata) {
            super(node);
            this.storeFilesMetadata = storeFilesMetadata;
        }

        public NodeStoreFilesMetadata(StreamInput in) throws IOException {
            super(in);
            storeFilesMetadata = new StoreFilesMetadata(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            storeFilesMetadata.writeTo(out);
        }

        public StoreFilesMetadata storeFilesMetadata() {
            return storeFilesMetadata;
        }

        @Override
        public String toString() {
            return "[[" + getNode() + "][" + storeFilesMetadata + "]]";
        }
    }
}
