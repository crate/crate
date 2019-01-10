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

package org.elasticsearch.index.replication;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.BulkShardResponse;
import org.elasticsearch.action.bulk.MappingUpdatePerformer;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.resync.ResyncReplicationRequest;
import org.elasticsearch.action.resync.ResyncReplicationResponse;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ReplicaResponse;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.action.support.replication.TransportWriteActionTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.DocIdSeqNoAndTerm;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public abstract class ESIndexLevelReplicationTestCase extends IndexShardTestCase {

    protected final Index index = new Index("test", "uuid");
    private final ShardId shardId = new ShardId(index, 0);
    protected final Map<String, String> indexMapping = Collections.singletonMap("type", "{ \"type\": {} }");

    protected ReplicationGroup createGroup(int replicas) throws IOException {
        return createGroup(replicas, Settings.EMPTY);
    }

    protected ReplicationGroup createGroup(int replicas, Settings settings) throws IOException {
        IndexMetaData metaData = buildIndexMetaData(replicas, settings, indexMapping);
        return new ReplicationGroup(metaData);
    }

    protected IndexMetaData buildIndexMetaData(int replicas) throws IOException {
        return buildIndexMetaData(replicas, indexMapping);
    }

    protected IndexMetaData buildIndexMetaData(int replicas, Map<String, String> mappings) throws IOException {
        return buildIndexMetaData(replicas, Settings.EMPTY, mappings);
    }

    protected IndexMetaData buildIndexMetaData(int replicas, Settings indexSettings, Map<String, String> mappings) throws IOException {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, replicas)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean())
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(),
                randomBoolean() ? IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.get(Settings.EMPTY) : between(0, 1000))
            .put(indexSettings)
            .build();
        IndexMetaData.Builder metaData = IndexMetaData.builder(index.getName())
            .settings(settings)
            .primaryTerm(0, randomIntBetween(1, 100));
        for (Map.Entry<String, String> typeMapping : mappings.entrySet()) {
            metaData.putMapping(typeMapping.getKey(), typeMapping.getValue());
        }
        return metaData.build();
    }

    protected IndexRequest copyIndexRequest(IndexRequest inRequest) throws IOException {
        final IndexRequest outRequest = new IndexRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            inRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                outRequest.readFrom(in);
            }
        }
        return outRequest;
    }

    protected DiscoveryNode getDiscoveryNode(String id) {
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNode.Role.DATA), Version.CURRENT);
    }

    protected class ReplicationGroup implements AutoCloseable, Iterable<IndexShard> {
        private IndexShard primary;
        private IndexMetaData indexMetaData;
        private final List<IndexShard> replicas;
        private final AtomicInteger replicaId = new AtomicInteger();
        private final AtomicInteger docId = new AtomicInteger();
        boolean closed = false;
        private ReplicationTargets replicationTargets;

        private final PrimaryReplicaSyncer primaryReplicaSyncer = new PrimaryReplicaSyncer(Settings.EMPTY,
            new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet()),
            (request, parentTask, primaryAllocationId, primaryTerm, listener) -> {
                try {
                    new ResyncAction(request, listener, ReplicationGroup.this).execute();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });

        protected ReplicationGroup(final IndexMetaData indexMetaData) throws IOException {
            final ShardRouting primaryRouting = this.createShardRouting("s0", true);
            primary = newShard(primaryRouting, indexMetaData, null, getEngineFactory(primaryRouting), () -> {});
            replicas = new CopyOnWriteArrayList<>();
            this.indexMetaData = indexMetaData;
            updateAllocationIDsOnPrimary();
            for (int i = 0; i < indexMetaData.getNumberOfReplicas(); i++) {
                addReplica();
            }
        }

        private ShardRouting createShardRouting(String nodeId, boolean primary) {
            return TestShardRouting.newShardRouting(shardId, nodeId, primary, ShardRoutingState.INITIALIZING,
                primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE);
        }

        protected EngineFactory getEngineFactory(ShardRouting routing) {
            return new InternalEngineFactory();
        }

        public int indexDocs(final int numOfDoc) throws Exception {
            for (int doc = 0; doc < numOfDoc; doc++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName(), "type", Integer.toString(docId.incrementAndGet()))
                        .source("{}", XContentType.JSON);
                final BulkItemResponse response = index(indexRequest);
                if (response.isFailed()) {
                    throw response.getFailure().getCause();
                } else {
                    assertEquals(DocWriteResponse.Result.CREATED, response.getResponse().getResult());
                }
            }
            return numOfDoc;
        }

        public int appendDocs(final int numOfDoc) throws Exception {
            for (int doc = 0; doc < numOfDoc; doc++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName(), "type").source("{}", XContentType.JSON);
                final BulkItemResponse response = index(indexRequest);
                if (response.isFailed()) {
                    throw response.getFailure().getCause();
                } else if (response.isFailed() == false) {
                    assertEquals(DocWriteResponse.Result.CREATED, response.getResponse().getResult());
                }
            }
            return numOfDoc;
        }

        public BulkItemResponse index(IndexRequest indexRequest) throws Exception {
            return executeWriteRequest(indexRequest, indexRequest.getRefreshPolicy());
        }

        public BulkItemResponse delete(DeleteRequest deleteRequest) throws Exception {
            return executeWriteRequest(deleteRequest, deleteRequest.getRefreshPolicy());
        }

        private BulkItemResponse executeWriteRequest(
            DocWriteRequest<?> writeRequest, WriteRequest.RefreshPolicy refreshPolicy) throws Exception {
            PlainActionFuture<BulkItemResponse> listener = new PlainActionFuture<>();
            final ActionListener<BulkShardResponse> wrapBulkListener = ActionListener.wrap(
                bulkShardResponse -> listener.onResponse(bulkShardResponse.getResponses()[0]),
                listener::onFailure);
            BulkItemRequest[] items = new BulkItemRequest[1];
            items[0] = new BulkItemRequest(0, writeRequest);
            BulkShardRequest request = new BulkShardRequest(shardId, refreshPolicy, items);
            new WriteReplicationAction(request, wrapBulkListener, this).execute();
            return listener.get();
        }

        public synchronized void startAll() throws IOException {
            startReplicas(replicas.size());
        }

        public synchronized int startReplicas(int numOfReplicasToStart) throws IOException {
            if (primary.routingEntry().initializing()) {
                startPrimary();
            }
            int started = 0;
            for (IndexShard replicaShard : replicas) {
                if (replicaShard.routingEntry().initializing()) {
                    recoverReplica(replicaShard);
                    started++;
                    if (started > numOfReplicasToStart) {
                        break;
                    }
                }
            }
            return started;
        }

        public void startPrimary() throws IOException {
            final DiscoveryNode pNode = getDiscoveryNode(primary.routingEntry().currentNodeId());
            primary.markAsRecovering("store", new RecoveryState(primary.routingEntry(), pNode, null));
            primary.recoverFromStore();
            HashSet<String> activeIds = new HashSet<>();
            activeIds.addAll(activeIds());
            activeIds.add(primary.routingEntry().allocationId().getId());
            ShardRouting startedRoutingEntry = ShardRoutingHelper.moveToStarted(primary.routingEntry());
            IndexShardRoutingTable routingTable = routingTable(shr -> shr == primary.routingEntry() ? startedRoutingEntry : shr);
            primary.updateShardState(startedRoutingEntry, primary.getPendingPrimaryTerm(), null,
                currentClusterStateVersion.incrementAndGet(), activeIds, routingTable, Collections.emptySet());
            for (final IndexShard replica : replicas) {
                recoverReplica(replica);
            }
            computeReplicationTargets();
        }

        public IndexShard addReplica() throws IOException {
            final ShardRouting replicaRouting = createShardRouting("s" + replicaId.incrementAndGet(), false);
            final IndexShard replica =
                newShard(replicaRouting, indexMetaData, null, getEngineFactory(replicaRouting), () -> {});
            addReplica(replica);
            return replica;
        }

        public synchronized void addReplica(IndexShard replica) throws IOException {
            assert shardRoutings().stream()
                .filter(shardRouting -> shardRouting.isSameAllocation(replica.routingEntry())).findFirst().isPresent() == false :
                "replica with aId [" + replica.routingEntry().allocationId() + "] already exists";
            replicas.add(replica);
            if (replicationTargets != null) {
                replicationTargets.addReplica(replica);
            }
            updateAllocationIDsOnPrimary();
        }


        public synchronized IndexShard addReplicaWithExistingPath(final ShardPath shardPath, final String nodeId) throws IOException {
            final ShardRouting shardRouting = TestShardRouting.newShardRouting(
                    shardId,
                    nodeId,
                    false, ShardRoutingState.INITIALIZING,
                    RecoverySource.PeerRecoverySource.INSTANCE);

            final IndexShard newReplica =
                    newShard(shardRouting, shardPath, indexMetaData, null, null, getEngineFactory(shardRouting),
                            () -> {}, EMPTY_EVENT_LISTENER);
            replicas.add(newReplica);
            if (replicationTargets != null) {
                replicationTargets.addReplica(newReplica);
            }
            updateAllocationIDsOnPrimary();
            return newReplica;
        }

        public synchronized List<IndexShard> getReplicas() {
            return Collections.unmodifiableList(replicas);
        }

        /**
         * promotes the specific replica as the new primary
         */
        public Future<PrimaryReplicaSyncer.ResyncTask> promoteReplicaToPrimary(IndexShard replica) throws IOException {
            PlainActionFuture<PrimaryReplicaSyncer.ResyncTask> fut = new PlainActionFuture<>();
            promoteReplicaToPrimary(replica, (shard, listener) -> {
                computeReplicationTargets();
                primaryReplicaSyncer.resync(shard,
                    new ActionListener<PrimaryReplicaSyncer.ResyncTask>() {
                        @Override
                        public void onResponse(PrimaryReplicaSyncer.ResyncTask resyncTask) {
                            listener.onResponse(resyncTask);
                            fut.onResponse(resyncTask);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                            fut.onFailure(e);
                        }
                    });
            });
            return fut;
        }

        public synchronized void promoteReplicaToPrimary(IndexShard replica,
                 BiConsumer<IndexShard, ActionListener<PrimaryReplicaSyncer.ResyncTask>> primaryReplicaSyncer)
            throws IOException {
            final long newTerm = indexMetaData.primaryTerm(shardId.id()) + 1;
            IndexMetaData.Builder newMetaData = IndexMetaData.builder(indexMetaData).primaryTerm(shardId.id(), newTerm);
            indexMetaData = newMetaData.build();
            assertTrue(replicas.remove(replica));
            closeShards(primary);
            primary = replica;
            assert primary.routingEntry().active() : "only active replicas can be promoted to primary: " + primary.routingEntry();
            ShardRouting primaryRouting = replica.routingEntry().moveActiveReplicaToPrimary();
            IndexShardRoutingTable routingTable = routingTable(shr -> shr == replica.routingEntry() ? primaryRouting : shr);

            primary.updateShardState(primaryRouting, newTerm, primaryReplicaSyncer, currentClusterStateVersion.incrementAndGet(),
                activeIds(), routingTable, Collections.emptySet());
        }

        private synchronized Set<String> activeIds() {
            return shardRoutings().stream()
                .filter(ShardRouting::active).map(ShardRouting::allocationId).map(AllocationId::getId).collect(Collectors.toSet());
        }

        private synchronized IndexShardRoutingTable routingTable(Function<ShardRouting, ShardRouting> transformer) {
            IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(primary.shardId());
            shardRoutings().stream().map(transformer).forEach(routingTable::addShard);
            return routingTable.build();
        }

        public synchronized boolean removeReplica(IndexShard replica) throws IOException {
            final boolean removed = replicas.remove(replica);
            if (removed) {
                updateAllocationIDsOnPrimary();
                computeReplicationTargets();
            }
            return removed;
        }

        public void recoverReplica(IndexShard replica) throws IOException {
            recoverReplica(replica, (r, sourceNode) -> new RecoveryTarget(r, sourceNode, recoveryListener, version -> {}));
        }

        public void recoverReplica(IndexShard replica, BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier)
            throws IOException {
            recoverReplica(replica, targetSupplier, true);
        }

        public void recoverReplica(
            IndexShard replica,
            BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier,
            boolean markAsRecovering) throws IOException {
            final IndexShardRoutingTable routingTable = routingTable(Function.identity());
            final Set<String> inSyncIds = activeIds();
            ESIndexLevelReplicationTestCase.this.recoverUnstartedReplica(replica, primary, targetSupplier, markAsRecovering, inSyncIds,
                routingTable);
            ESIndexLevelReplicationTestCase.this.startReplicaAfterRecovery(replica, primary, inSyncIds, routingTable);
            computeReplicationTargets();
        }

        public synchronized DiscoveryNode getPrimaryNode() {
            return getDiscoveryNode(primary.routingEntry().currentNodeId());
        }

        public Future<Void> asyncRecoverReplica(
                final IndexShard replica, final BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier) throws IOException {
            final FutureTask<Void> task = new FutureTask<>(() -> {
                recoverReplica(replica, targetSupplier);
                return null;
            });
            threadPool.generic().execute(task);
            return task;
        }

        public synchronized void assertAllEqual(int expectedCount) throws IOException {
            Set<String> primaryIds = getShardDocUIDs(primary);
            assertThat(primaryIds.size(), equalTo(expectedCount));
            for (IndexShard replica : replicas) {
                Set<String> replicaIds = getShardDocUIDs(replica);
                Set<String> temp = new HashSet<>(primaryIds);
                temp.removeAll(replicaIds);
                assertThat(replica.routingEntry() + " is missing docs", temp, empty());
                temp = new HashSet<>(replicaIds);
                temp.removeAll(primaryIds);
                assertThat(replica.routingEntry() + " has extra docs", temp, empty());
            }
        }

        public synchronized void refresh(String source) {
            for (IndexShard shard : this) {
                shard.refresh(source);
            }
        }

        public synchronized void flush() {
            final FlushRequest request = new FlushRequest();
            for (IndexShard shard : this) {
                shard.flush(request);
            }
        }

        public synchronized List<ShardRouting> shardRoutings() {
            return StreamSupport.stream(this.spliterator(), false).map(IndexShard::routingEntry).collect(Collectors.toList());
        }

        @Override
        public synchronized void close() throws Exception {
            if (closed == false) {
                closed = true;
                try {
                    final List<DocIdSeqNoAndTerm> docsOnPrimary = getDocIdAndSeqNos(primary);
                    for (IndexShard replica : replicas) {
                        assertThat(replica.getMaxSeenAutoIdTimestamp(), equalTo(primary.getMaxSeenAutoIdTimestamp()));
                        assertThat(replica.getMaxSeqNoOfUpdatesOrDeletes(), greaterThanOrEqualTo(primary.getMaxSeqNoOfUpdatesOrDeletes()));
                        assertThat(getDocIdAndSeqNos(replica), equalTo(docsOnPrimary));
                    }
                } catch (AlreadyClosedException ignored) { }
                closeShards(this);
            } else {
                throw new AlreadyClosedException("too bad");
            }
        }

        @Override
        public Iterator<IndexShard> iterator() {
            return Iterators.concat(replicas.iterator(), Collections.singleton(primary).iterator());
        }

        public IndexShard getPrimary() {
            return primary;
        }

        public synchronized void reinitPrimaryShard() throws IOException {
            primary = reinitShard(primary);
            computeReplicationTargets();
        }

        public void syncGlobalCheckpoint() {
            PlainActionFuture<ReplicationResponse> listener = new PlainActionFuture<>();
            try {
                new GlobalCheckpointSync(listener, this).execute();
                listener.get();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        private void updateAllocationIDsOnPrimary() throws IOException {

            primary.updateShardState(primary.routingEntry(), primary.getPendingPrimaryTerm(), null,
                currentClusterStateVersion.incrementAndGet(),
                activeIds(), routingTable(Function.identity()), Collections.emptySet());
        }

        private synchronized void computeReplicationTargets() {
            this.replicationTargets = new ReplicationTargets(this.primary, new ArrayList<>(this.replicas));
        }

        private synchronized ReplicationTargets getReplicationTargets() {
            return replicationTargets;
        }
    }

    static final class ReplicationTargets {
        final IndexShard primary;
        final List<IndexShard> replicas;

        ReplicationTargets(IndexShard primary, List<IndexShard> replicas) {
            this.primary = primary;
            this.replicas = replicas;
        }

        /**
         * This does not modify the replication targets, but only adds a replica to the list.
         * If the targets is updated to include the given replica, a replication action would
         * be able to find this replica to execute write requests on it.
         */
        synchronized void addReplica(IndexShard replica) {
            replicas.add(replica);
        }

        synchronized IndexShard findReplicaShard(ShardRouting replicaRouting) {
            for (IndexShard replica : replicas) {
                if (replica.routingEntry().isSameAllocation(replicaRouting)) {
                    return replica;
                }
            }
            throw new AssertionError("replica [" + replicaRouting + "] is not found; replicas[" + replicas + "] primary[" + primary + "]");
        }
    }

    protected abstract class ReplicationAction<Request extends ReplicationRequest<Request>,
        ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
        Response extends ReplicationResponse> {
        private final Request request;
        private ActionListener<Response> listener;
        private final ReplicationTargets replicationTargets;
        private final String opType;

        protected ReplicationAction(Request request, ActionListener<Response> listener, ReplicationGroup group, String opType) {
            this.request = request;
            this.listener = listener;
            this.replicationTargets = group.getReplicationTargets();
            this.opType = opType;
        }

        public void execute() {
            try {
                new ReplicationOperation<>(request, new PrimaryRef(),
                    new ActionListener<PrimaryResult>() {
                        @Override
                        public void onResponse(PrimaryResult result) {
                            result.respond(listener);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    }, new ReplicasRef(), logger, opType).execute();
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        IndexShard getPrimaryShard() {
            return replicationTargets.primary;
        }

        protected abstract PrimaryResult performOnPrimary(IndexShard primary, Request request) throws Exception;

        protected abstract void performOnReplica(ReplicaRequest request, IndexShard replica) throws Exception;

        class PrimaryRef implements ReplicationOperation.Primary<Request, ReplicaRequest, PrimaryResult> {

            @Override
            public ShardRouting routingEntry() {
                return getPrimaryShard().routingEntry();
            }

            @Override
            public void failShard(String message, Exception exception) {
                throw new UnsupportedOperationException("failing a primary isn't supported. failure: " + message, exception);
            }

            @Override
            public PrimaryResult perform(Request request) throws Exception {
                return performOnPrimary(getPrimaryShard(), request);
            }

            @Override
            public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {
                getPrimaryShard().updateLocalCheckpointForShard(allocationId, checkpoint);
            }

            @Override
            public void updateGlobalCheckpointForShard(String allocationId, long globalCheckpoint) {
                getPrimaryShard().updateGlobalCheckpointForShard(allocationId, globalCheckpoint);
            }

            @Override
            public long localCheckpoint() {
                return getPrimaryShard().getLocalCheckpoint();
            }

            @Override
            public long globalCheckpoint() {
                return getPrimaryShard().getGlobalCheckpoint();
            }

            @Override
            public long maxSeqNoOfUpdatesOrDeletes() {
                return getPrimaryShard().getMaxSeqNoOfUpdatesOrDeletes();
            }

            @Override
            public org.elasticsearch.index.shard.ReplicationGroup getReplicationGroup() {
                return getPrimaryShard().getReplicationGroup();
            }

        }

        class ReplicasRef implements ReplicationOperation.Replicas<ReplicaRequest> {

            @Override
            public void performOn(
                final ShardRouting replicaRouting,
                final ReplicaRequest request,
                final long globalCheckpoint,
                final long maxSeqNoOfUpdatesOrDeletes,
                final ActionListener<ReplicationOperation.ReplicaResponse> listener) {
                IndexShard replica = replicationTargets.findReplicaShard(replicaRouting);
                replica.acquireReplicaOperationPermit(
                        getPrimaryShard().getPendingPrimaryTerm(),
                        globalCheckpoint,
                        maxSeqNoOfUpdatesOrDeletes,
                        new ActionListener<Releasable>() {
                            @Override
                            public void onResponse(Releasable releasable) {
                                try {
                                    performOnReplica(request, replica);
                                    releasable.close();
                                    listener.onResponse(new ReplicaResponse(replica.getLocalCheckpoint(), replica.getGlobalCheckpoint()));
                                } catch (final Exception e) {
                                    Releasables.closeWhileHandlingException(releasable);
                                    listener.onFailure(e);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        },
                        ThreadPool.Names.INDEX, request);
            }

            @Override
            public void failShardIfNeeded(ShardRouting replica, String message, Exception exception,
                                          Runnable onSuccess, Consumer<Exception> onPrimaryDemoted,
                                          Consumer<Exception> onIgnoredFailure) {
                throw new UnsupportedOperationException("failing shard " + replica + " isn't supported. failure: " + message, exception);
            }

            @Override
            public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, Runnable onSuccess,
                                                     Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {
                throw new UnsupportedOperationException("can't mark " + shardId  + ", aid [" + allocationId + "] as stale");
            }
        }

        protected class PrimaryResult implements ReplicationOperation.PrimaryResult<ReplicaRequest> {
            final ReplicaRequest replicaRequest;
            final Response finalResponse;

            public PrimaryResult(ReplicaRequest replicaRequest, Response finalResponse) {
                this.replicaRequest = replicaRequest;
                this.finalResponse = finalResponse;
            }

            @Override
            public ReplicaRequest replicaRequest() {
                return replicaRequest;
            }

            @Override
            public void setShardInfo(ReplicationResponse.ShardInfo shardInfo) {
                finalResponse.setShardInfo(shardInfo);
            }

            public void respond(ActionListener<Response> listener) {
                listener.onResponse(finalResponse);
            }
        }

    }

    class WriteReplicationAction extends ReplicationAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

        WriteReplicationAction(BulkShardRequest request, ActionListener<BulkShardResponse> listener, ReplicationGroup replicationGroup) {
            super(request, listener, replicationGroup, "indexing");
        }

        @Override
        protected PrimaryResult performOnPrimary(IndexShard primary, BulkShardRequest request) throws Exception {
            final TransportWriteAction.WritePrimaryResult<BulkShardRequest, BulkShardResponse>
                result = executeShardBulkOnPrimary(primary, request);
            return new PrimaryResult(result.replicaRequest(), result.finalResponseIfSuccessful);
        }

        @Override
        protected void performOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
            executeShardBulkOnReplica(request, replica, getPrimaryShard().getPendingPrimaryTerm(),
                getPrimaryShard().getGlobalCheckpoint(), getPrimaryShard().getMaxSeqNoOfUpdatesOrDeletes());
        }
    }

    private TransportWriteAction.WritePrimaryResult<BulkShardRequest, BulkShardResponse> executeShardBulkOnPrimary(
        IndexShard primary, BulkShardRequest request) throws Exception {
        for (BulkItemRequest itemRequest : request.items()) {
            if (itemRequest.request() instanceof IndexRequest) {
                ((IndexRequest) itemRequest.request()).process(Version.CURRENT, null, index.getName());
            }
        }
        final PlainActionFuture<Releasable> permitAcquiredFuture = new PlainActionFuture<>();
        primary.acquirePrimaryOperationPermit(permitAcquiredFuture, ThreadPool.Names.SAME, request);
        final TransportWriteAction.WritePrimaryResult<BulkShardRequest, BulkShardResponse> result;
        try (Releasable ignored = permitAcquiredFuture.actionGet()) {
            MappingUpdatePerformer noopMappingUpdater = (update, shardId, type) -> { };
            result = TransportShardBulkAction.performOnPrimary(request, primary, null, System::currentTimeMillis, noopMappingUpdater,
                null);
        }
        TransportWriteActionTestHelper.performPostWriteActions(primary, request, result.location, logger);
        return result;
    }

    private <Request extends ReplicatedWriteRequest & DocWriteRequest>
    BulkShardRequest executeReplicationRequestOnPrimary(IndexShard primary, Request request) throws Exception {
        final BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, request.getRefreshPolicy(),
            new BulkItemRequest[]{new BulkItemRequest(0, request)});
        return executeShardBulkOnPrimary(primary, bulkShardRequest).replicaRequest();
    }

    private void executeShardBulkOnReplica(BulkShardRequest request, IndexShard replica, long operationPrimaryTerm,
                                           long globalCheckpointOnPrimary, long maxSeqNoOfUpdatesOrDeletes) throws Exception {
        final PlainActionFuture<Releasable> permitAcquiredFuture = new PlainActionFuture<>();
        replica.acquireReplicaOperationPermit(operationPrimaryTerm, globalCheckpointOnPrimary,
            maxSeqNoOfUpdatesOrDeletes, permitAcquiredFuture, ThreadPool.Names.SAME, request);
        final Translog.Location location;
        try (Releasable ignored = permitAcquiredFuture.actionGet()) {
            location = TransportShardBulkAction.performOnReplica(request, replica);
        }
        TransportWriteActionTestHelper.performPostWriteActions(replica, request, location, logger);
    }

    /**
     * indexes the given requests on the supplied primary, modifying it for replicas
     */
    BulkShardRequest indexOnPrimary(IndexRequest request, IndexShard primary) throws Exception {
        return executeReplicationRequestOnPrimary(primary, request);
    }

    /**
     * Executes the delete request on the primary, and modifies it for replicas.
     */
    BulkShardRequest deleteOnPrimary(DeleteRequest request, IndexShard primary) throws Exception {
        return executeReplicationRequestOnPrimary(primary, request);
    }

    /**
     * indexes the given requests on the supplied replica shard
     */
    void indexOnReplica(BulkShardRequest request, ReplicationGroup group, IndexShard replica) throws Exception {
        indexOnReplica(request, group, replica, group.primary.getPendingPrimaryTerm());
    }

    void indexOnReplica(BulkShardRequest request, ReplicationGroup group, IndexShard replica, long term) throws Exception {
        executeShardBulkOnReplica(request, replica, term,
            group.primary.getGlobalCheckpoint(), group.primary.getMaxSeqNoOfUpdatesOrDeletes());
    }

    /**
     * Executes the delete request on the given replica shard.
     */
    void deleteOnReplica(BulkShardRequest request, ReplicationGroup group, IndexShard replica) throws Exception {
        executeShardBulkOnReplica(request, replica, group.primary.getPendingPrimaryTerm(),
            group.primary.getGlobalCheckpoint(), group.primary.getMaxSeqNoOfUpdatesOrDeletes());
    }

    class GlobalCheckpointSync extends ReplicationAction<
            GlobalCheckpointSyncAction.Request,
            GlobalCheckpointSyncAction.Request,
            ReplicationResponse> {

        GlobalCheckpointSync(final ActionListener<ReplicationResponse> listener, final ReplicationGroup replicationGroup) {
            super(
                    new GlobalCheckpointSyncAction.Request(replicationGroup.getPrimary().shardId()),
                    listener,
                    replicationGroup,
                    "global_checkpoint_sync");
        }

        @Override
        protected PrimaryResult performOnPrimary(
                final IndexShard primary, final GlobalCheckpointSyncAction.Request request) throws Exception {
            primary.sync();
            return new PrimaryResult(request, new ReplicationResponse());
        }

        @Override
        protected void performOnReplica(final GlobalCheckpointSyncAction.Request request, final IndexShard replica) throws IOException {
            replica.sync();
        }
    }

    class ResyncAction extends ReplicationAction<ResyncReplicationRequest, ResyncReplicationRequest, ResyncReplicationResponse> {

        ResyncAction(ResyncReplicationRequest request, ActionListener<ResyncReplicationResponse> listener, ReplicationGroup group) {
            super(request, listener, group, "resync");
        }

        @Override
        protected PrimaryResult performOnPrimary(IndexShard primary, ResyncReplicationRequest request) throws Exception {
            final TransportWriteAction.WritePrimaryResult<ResyncReplicationRequest, ResyncReplicationResponse> result =
                executeResyncOnPrimary(primary, request);
            return new PrimaryResult(result.replicaRequest(), result.finalResponseIfSuccessful);
        }

        @Override
        protected void performOnReplica(ResyncReplicationRequest request, IndexShard replica) throws Exception {
            executeResyncOnReplica(replica, request, getPrimaryShard().getPendingPrimaryTerm(),
                getPrimaryShard().getGlobalCheckpoint(), getPrimaryShard().getMaxSeqNoOfUpdatesOrDeletes());
        }
    }

    private TransportWriteAction.WritePrimaryResult<ResyncReplicationRequest, ResyncReplicationResponse> executeResyncOnPrimary(
        IndexShard primary, ResyncReplicationRequest request) throws Exception {
        final TransportWriteAction.WritePrimaryResult<ResyncReplicationRequest, ResyncReplicationResponse> result =
            new TransportWriteAction.WritePrimaryResult<>(TransportResyncReplicationAction.performOnPrimary(request, primary),
                new ResyncReplicationResponse(), null, null, primary, logger);
        TransportWriteActionTestHelper.performPostWriteActions(primary, request, result.location, logger);
        return result;
    }

    private void executeResyncOnReplica(IndexShard replica, ResyncReplicationRequest request, long operationPrimaryTerm,
                                        long globalCheckpointOnPrimary, long maxSeqNoOfUpdatesOrDeletes) throws Exception {
        final Translog.Location location;
        final PlainActionFuture<Releasable> acquirePermitFuture = new PlainActionFuture<>();
        replica.acquireReplicaOperationPermit(operationPrimaryTerm, globalCheckpointOnPrimary,
            maxSeqNoOfUpdatesOrDeletes, acquirePermitFuture, ThreadPool.Names.SAME, request);
        try (Releasable ignored = acquirePermitFuture.actionGet()) {
            location = TransportResyncReplicationAction.performOnReplica(request, replica);
        }
        TransportWriteActionTestHelper.performPostWriteActions(replica, request, location, logger);
    }
}
