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

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.crate.common.collections.Tuple;
import io.crate.common.exceptions.Exceptions;
import io.crate.common.io.IOUtils;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;

/**
 * Loads (and maybe upgrades) cluster metadata at startup, and persistently stores cluster metadata for future restarts.
 *
 * When started, ensures that this version is compatible with the state stored on disk, and performs a state upgrade if necessary. Note that
 * the state being loaded when constructing the instance of this class is not necessarily the state that will be used as {@link
 * ClusterState#metadata()} because it might be stale or incomplete. Master-eligible nodes must perform an election to find a complete and
 * non-stale state, and master-ineligible nodes receive the real cluster state from the elected master after joining the cluster.
 */
public class GatewayMetaState implements Closeable {

    /**
     * Fake node ID for a voting configuration written by a master-ineligible data node to indicate that its on-disk state is potentially
     * stale (since it is written asynchronously after application, rather than before acceptance). This node ID means that if the node is
     * restarted as a master-eligible node then it does not win any elections until it has received a fresh cluster state.
     */
    public static final String STALE_STATE_CONFIG_NODE_ID = "STALE_STATE_CONFIG";

    // Set by calling start()
    private final SetOnce<PersistedState> persistedState = new SetOnce<>();

    public PersistedState getPersistedState() {
        final PersistedState persistedState = this.persistedState.get();
        assert persistedState != null : "not started";
        return persistedState;
    }

    public Metadata getMetadata() {
        return getPersistedState().getLastAcceptedState().metadata();
    }

    public void start(Settings settings, TransportService transportService, ClusterService clusterService,
                      MetaStateService metaStateService, MetadataIndexUpgradeService metadataIndexUpgradeService,
                      MetadataUpgrader metadataUpgrader, PersistedClusterStateService persistedClusterStateService) {
        assert persistedState.get() == null : "should only start once, but already have " + persistedState.get();

        if (DiscoveryNode.isMasterEligibleNode(settings) || DiscoveryNode.isDataNode(settings)) {
            try {
                final PersistedClusterStateService.OnDiskState onDiskState = persistedClusterStateService.loadBestOnDiskState();

                Metadata metadata = onDiskState.metadata;
                long lastAcceptedVersion = onDiskState.lastAcceptedVersion;
                long currentTerm = onDiskState.currentTerm;

                if (onDiskState.empty()) {
                    final Tuple<Manifest, Metadata> legacyState = metaStateService.loadFullState();
                    if (legacyState.v1().isEmpty() == false) {
                        metadata = legacyState.v2();
                        lastAcceptedVersion = legacyState.v1().getClusterStateVersion();
                        currentTerm = legacyState.v1().getCurrentTerm();
                    }
                }

                PersistedState persistedState = null;
                boolean success = false;
                try {
                    final ClusterState clusterState = prepareInitialClusterState(
                        transportService, clusterService,
                        ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
                            .version(lastAcceptedVersion)
                            .metadata(upgradeMetadataForNode(metadata, metadataIndexUpgradeService, metadataUpgrader))
                            .build());
                    if (DiscoveryNode.isMasterEligibleNode(settings)) {
                        persistedState = new LucenePersistedState(persistedClusterStateService, currentTerm, clusterState);
                    } else {
                        persistedState = new AsyncLucenePersistedState(settings, transportService.getThreadPool(),
                                                                       new LucenePersistedState(persistedClusterStateService, currentTerm, clusterState));
                    }
                    if (DiscoveryNode.isDataNode(settings)) {
                        metaStateService.unreferenceAll(); // unreference legacy files (only keep them for dangling indices functionality)
                    } else {
                        metaStateService.deleteAll(); // delete legacy files
                    }
                    // write legacy node metadata to prevent accidental downgrades from spawning empty cluster state
                    NodeMetadata.FORMAT.writeAndCleanup(new NodeMetadata(persistedClusterStateService.getNodeId(), Version.CURRENT), persistedClusterStateService.getDataPaths());
                    success = true;
                } finally {
                    if (success == false) {
                        IOUtils.closeWhileHandlingException(persistedState);
                    }
                }

                this.persistedState.set(persistedState);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to load metadata", e);
            }
        } else {
            final long currentTerm = 0L;
            final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings)).build();
            if (persistedClusterStateService.getDataPaths().length > 0) {
                // write empty cluster state just so that we have a persistent node id. There is no need to write out global metadata with
                // cluster uuid as coordinating-only nodes do not snap into a cluster as they carry no state
                try (PersistedClusterStateService.Writer persistenceWriter = persistedClusterStateService.createWriter()) {
                    persistenceWriter.writeFullStateAndCommit(currentTerm, clusterState);
                } catch (IOException e) {
                    throw new ElasticsearchException("failed to load metadata", e);
                }
                try {
                    // delete legacy cluster state files
                    metaStateService.deleteAll();
                    // write legacy node metadata to prevent downgrades from spawning empty cluster state
                    NodeMetadata.FORMAT.writeAndCleanup(new NodeMetadata(persistedClusterStateService.getNodeId(), Version.CURRENT), persistedClusterStateService.getDataPaths());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            persistedState.set(new InMemoryPersistedState(currentTerm, clusterState));
        }
    }

    // exposed so it can be overridden by tests
    ClusterState prepareInitialClusterState(TransportService transportService, ClusterService clusterService, ClusterState clusterState) {
        assert clusterState.nodes().getLocalNode() == null : "prepareInitialClusterState must only be called once";
        assert transportService.getLocalNode() != null : "transport service is not yet started";
        return Function.<ClusterState>identity()
            .andThen(ClusterStateUpdaters::addStateNotRecoveredBlock)
            .andThen(state -> ClusterStateUpdaters.setLocalNode(state, transportService.getLocalNode()))
            .andThen(state -> ClusterStateUpdaters.upgradeAndArchiveUnknownOrInvalidSettings(state, clusterService.getClusterSettings()))
            .andThen(ClusterStateUpdaters::recoverClusterBlocks)
            .apply(clusterState);
    }

    // exposed so it can be overridden by tests
    Metadata upgradeMetadataForNode(Metadata metadata,
                                    MetadataIndexUpgradeService metadataIndexUpgradeService,
                                    MetadataUpgrader metadataUpgrader) {
        return upgradeMetadata(metadata, metadataIndexUpgradeService, metadataUpgrader);
    }

    /**
     * Elasticsearch 2.0 removed several deprecated features and as well as support for Lucene 3.x. This method calls
     * {@link MetadataIndexUpgradeService} to makes sure that indices are compatible with the current version. The
     * MetadataIndexUpgradeService might also update obsolete settings if needed.
     *
     * @return input <code>metadata</code> if no upgrade is needed or an upgraded metadata
     */
    public static Metadata upgradeMetadata(Metadata metadata,
                                    MetadataIndexUpgradeService metadataIndexUpgradeService,
                                    MetadataUpgrader metadataUpgrader) {
        boolean changed = false;
        final Metadata.Builder upgradedMetadata = Metadata.builder(metadata);

        // carries upgraded IndexTemplateMetadata to MetadataIndexUpgradeService.upgradeIndexMetadata
        final Map<String, IndexTemplateMetadata> upgradedIndexTemplateMetadata = new HashMap<>();

        // upgrade current templates
        if (applyPluginUpgraders(
            metadata.templates(),
            metadataUpgrader.indexTemplateMetadataUpgraders,
            upgradedMetadata::removeTemplate,
            (s, indexTemplateMetadata) -> {
                upgradedIndexTemplateMetadata.put(s, indexTemplateMetadata);
                upgradedMetadata.put(indexTemplateMetadata);
            })) {
            changed = true;
        }

        // upgrade index meta data
        for (IndexMetadata indexMetadata : metadata) {
            String indexName = indexMetadata.getIndex().getName();
            IndexMetadata newMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(
                indexMetadata,
                IndexParts.isPartitioned(indexName) ?
                    upgradedIndexTemplateMetadata.get(PartitionName.templateName(indexName)) :
                    null,
                Version.CURRENT.minimumIndexCompatibilityVersion());
            changed |= indexMetadata != newMetadata;
            upgradedMetadata.put(newMetadata, false);
        }

        // upgrade global custom meta data
        if (applyPluginUpgraders(metadata.customs(), metadataUpgrader.customMetadataUpgraders,
                upgradedMetadata::removeCustom, upgradedMetadata::putCustom)) {
            changed = true;
        }
        return changed ? upgradedMetadata.build() : metadata;
    }

    private static <Data> boolean applyPluginUpgraders(ImmutableOpenMap<String, Data> existingData,
                                                       UnaryOperator<Map<String, Data>> upgrader,
                                                       Consumer<String> removeData,
                                                       BiConsumer<String, Data> putData) {
        // collect current data
        Map<String, Data> existingMap = new HashMap<>();
        for (ObjectObjectCursor<String, Data> customCursor : existingData) {
            existingMap.put(customCursor.key, customCursor.value);
        }
        // upgrade global custom meta data
        Map<String, Data> upgradedCustoms = upgrader.apply(existingMap);
        if (upgradedCustoms.equals(existingMap) == false) {
            // remove all data first so a plugin can remove custom metadata or templates if needed
            existingMap.keySet().forEach(removeData);
            for (Map.Entry<String, Data> upgradedCustomEntry : upgradedCustoms.entrySet()) {
                putData.accept(upgradedCustomEntry.getKey(), upgradedCustomEntry.getValue());
            }
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(persistedState.get());
    }

    // visible for testing
    public boolean allPendingAsyncStatesWritten() {
        final PersistedState ps = persistedState.get();
        if (ps instanceof AsyncLucenePersistedState) {
            return ((AsyncLucenePersistedState) ps).allPendingAsyncStatesWritten();
        } else {
            return true;
        }
    }

    static class AsyncLucenePersistedState extends InMemoryPersistedState {

        private static final Logger LOGGER = LogManager.getLogger(AsyncLucenePersistedState.class);

        static final String THREAD_NAME = "AsyncLucenePersistedState#updateTask";

        private final EsThreadPoolExecutor threadPoolExecutor;
        private final PersistedState persistedState;

        boolean newCurrentTermQueued = false;
        boolean newStateQueued = false;

        private final Object mutex = new Object();

        AsyncLucenePersistedState(Settings settings, ThreadPool threadPool, PersistedState persistedState) {
            super(persistedState.getCurrentTerm(), persistedState.getLastAcceptedState());
            final String nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(settings));
            threadPoolExecutor = EsExecutors.newFixed(
                nodeName + "/" + THREAD_NAME,
                1, 1,
                daemonThreadFactory(nodeName, THREAD_NAME));
            this.persistedState = persistedState;
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            synchronized (mutex) {
                super.setCurrentTerm(currentTerm);
                if (newCurrentTermQueued) {
                    LOGGER.trace("term update already queued (setting term to {})", currentTerm);
                } else {
                    LOGGER.trace("queuing term update (setting term to {})", currentTerm);
                    newCurrentTermQueued = true;
                    if (newStateQueued == false) {
                        scheduleUpdate();
                    }
                }
            }
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            synchronized (mutex) {
                super.setLastAcceptedState(clusterState);
                if (newStateQueued) {
                    LOGGER.trace("cluster state update already queued (setting cluster state to {})", clusterState.version());
                } else {
                    LOGGER.trace("queuing cluster state update (setting cluster state to {})", clusterState.version());
                    newStateQueued = true;
                    if (newCurrentTermQueued == false) {
                        scheduleUpdate();
                    }
                }
            }
        }

        private void scheduleUpdate() {
            assert Thread.holdsLock(mutex);
            assert threadPoolExecutor.getQueue().isEmpty() : "threadPoolExecutor queue not empty";
            threadPoolExecutor.execute(new AbstractRunnable() {

                @Override
                public void onFailure(Exception e) {
                    LOGGER.error("Exception occurred when storing new meta data", e);
                }

                @Override
                public void onRejection(Exception e) {
                    assert threadPoolExecutor.isShutdown() : "only expect rejections when shutting down";
                }

                @Override
                protected void doRun() {
                    final Long term;
                    final ClusterState clusterState;
                    synchronized (mutex) {
                        if (newCurrentTermQueued) {
                            term = getCurrentTerm();
                            LOGGER.trace("resetting newCurrentTermQueued");
                            newCurrentTermQueued = false;
                        } else {
                            term = null;
                        }
                        if (newStateQueued) {
                            clusterState = getLastAcceptedState();
                            LOGGER.trace("resetting newStateQueued");
                            newStateQueued = false;
                        } else {
                            clusterState = null;
                        }
                    }
                    // write current term before last accepted state so that it is never below term in last accepted state
                    if (term != null) {
                        persistedState.setCurrentTerm(term);
                    }
                    if (clusterState != null) {
                        persistedState.setLastAcceptedState(resetVotingConfiguration(clusterState));
                    }
                }
            });
        }

        static final CoordinationMetadata.VotingConfiguration STALE_STATE_CONFIG =
            new CoordinationMetadata.VotingConfiguration(Collections.singleton(STALE_STATE_CONFIG_NODE_ID));

        static ClusterState resetVotingConfiguration(ClusterState clusterState) {
            CoordinationMetadata newCoordinationMetadata = CoordinationMetadata.builder(clusterState.coordinationMetadata())
                .lastAcceptedConfiguration(STALE_STATE_CONFIG)
                .lastCommittedConfiguration(STALE_STATE_CONFIG)
                .build();
            return ClusterState.builder(clusterState).metadata(Metadata.builder(clusterState.metadata())
                                                                   .coordinationMetadata(newCoordinationMetadata).build()).build();
        }

        @Override
        public void close() throws IOException {
            try {
                ThreadPool.terminate(threadPoolExecutor, 10, TimeUnit.SECONDS);
            } finally {
                persistedState.close();
            }
        }

        boolean allPendingAsyncStatesWritten() {
            synchronized (mutex) {
                if (newCurrentTermQueued || newStateQueued) {
                    return false;
                }
                return threadPoolExecutor.getActiveCount() == 0;
            }
        }
    }

    /**
     * Encapsulates the incremental writing of metadata to a {@link PersistedClusterStateService.Writer}.
     */
    static class LucenePersistedState implements PersistedState {

        private long currentTerm;
        private ClusterState lastAcceptedState;
        private final PersistedClusterStateService persistedClusterStateService;

        // As the close method can be concurrently called to the other PersistedState methods, this class has extra protection in place.
        private final AtomicReference<PersistedClusterStateService.Writer> persistenceWriter = new AtomicReference<>();
        boolean writeNextStateFully;

        LucenePersistedState(PersistedClusterStateService persistedClusterStateService, long currentTerm, ClusterState lastAcceptedState)
            throws IOException {
            this.persistedClusterStateService = persistedClusterStateService;
            this.currentTerm = currentTerm;
            this.lastAcceptedState = lastAcceptedState;
            // Write the whole state out to be sure it's fresh and using the latest format. Called during initialisation, so that
            // (1) throwing an IOException is enough to halt the node, and
            // (2) the index is currently empty since it was opened with IndexWriterConfig.OpenMode.CREATE

            // In the common case it's actually sufficient to commit() the existing state and not do any indexing. For instance,
            // this is true if there's only one data path on this master node, and the commit we just loaded was already written out
            // by this version of Elasticsearch. TODO TBD should we avoid indexing when possible?
            final PersistedClusterStateService.Writer writer = persistedClusterStateService.createWriter();
            try {
                writer.writeFullStateAndCommit(currentTerm, lastAcceptedState);
            } catch (Exception e) {
                try {
                    writer.close();
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                }
                throw e;
            }
            persistenceWriter.set(writer);
        }

        @Override
        public long getCurrentTerm() {
            return currentTerm;
        }

        @Override
        public ClusterState getLastAcceptedState() {
            return lastAcceptedState;
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            try {
                if (writeNextStateFully) {
                    getWriterSafe().writeFullStateAndCommit(currentTerm, lastAcceptedState);
                    writeNextStateFully = false;
                } else {
                    getWriterSafe().writeIncrementalTermUpdateAndCommit(currentTerm, lastAcceptedState.version());
                }
            } catch (Exception e) {
                handleExceptionOnWrite(e);
            }
            this.currentTerm = currentTerm;
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            try {
                if (writeNextStateFully) {
                    getWriterSafe().writeFullStateAndCommit(currentTerm, clusterState);
                    writeNextStateFully = false;
                } else {
                    if (clusterState.term() != lastAcceptedState.term()) {
                        assert clusterState.term() > lastAcceptedState.term() : clusterState.term() + " vs " + lastAcceptedState.term();
                        // In a new currentTerm, we cannot compare the persisted metadata's lastAcceptedVersion to those in the new state,
                        // so it's simplest to write everything again.
                        getWriterSafe().writeFullStateAndCommit(currentTerm, clusterState);
                    } else {
                        // Within the same currentTerm, we _can_ use metadata versions to skip unnecessary writing.
                        getWriterSafe().writeIncrementalStateAndCommit(currentTerm, lastAcceptedState, clusterState);
                    }
                }
            } catch (Exception e) {
                handleExceptionOnWrite(e);
            }

            lastAcceptedState = clusterState;
        }

        private PersistedClusterStateService.Writer getWriterSafe() {
            final PersistedClusterStateService.Writer writer = persistenceWriter.get();
            if (writer == null) {
                throw new AlreadyClosedException("persisted state has been closed");
            }
            if (writer.isOpen()) {
                return writer;
            } else {
                try {
                    final PersistedClusterStateService.Writer newWriter = persistedClusterStateService.createWriter();
                    if (persistenceWriter.compareAndSet(writer, newWriter)) {
                        return newWriter;
                    } else {
                        assert persistenceWriter.get() == null : "expected no concurrent calls to getWriterSafe";
                        newWriter.close();
                        throw new AlreadyClosedException("persisted state has been closed");
                    }
                } catch (Exception e) {
                    throw Exceptions.toRuntimeException(e);
                }
            }
        }

        private void handleExceptionOnWrite(Exception e) {
            writeNextStateFully = true;
            throw Exceptions.toRuntimeException(e);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(persistenceWriter.getAndSet(null));
        }
    }
}
