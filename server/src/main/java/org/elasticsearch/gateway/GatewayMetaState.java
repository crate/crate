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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import io.crate.common.collections.Tuple;
import org.elasticsearch.common.settings.Settings;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * This class is responsible for storing/retrieving metadata to/from disk.
 * When instance of this class is created, constructor ensures that this version is compatible with state stored on disk and performs
 * state upgrade if necessary. Also it checks that atomic move is supported on the filesystem level, because it's a must for metadata
 * store algorithm.
 * Please note that the state being loaded when constructing the instance of this class is NOT the state that will be used as a
 * {@link ClusterState#metadata()}. Instead when node is starting up, it calls {@link #getMetadata()} method and if this node is
 * elected as master, it requests metadata from other master eligible nodes. After that, master node performs re-conciliation on the
 * gathered results, re-creates {@link ClusterState} and broadcasts this state to other nodes in the cluster.
 */
public class GatewayMetaState implements ClusterStateApplier, CoordinationState.PersistedState {

    protected static final Logger LOGGER = LogManager.getLogger(GatewayMetaState.class);

    private final NodeEnvironment nodeEnv;
    private final MetaStateService metaStateService;
    private final Settings settings;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final TransportService transportService;

    //there is a single thread executing updateClusterState calls, hence no volatile modifier
    protected Manifest previousManifest;
    protected ClusterState previousClusterState;
    protected boolean incrementalWrite;

    public GatewayMetaState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService,
                            MetadataIndexUpgradeService metadataIndexUpgradeService, MetadataUpgrader metadataUpgrader,
                            TransportService transportService, ClusterService clusterService,
                            IndicesService indicesService) throws IOException {
        this.settings = settings;
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;

        upgradeMetadata(metadataIndexUpgradeService, metadataUpgrader);
        initializeClusterState(ClusterName.CLUSTER_NAME_SETTING.get(settings));
        incrementalWrite = false;
    }

    public PersistedState getPersistedState(Settings settings, ClusterApplierService clusterApplierService) {
        applyClusterStateUpdaters();
        if (DiscoveryNode.isMasterNode(settings) == false) {
            // use Zen1 way of writing cluster state for non-master-eligible nodes
            // this avoids concurrent manipulating of IndexMetadata with IndicesStore
            clusterApplierService.addLowPriorityApplier(this);
            return new InMemoryPersistedState(getCurrentTerm(), getLastAcceptedState());
        }
        return this;
    }

    private void initializeClusterState(ClusterName clusterName) throws IOException {
        long startNS = System.nanoTime();
        Tuple<Manifest, Metadata> manifestAndMetadata = metaStateService.loadFullState();
        previousManifest = manifestAndMetadata.v1();

        final Metadata metadata = manifestAndMetadata.v2();

        previousClusterState = ClusterState.builder(clusterName)
                .version(previousManifest.getClusterStateVersion())
                .metadata(metadata).build();

        LOGGER.debug("took {} to load state", TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - startNS)));
    }

    public void applyClusterStateUpdaters() {
        assert previousClusterState.nodes().getLocalNode() == null : "applyClusterStateUpdaters must only be called once";
        assert transportService.getLocalNode() != null : "transport service is not yet started";

        previousClusterState = Function.<ClusterState>identity()
            .andThen(ClusterStateUpdaters::addStateNotRecoveredBlock)
            .andThen(state -> ClusterStateUpdaters.setLocalNode(state, transportService.getLocalNode()))
            .andThen(state -> ClusterStateUpdaters.upgradeAndArchiveUnknownOrInvalidSettings(state, clusterService.getClusterSettings()))
            .andThen(ClusterStateUpdaters::recoverClusterBlocks)
            .apply(previousClusterState);
    }

    protected void upgradeMetadata(MetadataIndexUpgradeService metadataIndexUpgradeService, MetadataUpgrader metadataUpgrader)
            throws IOException {
        if (isMasterOrDataNode()) {
            try {
                final Tuple<Manifest, Metadata> metaStateAndData = metaStateService.loadFullState();
                final Manifest manifest = metaStateAndData.v1();
                final Metadata metadata = metaStateAndData.v2();

                // We finished global state validation and successfully checked all indices for backward compatibility
                // and found no non-upgradable indices, which means the upgrade can continue.
                // Now it's safe to overwrite global and index metadata.
                // We don't re-write metadata if it's not upgraded by upgrade plugins, because
                // if there is manifest file, it means metadata is properly persisted to all data paths
                // if there is no manifest file (upgrade from 6.x to 7.x) metadata might be missing on some data paths,
                // but anyway we will re-write it as soon as we receive first ClusterState
                final AtomicClusterStateWriter writer = new AtomicClusterStateWriter(metaStateService, manifest);
                final Metadata upgradedMetadata = upgradeMetadata(metadata, metadataIndexUpgradeService, metadataUpgrader);

                final long globalStateGeneration;
                if (Metadata.isGlobalStateEquals(metadata, upgradedMetadata) == false) {
                    globalStateGeneration = writer.writeGlobalState("upgrade", upgradedMetadata);
                } else {
                    globalStateGeneration = manifest.getGlobalGeneration();
                }

                Map<Index, Long> indices = new HashMap<>(manifest.getIndexGenerations());
                for (IndexMetadata indexMetadata : upgradedMetadata) {
                    if (metadata.hasIndexMetadata(indexMetadata) == false) {
                        final long generation = writer.writeIndex("upgrade", indexMetadata);
                        indices.put(indexMetadata.getIndex(), generation);
                    }
                }

                final Manifest newManifest = new Manifest(manifest.getCurrentTerm(), manifest.getClusterStateVersion(),
                        globalStateGeneration, indices);
                writer.writeManifestAndCleanup("startup", newManifest);
            } catch (Exception e) {
                LOGGER.error("failed to read or upgrade local state, exiting...", e);
                throw e;
            }
        }
    }

    protected boolean isMasterOrDataNode() {
        return DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings);
    }

    public Metadata getMetadata() {
        return previousClusterState.metadata();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (isMasterOrDataNode() == false) {
            return;
        }

        if (event.state().blocks().disableStatePersistence()) {
            incrementalWrite = false;
            return;
        }

        try {
            // Hack: This is to ensure that non-master-eligible Zen2 nodes always store a current term
            // that's higher than the last accepted term.
            // TODO: can we get rid of this hack?
            if (event.state().term() > getCurrentTerm()) {
                innerSetCurrentTerm(event.state().term());
            }

            updateClusterState(event.state(), event.previousState());
            incrementalWrite = true;
        } catch (WriteStateException e) {
            LOGGER.warn("Exception occurred when storing new meta data", e);
        }
    }

    @Override
    public long getCurrentTerm() {
        return previousManifest.getCurrentTerm();
    }

    @Override
    public ClusterState getLastAcceptedState() {
        assert previousClusterState.nodes().getLocalNode() != null : "Cluster state is not fully built yet";
        return previousClusterState;
    }

    @Override
    public void setCurrentTerm(long currentTerm) {
        try {
            innerSetCurrentTerm(currentTerm);
        } catch (WriteStateException e) {
            LOGGER.error(new ParameterizedMessage("Failed to set current term to {}", currentTerm), e);
            e.rethrowAsErrorOrUncheckedException();
        }
    }

    private void innerSetCurrentTerm(long currentTerm) throws WriteStateException {
        Manifest manifest = new Manifest(currentTerm, previousManifest.getClusterStateVersion(), previousManifest.getGlobalGeneration(),
            new HashMap<>(previousManifest.getIndexGenerations()));
        metaStateService.writeManifestAndCleanup("current term changed", manifest);
        previousManifest = manifest;
    }

    @Override
    public void setLastAcceptedState(ClusterState clusterState) {
        try {
            incrementalWrite = previousClusterState.term() == clusterState.term();
            updateClusterState(clusterState, previousClusterState);
        } catch (WriteStateException e) {
            LOGGER.error(new ParameterizedMessage("Failed to set last accepted state with version {}", clusterState.version()), e);
            e.rethrowAsErrorOrUncheckedException();
        }
    }

    /**
     * This class is used to write changed global {@link Metadata}, {@link IndexMetadata} and {@link Manifest} to disk.
     * This class delegates <code>write*</code> calls to corresponding write calls in {@link MetaStateService} and
     * additionally it keeps track of cleanup actions to be performed if transaction succeeds or fails.
     */
    static class AtomicClusterStateWriter {
        private static final String FINISHED_MSG = "AtomicClusterStateWriter is finished";
        private final List<Runnable> commitCleanupActions;
        private final List<Runnable> rollbackCleanupActions;
        private final Manifest previousManifest;
        private final MetaStateService metaStateService;
        private boolean finished;

        AtomicClusterStateWriter(MetaStateService metaStateService, Manifest previousManifest) {
            this.metaStateService = metaStateService;
            assert previousManifest != null;
            this.previousManifest = previousManifest;
            this.commitCleanupActions = new ArrayList<>();
            this.rollbackCleanupActions = new ArrayList<>();
            this.finished = false;
        }

        long writeGlobalState(String reason, Metadata metadata) throws WriteStateException {
            assert finished == false : FINISHED_MSG;
            try {
                rollbackCleanupActions.add(() -> metaStateService.cleanupGlobalState(previousManifest.getGlobalGeneration()));
                long generation = metaStateService.writeGlobalState(reason, metadata);
                commitCleanupActions.add(() -> metaStateService.cleanupGlobalState(generation));
                return generation;
            } catch (WriteStateException e) {
                rollback();
                throw e;
            }
        }

        long writeIndex(String reason, IndexMetadata metadata) throws WriteStateException {
            assert finished == false : FINISHED_MSG;
            try {
                Index index = metadata.getIndex();
                Long previousGeneration = previousManifest.getIndexGenerations().get(index);
                if (previousGeneration != null) {
                    // we prefer not to clean-up index metadata in case of rollback,
                    // if it's not referenced by previous manifest file
                    // not to break dangling indices functionality
                    rollbackCleanupActions.add(() -> metaStateService.cleanupIndex(index, previousGeneration));
                }
                long generation = metaStateService.writeIndex(reason, metadata);
                commitCleanupActions.add(() -> metaStateService.cleanupIndex(index, generation));
                return generation;
            } catch (WriteStateException e) {
                rollback();
                throw e;
            }
        }

        long writeManifestAndCleanup(String reason, Manifest manifest) throws WriteStateException {
            assert finished == false : FINISHED_MSG;
            try {
                long generation = metaStateService.writeManifestAndCleanup(reason, manifest);
                commitCleanupActions.forEach(Runnable::run);
                finished = true;
                return generation;
            } catch (WriteStateException e) {
                // if Manifest write results in dirty WriteStateException it's not safe to remove
                // new metadata files, because if Manifest was actually written to disk and its deletion
                // fails it will reference these new metadata files.
                // In the future, we might decide to add more fine grained check to understand if after
                // WriteStateException Manifest deletion has actually failed.
                if (e.isDirty() == false) {
                    rollback();
                }
                throw e;
            }
        }

        void rollback() {
            rollbackCleanupActions.forEach(Runnable::run);
            finished = true;
        }
    }

    /**
     * Updates manifest and meta data on disk.
     *
     * @param newState new {@link ClusterState}
     * @param previousState previous {@link ClusterState}
     *
     * @throws WriteStateException if exception occurs. See also {@link WriteStateException#isDirty()}.
     */
    protected void updateClusterState(ClusterState newState, ClusterState previousState)
            throws WriteStateException {
        Metadata newMetadata = newState.metadata();

        final AtomicClusterStateWriter writer = new AtomicClusterStateWriter(metaStateService, previousManifest);
        long globalStateGeneration = writeGlobalState(writer, newMetadata);
        Map<Index, Long> indexGenerations = writeIndicesMetadata(writer, newState, previousState);
        Manifest manifest = new Manifest(previousManifest.getCurrentTerm(), newState.version(), globalStateGeneration, indexGenerations);
        writeManifest(writer, manifest);

        previousManifest = manifest;
        previousClusterState = newState;
    }

    private void writeManifest(AtomicClusterStateWriter writer, Manifest manifest) throws WriteStateException {
        if (manifest.equals(previousManifest) == false) {
            writer.writeManifestAndCleanup("changed", manifest);
        }
    }

    private Map<Index, Long> writeIndicesMetadata(AtomicClusterStateWriter writer, ClusterState newState, ClusterState previousState)
            throws WriteStateException {
        Map<Index, Long> previouslyWrittenIndices = previousManifest.getIndexGenerations();
        Set<Index> relevantIndices = getRelevantIndices(newState, previousState, previouslyWrittenIndices.keySet());

        Map<Index, Long> newIndices = new HashMap<>();

        Metadata previousMetadata = incrementalWrite ? previousState.metadata() : null;
        Iterable<IndexMetadataAction> actions = resolveIndexMetadataActions(previouslyWrittenIndices, relevantIndices, previousMetadata,
                newState.metadata());

        for (IndexMetadataAction action : actions) {
            long generation = action.execute(writer);
            newIndices.put(action.getIndex(), generation);
        }

        return newIndices;
    }

    private long writeGlobalState(AtomicClusterStateWriter writer, Metadata newMetadata)
            throws WriteStateException {
        if (incrementalWrite == false || Metadata.isGlobalStateEquals(previousClusterState.metadata(), newMetadata) == false) {
            return writer.writeGlobalState("changed", newMetadata);
        }
        return previousManifest.getGlobalGeneration();
    }

    public static Set<Index> getRelevantIndices(ClusterState state, ClusterState previousState, Set<Index> previouslyWrittenIndices) {
        Set<Index> relevantIndices;
        if (isDataOnlyNode(state)) {
            relevantIndices = getRelevantIndicesOnDataOnlyNode(state, previousState, previouslyWrittenIndices);
        } else if (state.nodes().getLocalNode().isMasterNode()) {
            relevantIndices = getRelevantIndicesForMasterEligibleNode(state);
        } else {
            relevantIndices = Collections.emptySet();
        }
        return relevantIndices;
    }

    private static boolean isDataOnlyNode(ClusterState state) {
        return ((state.nodes().getLocalNode().isMasterNode() == false) && state.nodes().getLocalNode().isDataNode());
    }

    /**
     * Elasticsearch 2.0 removed several deprecated features and as well as support for Lucene 3.x. This method calls
     * {@link MetadataIndexUpgradeService} to makes sure that indices are compatible with the current version. The
     * MetadataIndexUpgradeService might also update obsolete settings if needed.
     * Allows upgrading global custom meta data via {@link MetadataUpgrader#customMetadataUpgraders}
     *
     * @return input <code>metadata</code> if no upgrade is needed or an upgraded metadata
     */
    static Metadata upgradeMetadata(Metadata metadata,
                                    MetadataIndexUpgradeService metadataIndexUpgradeService,
                                    MetadataUpgrader metadataUpgrader) {
        // upgrade index meta data
        boolean changed = false;
        final Metadata.Builder upgradedMetadata = Metadata.builder(metadata);
        for (IndexMetadata indexMetadata : metadata) {
            IndexMetadata newMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(indexMetadata,
                    Version.CURRENT.minimumIndexCompatibilityVersion());
            changed |= indexMetadata != newMetadata;
            upgradedMetadata.put(newMetadata, false);
        }
        // upgrade global custom meta data
        if (applyPluginUpgraders(metadata.getCustoms(), metadataUpgrader.customMetadataUpgraders,
                upgradedMetadata::removeCustom, upgradedMetadata::putCustom)) {
            changed = true;
        }
        // upgrade current templates
        if (applyPluginUpgraders(metadata.getTemplates(), metadataUpgrader.indexTemplateMetadataUpgraders,
                upgradedMetadata::removeTemplate, (s, indexTemplateMetadata) -> upgradedMetadata.put(indexTemplateMetadata))) {
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

    /**
     * Returns list of {@link IndexMetadataAction} for each relevant index.
     * For each relevant index there are 3 options:
     * <ol>
     * <li>
     * {@link KeepPreviousGeneration} - index metadata is already stored to disk and index metadata version is not changed, no
     * action is required.
     * </li>
     * <li>
     * {@link WriteNewIndexMetadata} - there is no index metadata on disk and index metadata for this index should be written.
     * </li>
     * <li>
     * {@link WriteChangedIndexMetadata} - index metadata is already on disk, but index metadata version has changed. Updated
     * index metadata should be written to disk.
     * </li>
     * </ol>
     *
     * @param previouslyWrittenIndices A list of indices for which the state was already written before
     * @param relevantIndices          The list of indices for which state should potentially be written
     * @param previousMetadata         The last meta data we know of
     * @param newMetadata              The new metadata
     * @return list of {@link IndexMetadataAction} for each relevant index.
     */
    public static List<IndexMetadataAction> resolveIndexMetadataActions(Map<Index, Long> previouslyWrittenIndices,
                                                                        Set<Index> relevantIndices,
                                                                        Metadata previousMetadata,
                                                                        Metadata newMetadata) {
        List<IndexMetadataAction> actions = new ArrayList<>();
        for (Index index : relevantIndices) {
            IndexMetadata newIndexMetadata = newMetadata.getIndexSafe(index);
            IndexMetadata previousIndexMetadata = previousMetadata == null ? null : previousMetadata.index(index);

            if (previouslyWrittenIndices.containsKey(index) == false || previousIndexMetadata == null) {
                actions.add(new WriteNewIndexMetadata(newIndexMetadata));
            } else if (previousIndexMetadata.getVersion() != newIndexMetadata.getVersion()) {
                actions.add(new WriteChangedIndexMetadata(previousIndexMetadata, newIndexMetadata));
            } else {
                actions.add(new KeepPreviousGeneration(index, previouslyWrittenIndices.get(index)));
            }
        }
        return actions;
    }

    private static Set<Index> getRelevantIndicesOnDataOnlyNode(ClusterState state, ClusterState previousState, Set<Index>
            previouslyWrittenIndices) {
        RoutingNode newRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (newRoutingNode == null) {
            throw new IllegalStateException("cluster state does not contain this node - cannot write index meta state");
        }
        Set<Index> indices = new HashSet<>();
        for (ShardRouting routing : newRoutingNode) {
            indices.add(routing.index());
        }
        // we have to check the meta data also: closed indices will not appear in the routing table, but we must still write the state if
        // we have it written on disk previously
        for (IndexMetadata indexMetadata : state.metadata()) {
            boolean isOrWasClosed = indexMetadata.getState().equals(IndexMetadata.State.CLOSE);
            // if the index is open we might still have to write the state if it just transitioned from closed to open
            // so we have to check for that as well.
            IndexMetadata previousMetadata = previousState.metadata().index(indexMetadata.getIndex());
            if (previousMetadata != null) {
                isOrWasClosed = isOrWasClosed || previousMetadata.getState().equals(IndexMetadata.State.CLOSE);
            }
            if (previouslyWrittenIndices.contains(indexMetadata.getIndex()) && isOrWasClosed) {
                indices.add(indexMetadata.getIndex());
            }
        }
        return indices;
    }

    private static Set<Index> getRelevantIndicesForMasterEligibleNode(ClusterState state) {
        Set<Index> relevantIndices;
        relevantIndices = new HashSet<>();
        // we have to iterate over the metadata to make sure we also capture closed indices
        for (IndexMetadata indexMetadata : state.metadata()) {
            relevantIndices.add(indexMetadata.getIndex());
        }
        return relevantIndices;
    }

    /**
     * Action to perform with index metadata.
     */
    public interface IndexMetadataAction {
        /**
         * @return index for index metadata.
         */
        Index getIndex();

        /**
         * Executes this action using provided {@link AtomicClusterStateWriter}.
         *
         * @return new index metadata state generation, to be used in manifest file.
         * @throws WriteStateException if exception occurs.
         */
        long execute(AtomicClusterStateWriter writer) throws WriteStateException;
    }

    public static class KeepPreviousGeneration implements IndexMetadataAction {
        private final Index index;
        private final long generation;

        KeepPreviousGeneration(Index index, long generation) {
            this.index = index;
            this.generation = generation;
        }

        @Override
        public Index getIndex() {
            return index;
        }

        @Override
        public long execute(AtomicClusterStateWriter writer) {
            return generation;
        }
    }

    public static class WriteNewIndexMetadata implements IndexMetadataAction {
        private final IndexMetadata indexMetadata;

        WriteNewIndexMetadata(IndexMetadata indexMetadata) {
            this.indexMetadata = indexMetadata;
        }

        @Override
        public Index getIndex() {
            return indexMetadata.getIndex();
        }

        @Override
        public long execute(AtomicClusterStateWriter writer) throws WriteStateException {
            return writer.writeIndex("freshly created", indexMetadata);
        }
    }

    public static class WriteChangedIndexMetadata implements IndexMetadataAction {
        private final IndexMetadata newIndexMetadata;
        private final IndexMetadata oldIndexMetadata;

        WriteChangedIndexMetadata(IndexMetadata oldIndexMetadata, IndexMetadata newIndexMetadata) {
            this.oldIndexMetadata = oldIndexMetadata;
            this.newIndexMetadata = newIndexMetadata;
        }

        @Override
        public Index getIndex() {
            return newIndexMetadata.getIndex();
        }

        @Override
        public long execute(AtomicClusterStateWriter writer) throws WriteStateException {
            return writer.writeIndex(
                    "version changed from [" + oldIndexMetadata.getVersion() + "] to [" + newIndexMetadata.getVersion() + "]",
                    newIndexMetadata);
        }
    }
}
