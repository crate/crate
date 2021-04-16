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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import io.crate.common.collections.Tuple;
import org.elasticsearch.common.settings.Settings;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Loads (and maybe upgrades) cluster metadata at startup, and persistently stores cluster metadata for future restarts.
 *
 * When started, ensures that this version is compatible with the state stored on disk, and performs a state upgrade if necessary. Note that
 * the state being loaded when constructing the instance of this class is not necessarily the state that will be used as {@link
 * ClusterState#metadata()} because it might be stale or incomplete. Master-eligible nodes must perform an election to find a complete and
 * non-stale state, and master-ineligible nodes receive the real cluster state from the elected master after joining the cluster.
 */
public class GatewayMetaState {
    private static final Logger LOGGER = LogManager.getLogger(GatewayMetaState.class);

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
                      MetadataUpgrader metadataUpgrader) {
        assert persistedState.get() == null : "should only start once, but already have " + persistedState.get();

        final Tuple<Manifest, ClusterState> manifestClusterStateTuple;
        try {
            upgradeMetadata(settings, metaStateService, metadataIndexUpgradeService, metadataUpgrader);
            manifestClusterStateTuple = loadStateAndManifest(ClusterName.CLUSTER_NAME_SETTING.get(settings), metaStateService);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to load metadata", e);
        }

        final IncrementalClusterStateWriter incrementalClusterStateWriter
            = new IncrementalClusterStateWriter(settings, clusterService.getClusterSettings(), metaStateService,
                manifestClusterStateTuple.v1(),
                prepareInitialClusterState(transportService, clusterService, manifestClusterStateTuple.v2()),
                transportService.getThreadPool()::relativeTimeInMillis);
        if (DiscoveryNode.isMasterNode(settings) == false) {
            if (DiscoveryNode.isDataNode(settings)) {
                // Master-eligible nodes persist index metadata for all indices regardless of whether they hold any shards or not. It's
                // vitally important to the safety of the cluster coordination system that master-eligible nodes persist this metadata when
                // _accepting_ the cluster state (i.e. before it is committed). This persistence happens on the generic threadpool.
                //
                // In contrast, master-ineligible data nodes only persist the index metadata for shards that they hold. When all shards of
                // an index are moved off such a node the IndicesStore is responsible for removing the corresponding index directory,
                // including the metadata, and does so on the cluster applier thread.
                //
                // This presents a problem: if a shard is unassigned from a node and then reassigned back to it again then there is a race
                // between the IndicesStore deleting the index folder and the CoordinationState concurrently trying to write the updated
                // metadata into it. We could probably solve this with careful synchronization, but in fact there is no need.  The persisted
                // state on master-ineligible data nodes is mostly ignored - it's only there to support dangling index imports, which is
                // inherently unsafe anyway. Thus we can safely delay metadata writes on master-ineligible data nodes until applying the
                // cluster state, which is what this does:
                clusterService.addLowPriorityApplier(new GatewayClusterApplier(incrementalClusterStateWriter));
            }

            // Master-ineligible nodes do not need to persist the cluster state when accepting it because they are not in the voting
            // configuration, so it's ok if they have a stale or incomplete cluster state when restarted. We track the latest cluster state
            // in memory instead.
            persistedState.set(new InMemoryPersistedState(manifestClusterStateTuple.v1().getCurrentTerm(), manifestClusterStateTuple.v2()));
        } else {
            // Master-ineligible nodes must persist the cluster state when accepting it because they must reload the (complete, fresh)
            // last-accepted cluster state when restarted.
            persistedState.set(new GatewayPersistedState(incrementalClusterStateWriter));
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
    void upgradeMetadata(Settings settings, MetaStateService metaStateService, MetadataIndexUpgradeService metadataIndexUpgradeService,
                         MetadataUpgrader metadataUpgrader) throws IOException {
        if (isMasterOrDataNode(settings)) {
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
                final IncrementalClusterStateWriter.AtomicClusterStateWriter writer
                    = new IncrementalClusterStateWriter.AtomicClusterStateWriter(metaStateService, manifest);
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

    private static Tuple<Manifest,ClusterState> loadStateAndManifest(ClusterName clusterName,
                                                                     MetaStateService metaStateService) throws IOException {
        final long startNS = System.nanoTime();
        final Tuple<Manifest, Metadata> manifestAndMetadata = metaStateService.loadFullState();
        final Manifest manifest = manifestAndMetadata.v1();

        final ClusterState clusterState = ClusterState.builder(clusterName)
            .version(manifest.getClusterStateVersion())
            .metadata(manifestAndMetadata.v2()).build();

        LOGGER.debug("took {} to load state", TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - startNS)));

        return Tuple.tuple(manifest, clusterState);
    }

    private static boolean isMasterOrDataNode(Settings settings) {
        return DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings);
    }

    /**
     * Elasticsearch 2.0 removed several deprecated features and as well as support for Lucene 3.x. This method calls
     * {@link MetadataIndexUpgradeService} to makes sure that indices are compatible with the current version. The
     * MetadataIndexUpgradeService might also update obsolete settings if needed.
     * Allows upgrading global custom meta data via {@link MetadataUpgrader#customMetadataUpgraders}
     *
     * @return input <code>metadata</code> if no upgrade is needed or an upgraded metadata
     */
    public static Metadata upgradeMetadata(Metadata metadata,
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


    private static class GatewayClusterApplier implements ClusterStateApplier {

        private final IncrementalClusterStateWriter incrementalClusterStateWriter;

        private GatewayClusterApplier(IncrementalClusterStateWriter incrementalClusterStateWriter) {
            this.incrementalClusterStateWriter = incrementalClusterStateWriter;
        }

        @Override
        public void applyClusterState(ClusterChangedEvent event) {
            if (event.state().blocks().disableStatePersistence()) {
                incrementalClusterStateWriter.setIncrementalWrite(false);
                return;
            }

            try {
                // Hack: This is to ensure that non-master-eligible Zen2 nodes always store a current term
                // that's higher than the last accepted term.
                // TODO: can we get rid of this hack?
                if (event.state().term() > incrementalClusterStateWriter.getPreviousManifest().getCurrentTerm()) {
                    incrementalClusterStateWriter.setCurrentTerm(event.state().term());
                }

                incrementalClusterStateWriter.updateClusterState(event.state());
                incrementalClusterStateWriter.setIncrementalWrite(true);
            } catch (WriteStateException e) {
                LOGGER.warn("Exception occurred when storing new meta data", e);
            }
        }

    }

    private static class GatewayPersistedState implements PersistedState {

        private final IncrementalClusterStateWriter incrementalClusterStateWriter;

        GatewayPersistedState(IncrementalClusterStateWriter incrementalClusterStateWriter) {
            this.incrementalClusterStateWriter = incrementalClusterStateWriter;
        }

        @Override
        public long getCurrentTerm() {
            return incrementalClusterStateWriter.getPreviousManifest().getCurrentTerm();
        }

        @Override
        public ClusterState getLastAcceptedState() {
            final ClusterState previousClusterState = incrementalClusterStateWriter.getPreviousClusterState();
            assert previousClusterState.nodes().getLocalNode() != null : "Cluster state is not fully built yet";
            return previousClusterState;
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            try {
                incrementalClusterStateWriter.setCurrentTerm(currentTerm);
            } catch (WriteStateException e) {
                LOGGER.error(new ParameterizedMessage("Failed to set current term to {}", currentTerm), e);
                e.rethrowAsErrorOrUncheckedException();
            }
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            try {
                incrementalClusterStateWriter.setIncrementalWrite(
                    incrementalClusterStateWriter.getPreviousClusterState().term() == clusterState.term());
                incrementalClusterStateWriter.updateClusterState(clusterState);
            } catch (WriteStateException e) {
                LOGGER.error(new ParameterizedMessage("Failed to set last accepted state with version {}", clusterState.version()), e);
                e.rethrowAsErrorOrUncheckedException();
            }
        }

    }

}
