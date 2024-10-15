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

package org.elasticsearch.cluster.metadata;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.ack.CreateIndexClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.indices.IndexCreationException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.unit.TimeValue;
import io.crate.execution.ddl.tables.CreateTableRequest;
import io.crate.execution.ddl.tables.MappingUtil;
import io.crate.metadata.DocReferences;
import io.crate.metadata.IndexName;
import io.crate.metadata.IndexReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfoFactory;

/**
 * Service responsible for submitting create index requests
 */
public class MetadataCreateIndexService {

    private static final Logger LOGGER = LogManager.getLogger(MetadataCreateIndexService.class);

    private final NodeContext nodeContext;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final Environment env;
    private final IndexScopedSettings indexScopedSettings;
    private final ActiveShardsObserver activeShardsObserver;
    private final boolean forbidPrivateIndexSettings;
    private final Settings settings;
    private final ShardLimitValidator shardLimitValidator;

    public MetadataCreateIndexService(NodeContext nodeContext,
                                      Settings settings,
                                      ClusterService clusterService,
                                      IndicesService indicesService,
                                      AllocationService allocationService,
                                      ShardLimitValidator shardLimitValidator,
                                      Environment env,
                                      IndexScopedSettings indexScopedSettings,
                                      ThreadPool threadPool,
                                      boolean forbidPrivateIndexSettings) {
        this.nodeContext = nodeContext;
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.env = env;
        this.indexScopedSettings = indexScopedSettings;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService);
        this.forbidPrivateIndexSettings = forbidPrivateIndexSettings;
        this.shardLimitValidator = shardLimitValidator;
    }

    /**
     * Validate the name for an index against some static rules and a cluster state.
     */
    public static void validateIndexName(String index, ClusterState state) {
        IndexName.validate(index);
        if (state.routingTable().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.routingTable().index(index).getIndex());
        }
        if (state.metadata().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.metadata().index(index).getIndex());
        }
        if (state.metadata().hasAlias(index)) {
            throw new InvalidIndexNameException(index, "already exists as alias");
        }
    }

    /**
     * @param createResponse params: (clusterStateAcknowledged, shardsAcknowledged)
     **/
    public <T> ActionListener<ClusterStateUpdateResponse> withWaitForShards(ActionListener<T> listener,
                                                                            String indexName,
                                                                            ActiveShardCount waitForActiveShards,
                                                                            TimeValue ackTimeout,
                                                                            BiFunction<Boolean, Boolean, T> createResponse) {
        return ActionListener.wrap(
            resp -> {
                if (resp.isAcknowledged()) {
                    String[] indexNames = new String[] { indexName };
                    activeShardsObserver.waitForActiveShards(
                        indexNames,
                        waitForActiveShards,
                        ackTimeout,
                        shardsAcknowledged -> {
                            if (shardsAcknowledged == false) {
                                LOGGER.debug(
                                    "[{}] index created, but the operation timed out waiting for enough shards to be started.",
                                    indexName
                                );

                                // onlyCreateIndex is acknowledged, so global OID is already advanced.
                                // CREATE TABLE is not successful because of timeout which means that we can
                                // have holes in OID sequence.
                                // However, there won't be any duplicates so it's still safe to use OIDs as
                                // source column names.
                            }
                            listener.onResponse(createResponse.apply(resp.isAcknowledged(), shardsAcknowledged));
                        },
                        listener::onFailure
                    );
                } else {
                    listener.onResponse(createResponse.apply(false, false));
                }
            },
            listener::onFailure
        );
    }

    /**
     * Creates an index in the cluster state and waits for the specified number of shard copies to
     * become active (as specified in {@link CreateIndexClusterStateUpdateRequest#waitForActiveShards()})
     * before sending the response on the listener. If the index creation was successfully applied on
     * the cluster state, then {@link CreateIndexClusterStateUpdateResponse#isAcknowledged()} will return
     * true, otherwise it will return false and no waiting will occur for started shards
     * ({@link CreateIndexClusterStateUpdateResponse#isShardsAcknowledged()} will also be false).  If the index
     * creation in the cluster state was successful and the requisite shard copies were started before
     * the timeout, then {@link CreateIndexClusterStateUpdateResponse#isShardsAcknowledged()} will
     * return true, otherwise if the operation timed out, then it will return false.
     *
     * @param request the index creation cluster state update request
     * @param createTableRequest carries CrateDB specific objects to create mapping if request param above has NULL mapping. Null if used in resize.
     * @param listener the listener on which to send the index creation cluster state update response
     */
    public void createIndex(CreateIndexClusterStateUpdateRequest request,
                            ActionListener<CreateIndexClusterStateUpdateResponse> listener) {
        ActionListener<ClusterStateUpdateResponse> actionListener = withWaitForShards(
            listener,
            request.index(),
            request.waitForActiveShards(),
            request.ackTimeout(),
            CreateIndexClusterStateUpdateResponse::new
        );
        Settings normalizedSettings = Settings.builder()
            .put(request.settings())
            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
            .build();
        indexScopedSettings.validate(normalizedSettings, true); // we do validate here - index setting must be consistent
        request.settings(normalizedSettings);
        clusterService.submitStateUpdateTask(
            "create-index [" + request.index() + "], cause [" + request.cause() + "]",
            new IndexCreationTask(
                    LOGGER,
                    allocationService,
                    request,
                    actionListener,
                    indicesService,
                    settings,
                    this::validate,
                    indexScopedSettings,
                    nodeContext));
    }

    interface IndexValidator {
        void validate(CreateIndexClusterStateUpdateRequest request, ClusterState state);
    }

    static class IndexCreationTask extends AckedClusterStateUpdateTask<ClusterStateUpdateResponse> {

        private final IndicesService indicesService;
        private final CreateIndexClusterStateUpdateRequest request;
        private final Logger logger;
        private final AllocationService allocationService;
        private final Settings settings;
        private final IndexValidator validator;
        private final IndexScopedSettings indexScopedSettings;
        private final NodeContext nodeContext;

        IndexCreationTask(Logger logger,
                          AllocationService allocationService,
                          CreateIndexClusterStateUpdateRequest request,
                          ActionListener<ClusterStateUpdateResponse> listener,
                          IndicesService indicesService,
                          Settings settings,
                          IndexValidator validator,
                          IndexScopedSettings indexScopedSettings,
                          NodeContext nodeContext) {
            super(Priority.URGENT, request, listener);
            this.request = request;
            this.logger = logger;
            this.allocationService = allocationService;
            this.indicesService = indicesService;
            this.settings = settings;
            this.validator = validator;
            this.indexScopedSettings = indexScopedSettings;
            this.nodeContext = nodeContext;
        }

        @Override
        protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
            return new ClusterStateUpdateResponse(acknowledged);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            validator.validate(request, currentState);

            for (Alias alias : request.aliases()) {
                AliasValidator.validateAlias(alias, request.index(), currentState.metadata());
            }

            // No template handling here since neither of usages of this service is relevant to them:
            // 1. Create non-partitioned tables, templates are not related. Partitioned tables are handled by MetadataIndexTemplateService.
            // 2. Resize partitioned tables. findTemplates always returns an empty list since
            //    request.index() has prefix "resized" which doesn't match table pattern. See testShrinkShardsOfPartition
            // 3. Creating partitions/indices by inserting into a partitioned table.
            //    We don't use this service anymore after introducing https://github.com/crate/crate/commit/f1c96b517d6d4f31ada5c7b42da49f5a41c12869
            //    where we have dedicated TransportCreatePartitionsAction, which is an optimized version of MetadataCreateIndexService
            IndexTemplateMetadata template = null;
            try {
                template = currentState.metadata().templates().get(PartitionName.templateName(request.index()));
            } catch (Exception ex) {
                // Creation of regular tables or resizing a partition don't pass validation.
                // Catching validation errors to do a safe assertion.
            }
            assert template == null : String.format(Locale.ENGLISH, "Found a matching template for index %s, invalid usage.", request.index());

            final Index recoverFromIndex = request.recoverFrom();
            MappingMetadata mapping;
            if (recoverFromIndex == null) {
                mapping = new MappingMetadata(Map.of());
            } else {
                IndexMetadata sourceMetadata = currentState.metadata().getIndexSafe(recoverFromIndex);
                mapping = sourceMetadata.mapping();
            }

            Settings.Builder indexSettingsBuilder = Settings.builder()
                .put(request.settings())
                .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, request.getProvidedName())
                .put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .put(SETTING_CREATION_DATE, Instant.now().toEpochMilli());

            if (indexSettingsBuilder.get(SETTING_NUMBER_OF_SHARDS) == null) {
                throw new IllegalArgumentException("Number of shards must be supplied");
            }
            if (request.resizeType() == null && request.copySettings() == false) {
                if (indexSettingsBuilder.get(SETTING_NUMBER_OF_REPLICAS) == null) {
                    indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                }
                if (settings.get(AutoExpandReplicas.SETTING_KEY) != null && indexSettingsBuilder.get(AutoExpandReplicas.SETTING_KEY) == null) {
                    indexSettingsBuilder.put(AutoExpandReplicas.SETTING_KEY, settings.get(AutoExpandReplicas.SETTING_KEY));
                }
            }
            setIndexVersionCreatedSetting(indexSettingsBuilder, currentState);
            validateSoftDeletesSetting(indexSettingsBuilder.build());

            final IndexMetadata.Builder tmpImdBuilder = IndexMetadata.builder(request.index());
            final Settings idxSettings = indexSettingsBuilder.build();
            int numTargetShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(idxSettings);
            final int routingNumShards;
            final Version indexVersionCreated = idxSettings.getAsVersion(IndexMetadata.SETTING_VERSION_CREATED, null);
            final IndexMetadata sourceMetadata = recoverFromIndex == null ? null :
                currentState.metadata().getIndexSafe(recoverFromIndex);
            if (sourceMetadata == null || sourceMetadata.getNumberOfShards() == 1) {
                // in this case we either have no index to recover from or
                // we have a source index with 1 shard and without an explicit split factor
                // or one that is valid in that case we can split into whatever and auto-generate a new factor.
                if (IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(idxSettings)) {
                    routingNumShards = IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(idxSettings);
                } else {
                    routingNumShards = calculateNumRoutingShards(numTargetShards, indexVersionCreated);
                }
            } else {
                assert IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(idxSettings) == false
                    : "index.number_of_routing_shards should not be present on the target index on resize";

                routingNumShards = sourceMetadata.getRoutingNumShards();
            }
            // remove the setting it's temporary and is only relevant once we create the index
            indexSettingsBuilder.remove(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey());
            tmpImdBuilder.setRoutingNumShards(routingNumShards);

            if (recoverFromIndex != null) {
                assert request.resizeType() != null;
                prepareResizeIndexSettings(
                        currentState,
                        indexSettingsBuilder,
                        recoverFromIndex,
                        request.index(),
                        request.resizeType(),
                        request.copySettings(),
                        indexScopedSettings);
            }
            final Settings actualIndexSettings = indexSettingsBuilder.build();
            tmpImdBuilder.settings(actualIndexSettings);

            if (recoverFromIndex != null) {
                /*
                    * We need to arrange that the primary term on all the shards in the shrunken index is at least as large as
                    * the maximum primary term on all the shards in the source index. This ensures that we have correct
                    * document-level semantics regarding sequence numbers in the shrunken index.
                    */
                final long primaryTerm =
                    IntStream
                        .range(0, sourceMetadata.getNumberOfShards())
                        .mapToLong(sourceMetadata::primaryTerm)
                        .max()
                        .getAsLong();
                for (int shardId = 0; shardId < tmpImdBuilder.numberOfShards(); shardId++) {
                    tmpImdBuilder.primaryTerm(shardId, primaryTerm);
                }
            }
            // Set up everything, now locally create the index to see that things are ok, and apply
            final IndexMetadata tmpImd = tmpImdBuilder.build();
            ActiveShardCount waitForActiveShards = request.waitForActiveShards();
            if (waitForActiveShards == ActiveShardCount.DEFAULT) {
                waitForActiveShards = tmpImd.getWaitForActiveShards();
            }
            if (waitForActiveShards.validate(tmpImd.getNumberOfReplicas()) == false) {
                throw new IllegalArgumentException("invalid wait_for_active_shards[" + request.waitForActiveShards() +
                    "]: cannot be greater than number of shard copies [" +
                    (tmpImd.getNumberOfReplicas() + 1) + "]");
            }
            // create the index here (on the master) to validate it can be created, as well as adding the mapping
            return indicesService.withTempIndexService(tmpImd, indexService -> {
                ClusterState updatedState = addIndex(
                    allocationService,
                    indexService,
                    currentState,
                    Metadata.builder(currentState.metadata()),
                    request.index(),
                    tmpImd,
                    mapping,
                    request.aliases(),
                    routingNumShards
                );
                if (!IndexName.isDangling(request.index())) {
                    // temporary index for resize or shrink operation are always allowed
                    var relationName = RelationName.fromIndexName(request.index());
                    new DocTableInfoFactory(nodeContext).create(relationName, updatedState.metadata());
                }
                return updatedState;
            });
        }

        @Override
        public void onFailure(String source, Exception e) {
            if (e instanceof ResourceAlreadyExistsException) {
                logger.trace(() -> new ParameterizedMessage("[{}] failed to create", request.index()), e);
            } else {
                logger.debug(() -> new ParameterizedMessage("[{}] failed to create", request.index()), e);
            }
            super.onFailure(source, e);
        }
    }

    private void validate(CreateIndexClusterStateUpdateRequest request, ClusterState state) {
        String index = request.index();
        IndexName.validate(index);
        if (state.routingTable().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.routingTable().index(index).getIndex());
        }
        if (state.metadata().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.metadata().index(index).getIndex());
        }
        if (state.metadata().hasAlias(index)) {
            throw new InvalidIndexNameException(index, "already exists as alias");
        }
        validateIndexSettings(request.index(), request.settings(), forbidPrivateIndexSettings);
        shardLimitValidator.validateShardLimit(request.settings(), state);
    }

    public void validateIndexSettings(String indexName, final Settings settings, final boolean forbidPrivateIndexSettings) throws IndexCreationException {
        List<String> validationErrors = getIndexSettingsValidationErrors(settings, forbidPrivateIndexSettings);
        if (validationErrors.isEmpty() == false) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new IndexCreationException(indexName, validationException);
        }
    }

    public List<String> getIndexSettingsValidationErrors(final Settings settings, final boolean forbidPrivateIndexSettings) {
        String customPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(settings);
        List<String> validationErrors = new ArrayList<>();
        if (Strings.isNullOrEmpty(customPath) == false && env.sharedDataFile() == null) {
            validationErrors.add("path.shared_data must be set in order to use custom data paths");
        } else if (Strings.isNullOrEmpty(customPath) == false) {
            Path resolvedPath = PathUtils.get(new Path[]{env.sharedDataFile()}, customPath);
            if (resolvedPath == null) {
                validationErrors.add("custom path [" + customPath + "] is not a sub-path of path.shared_data [" + env.sharedDataFile() + "]");
            }
        }
        if (forbidPrivateIndexSettings) {
            for (final String key : settings.keySet()) {
                final Setting<?> setting = indexScopedSettings.get(key);
                if (setting == null) {
                    assert indexScopedSettings.isPrivateSetting(key);
                } else if (setting.isPrivateIndex()) {
                    validationErrors.add("private index setting [" + key + "] can not be set explicitly");
                }
            }
        }
        return validationErrors;
    }

    /**
     * Validates the settings and mappings for shrinking an index.
     * @return the list of nodes at least one instance of the source index shards are allocated
     */
    static List<String> validateShrinkIndex(ClusterState state,
                                            String sourceIndex,
                                            String targetIndexName,
                                            Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexName, targetIndexSettings);
        assert IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings);
        IndexMetadata.selectShrinkShards(0, sourceMetadata, IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));

        if (sourceMetadata.getNumberOfShards() == 1) {
            throw new IllegalArgumentException("can't shrink an index with only one shard");
        }

        // now check that index is all on one node
        final IndexRoutingTable table = state.routingTable().index(sourceIndex);
        Map<String, AtomicInteger> nodesToNumRouting = new HashMap<>();
        int numShards = sourceMetadata.getNumberOfShards();
        for (ShardRouting routing : table.shardsWithState(ShardRoutingState.STARTED)) {
            nodesToNumRouting.computeIfAbsent(routing.currentNodeId(), (s) -> new AtomicInteger(0)).incrementAndGet();
        }
        List<String> nodesToAllocateOn = new ArrayList<>();
        for (Map.Entry<String, AtomicInteger> entries : nodesToNumRouting.entrySet()) {
            int numAllocations = entries.getValue().get();
            assert numAllocations <= numShards : "wait what? " + numAllocations + " is > than num shards " + numShards;
            if (numAllocations == numShards) {
                nodesToAllocateOn.add(entries.getKey());
            }
        }
        if (nodesToAllocateOn.isEmpty()) {
            throw new IllegalStateException("index " + sourceIndex +
                " must have all shards allocated on the same node to shrink index");
        }
        return nodesToAllocateOn;
    }

    static void validateSplitIndex(ClusterState state,
                                   String sourceIndex,
                                   String targetIndexName,
                                   Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexName, targetIndexSettings);
        IndexMetadata.selectSplitShard(0, sourceMetadata, IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static IndexMetadata validateResize(ClusterState state,
                                        String sourceIndex,
                                        String targetIndexName,
                                        Settings targetIndexSettings) {
        if (state.metadata().hasIndex(targetIndexName)) {
            throw new ResourceAlreadyExistsException(state.metadata().index(targetIndexName).getIndex());
        }
        final IndexMetadata sourceMetadata = state.metadata().index(sourceIndex);
        if (sourceMetadata == null) {
            throw new IndexNotFoundException(sourceIndex);
        }
        // ensure index is read-only
        if (state.blocks().indexBlocked(ClusterBlockLevel.WRITE, sourceIndex) == false) {
            throw new IllegalStateException("index " + sourceIndex + " must be read-only to resize index. use \"index.blocks.write=true\"");
        }

        if (IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings)) {
            // this method applies all necessary checks ie. if the target shards are less than the source shards
            // of if the source shards are divisible by the number of target shards
            IndexMetadata.getRoutingFactor(sourceMetadata.getNumberOfShards(),
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
        }
        return sourceMetadata;
    }

    static void prepareResizeIndexSettings(
            final ClusterState currentState,
            final Settings.Builder indexSettingsBuilder,
            final Index resizeSourceIndex,
            final String resizeIntoName,
            final ResizeType type,
            final boolean copySettings,
            final IndexScopedSettings indexScopedSettings) {
        final IndexMetadata sourceMetadata = currentState.metadata().index(resizeSourceIndex.getName());
        if (type == ResizeType.SHRINK) {
            final List<String> nodesToAllocateOn = validateShrinkIndex(
                currentState,
                resizeSourceIndex.getName(),
                resizeIntoName,
                indexSettingsBuilder.build());
            indexSettingsBuilder
                // we use "i.r.a.initial_recovery" rather than "i.r.a.require|include" since we want the replica to allocate right away
                // once we are allocated.
                .put(IndexMetadata.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getKey() + "_id",
                     Strings.arrayToCommaDelimitedString(nodesToAllocateOn.toArray()))
                // we add the legacy way of specifying it here for BWC. We can remove this once it's backported to 6.x
                .put(IndexMetadata.INDEX_SHRINK_SOURCE_NAME.getKey(), resizeSourceIndex.getName())
                .put(IndexMetadata.INDEX_SHRINK_SOURCE_UUID.getKey(), resizeSourceIndex.getUUID());
        } else if (type == ResizeType.SPLIT) {
            validateSplitIndex(currentState, resizeSourceIndex.getName(), resizeIntoName, indexSettingsBuilder.build());
        } else {
            throw new IllegalStateException("unknown resize type is " + type);
        }

        final Settings.Builder builder = Settings.builder();
        if (copySettings) {
            // copy all settings and non-copyable settings and settings that have already been set (e.g., from the request)
            for (final String key : sourceMetadata.getSettings().keySet()) {
                final Setting<?> setting = indexScopedSettings.get(key);
                if (setting == null) {
                    assert indexScopedSettings.isPrivateSetting(key) : key;
                } else if (setting.getProperties().contains(Setting.Property.NotCopyableOnResize)) {
                    continue;
                }
                // do not override settings that have already been set (for example, from the request)
                if (indexSettingsBuilder.keys().contains(key)) {
                    continue;
                }
                builder.copy(key, sourceMetadata.getSettings());
            }
        } else {
            final Predicate<String> sourceSettingsPredicate =
                (s) -> (s.startsWith("index.similarity.") || s.startsWith("index.analysis.") || s.startsWith("index.sort.") ||
                        s.equals("index.mapping.single_type") || s.equals("index.soft_deletes.enabled"))
                        && indexSettingsBuilder.keys().contains(s) == false;
            builder.put(sourceMetadata.getSettings().filter(sourceSettingsPredicate));
        }

        indexSettingsBuilder
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), sourceMetadata.getCreationVersion())
            .put(IndexMetadata.SETTING_VERSION_UPGRADED, sourceMetadata.getUpgradedVersion())
            .put(builder.build())
            .put(IndexMetadata.SETTING_ROUTING_PARTITION_SIZE, sourceMetadata.getRoutingPartitionSize())
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), resizeSourceIndex.getName())
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.getKey(), resizeSourceIndex.getUUID());
    }

    public static void validateSoftDeletesSetting(Settings settings) {
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(settings) == false
            && IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings).onOrAfter(Version.V_5_0_0)) {
            throw new IllegalArgumentException(
                "Creating tables with soft-deletes disabled is no longer supported. "
                + "Please do not specify a value for setting [soft_deletes.enabled]."
            );
        }
    }

    public static void setIndexVersionCreatedSetting(Settings.Builder indexSettingsBuilder, ClusterState clusterState) {
        if (indexSettingsBuilder.get(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey()) == null) {
            final DiscoveryNodes nodes = clusterState.nodes();
            final Version createdVersion = Version.min(Version.CURRENT,
                                                       nodes.getSmallestNonClientNodeVersion());
            indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), createdVersion);
        }
    }

    /**
     * Returns a default number of routing shards based on the number of shards of the index. The default number of routing shards will
     * allow any index to be split at least once and at most 10 times by a factor of two. The closer the number or shards gets to 1024
     * the less default split operations are supported
     */
    public static int calculateNumRoutingShards(int numShards, Version indexVersionCreated) {
        if (indexVersionCreated.onOrAfter(Version.V_5_8_0)) {
            // only select this automatically for indices that are created on or after 5.8, this will prevent this new behaviour
            // until we have a fully upgraded cluster. Additionally, it will make integrating testing easier since mixed clusters
            // will always have the behavior of the min node in the cluster.
            //
            // We use as a default number of routing shards the higher number that can be expressed
            // as {@code numShards * 2^x`} that is less than or equal to the maximum number of shards: 1024.
            int log2MaxNumShards = 10; // logBase2(1024)
            int log2NumShards = 32 - Integer.numberOfLeadingZeros(numShards - 1); // ceil(logBase2(numShards))
            int numSplits = log2MaxNumShards - log2NumShards;
            numSplits = Math.max(1, numSplits); // Ensure the index can be split at least once
            return numShards << numSplits;
        } else {
            return numShards;
        }
    }

    public ClusterState add(ClusterState currentState,
                            CreateTableRequest request,
                            Settings settings) throws IOException {
        RelationName tableName = request.getTableName();
        String indexName = tableName.indexNameOrAlias();

        validateIndexName(indexName, currentState);
        validateIndexSettings(indexName, request.settings(), forbidPrivateIndexSettings);
        shardLimitValidator.validateShardLimit(settings, currentState);

        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        final MappingMetadata mapping = new MappingMetadata(Map.of("default", MappingUtil.createMapping(
            MappingUtil.AllocPosition.forNewTable(),
            request.pkConstraintName(),
            DocReferences.applyOid(request.references(), metadataBuilder.columnOidSupplier()),
            request.pKeyIndices(),
            request.checkConstraints(),
            request.partitionedBy(),
            request.tableColumnPolicy(),
            request.routingColumn()
        )));

        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(settings)
            .put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, indexName)
            .put(SETTING_CREATION_DATE, Instant.now().toEpochMilli());

        if (indexSettingsBuilder.get(SETTING_NUMBER_OF_SHARDS) == null) {
            throw new IllegalArgumentException("Number of shards must be supplied");
        }

        final Settings idxSettings = indexSettingsBuilder.build();
        final int routingNumShards = IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(idxSettings)
            ? IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(idxSettings)
            : calculateNumRoutingShards(
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(idxSettings),
                idxSettings.getAsVersion(IndexMetadata.SETTING_VERSION_CREATED, null)
            );
        // remove the setting it's temporary and is only relevant once we create the index
        indexSettingsBuilder.remove(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey());

        // Set up everything, now locally create the index to see that things are ok, and apply
        final IndexMetadata tmpImd = IndexMetadata.builder(indexName)
            .settings(indexSettingsBuilder.build())
            .setRoutingNumShards(routingNumShards)
            .build();

        ActiveShardCount waitForActiveShards = tmpImd.getWaitForActiveShards();
        if (waitForActiveShards.validate(tmpImd.getNumberOfReplicas()) == false) {
            throw new IllegalArgumentException("invalid wait_for_active_shards[" + ActiveShardCount.DEFAULT +
                "]: cannot be greater than number of shard copies [" +
                (tmpImd.getNumberOfReplicas() + 1) + "]");
        }
        // create the index here (on the master) to validate it can be created, as well as adding the mapping
        return indicesService.withTempIndexService(tmpImd, indexService -> {
            IndexAnalyzers indexAnalyzers = indexService.indexAnalyzers();
            ensureUsedAnalyzersExist(indexAnalyzers, request.references());
            ClusterState updatedState = addIndex(
                allocationService,
                indexService,
                currentState,
                metadataBuilder,
                indexName,
                tmpImd,
                mapping,
                List.of(),
                routingNumShards
            );
            new DocTableInfoFactory(nodeContext).create(tableName, updatedState.metadata());
            return updatedState;
        });
    }

    private static ClusterState addIndex(AllocationService allocationService,
                                         IndexService indexService,
                                         ClusterState currentState,
                                         Metadata.Builder metadataBuilder,
                                         String indexName,
                                         IndexMetadata tmpImd,
                                         MappingMetadata mapping,
                                         Iterable<Alias> aliases,
                                         int routingNumShards) {
        final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(tmpImd.getSettings())
            .setRoutingNumShards(routingNumShards)
            .state(State.OPEN)
            .putMapping(mapping);

        for (int shardId = 0; shardId < tmpImd.getNumberOfShards(); shardId++) {
            indexMetadataBuilder.primaryTerm(shardId, tmpImd.primaryTerm(shardId));
        }
        for (Alias alias : aliases) {
            AliasMetadata aliasMetadata = new AliasMetadata(alias.name());
            indexMetadataBuilder.putAlias(aliasMetadata);
        }
        final IndexMetadata indexMetadata = indexMetadataBuilder.build();
        indexService.getIndexEventListener().beforeIndexAddedToCluster(
            indexMetadata.getIndex(),
            indexMetadata.getSettings()
        );
        LOGGER.info(
            "[{}] creating index, cause [create-table], shards [{}]/[{}]",
            indexName,
            indexMetadata.getNumberOfShards(),
            indexMetadata.getNumberOfReplicas());

        Metadata newMetadata = metadataBuilder.put(indexMetadata, false).build();
        ClusterState newState = ClusterState.builder(currentState)
            .blocks(
                ClusterBlocks.builder()
                    .blocks(currentState.blocks())
                    .updateBlocks(indexMetadata))
            .metadata(newMetadata)
            .routingTable(
                RoutingTable.builder(currentState.routingTable())
                    .addAsNew(newMetadata.index(indexName))
                    .build())
            .build();

        return allocationService.reroute(newState, "index [" + indexName + "] created");
    }

    private static void ensureUsedAnalyzersExist(IndexAnalyzers indexAnalyzers, List<Reference> references) {
        for (var ref : references) {
            if (ref instanceof IndexReference indexRef) {
                NamedAnalyzer namedAnalyzer = indexAnalyzers.get(indexRef.analyzer());
                if (namedAnalyzer == null) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "Analyzer \"%s\" not found for column \"%s\"",
                        indexRef.analyzer(),
                        indexRef.column()
                    ));
                }
            }
        }
    }
}
