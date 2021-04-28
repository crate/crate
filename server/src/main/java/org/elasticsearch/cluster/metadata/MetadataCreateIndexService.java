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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.cluster.block.ClusterBlock;
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
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndexCreationException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfoBuilder;

import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

/**
 * Service responsible for submitting create index requests
 */
public class MetadataCreateIndexService {

    private static final Logger LOGGER = LogManager.getLogger(MetadataCreateIndexService.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LogManager.getLogger(
        MetadataCreateIndexService.class));

    public static final int MAX_INDEX_NAME_BYTES = 255;

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final AliasValidator aliasValidator;
    private final Environment env;
    private final IndexScopedSettings indexScopedSettings;
    private final ActiveShardsObserver activeShardsObserver;
    private final NamedXContentRegistry xContentRegistry;
    private final boolean forbidPrivateIndexSettings;
    private final Settings settings;
    private final ShardLimitValidator shardLimitValidator;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public MetadataCreateIndexService(
            final Settings settings,
            final ClusterService clusterService,
            final IndicesService indicesService,
            final AllocationService allocationService,
            final AliasValidator aliasValidator,
            final ShardLimitValidator shardLimitValidator,
            final Environment env,
            final IndexScopedSettings indexScopedSettings,
            final ThreadPool threadPool,
            final NamedXContentRegistry xContentRegistry,
            final boolean forbidPrivateIndexSettings) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.aliasValidator = aliasValidator;
        this.env = env;
        this.indexScopedSettings = indexScopedSettings;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        this.xContentRegistry = xContentRegistry;
        this.forbidPrivateIndexSettings = forbidPrivateIndexSettings;
        this.shardLimitValidator = shardLimitValidator;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver();
    }

    /**
     * Validate the name for an index against some static rules and a cluster state.
     */
    public static void validateIndexName(String index, ClusterState state) {
        validateIndexOrAliasName(index, InvalidIndexNameException::new);
        if (!index.toLowerCase(Locale.ROOT).equals(index)) {
            throw new InvalidIndexNameException(index, "must be lowercase");
        }
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
     * Validate the name for an index or alias against some static rules.
     */
    public static void validateIndexOrAliasName(String index, BiFunction<String, String, ? extends RuntimeException> exceptionCtor) {
        if (!Strings.validFileName(index)) {
            throw exceptionCtor.apply(index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (index.contains("#")) {
            throw exceptionCtor.apply(index, "must not contain '#'");
        }
        if (index.contains(":")) {
            DEPRECATION_LOGGER.deprecated("index or alias name [" + index +
                            "] containing ':' is deprecated. CrateDB 4.x will read, " +
                            "but not allow creation of new indices containing ':'");
        }
        if (index.charAt(0) == '_' || index.charAt(0) == '-' || index.charAt(0) == '+') {
            throw exceptionCtor.apply(index, "must not start with '_', '-', or '+'");
        }
        int byteCount = 0;
        try {
            byteCount = index.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new ElasticsearchException("Unable to determine length of index name", e);
        }
        if (byteCount > MAX_INDEX_NAME_BYTES) {
            throw exceptionCtor.apply(index, "index name is too long, (" + byteCount + " > " + MAX_INDEX_NAME_BYTES + ")");
        }
        if (index.equals(".") || index.equals("..")) {
            throw exceptionCtor.apply(index, "must not be '.' or '..'");
        }
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
     * @param listener the listener on which to send the index creation cluster state update response
     */
    public void createIndex(final NodeContext nodeContext,
                            final CreateIndexClusterStateUpdateRequest request,
                            final ActionListener<CreateIndexClusterStateUpdateResponse> listener) {
        onlyCreateIndex(nodeContext, request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                activeShardsObserver.waitForActiveShards(new String[]{request.index()}, request.waitForActiveShards(), request.ackTimeout(),
                    shardsAcknowledged -> {
                        if (shardsAcknowledged == false) {
                            LOGGER.debug("[{}] index created, but the operation timed out while waiting for " +
                                             "enough shards to be started.", request.index());
                        }
                        listener.onResponse(new CreateIndexClusterStateUpdateResponse(response.isAcknowledged(), shardsAcknowledged));
                    }, listener::onFailure);
            } else {
                listener.onResponse(new CreateIndexClusterStateUpdateResponse(false, false));
            }
        }, listener::onFailure));
    }

    private void onlyCreateIndex(final NodeContext nodeContext,
                                 final CreateIndexClusterStateUpdateRequest request,
                                 final ActionListener<ClusterStateUpdateResponse> listener) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        Settings build = updatedSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        indexScopedSettings.validate(build, true); // we do validate here - index setting must be consistent
        request.settings(build);
        clusterService.submitStateUpdateTask(
            "create-index [" + request.index() + "], cause [" + request.cause() + "]",
            new IndexCreationTask(
                    LOGGER,
                    allocationService,
                    request,
                    listener,
                    indicesService,
                    aliasValidator,
                    xContentRegistry,
                    settings,
                    this::validate,
                    indexScopedSettings,
                    nodeContext,
                    indexNameExpressionResolver));
    }

    interface IndexValidator {
        void validate(CreateIndexClusterStateUpdateRequest request, ClusterState state);
    }

    static class IndexCreationTask extends AckedClusterStateUpdateTask<ClusterStateUpdateResponse> {

        private final IndicesService indicesService;
        private final AliasValidator aliasValidator;
        private final NamedXContentRegistry xContentRegistry;
        private final CreateIndexClusterStateUpdateRequest request;
        private final Logger logger;
        private final AllocationService allocationService;
        private final Settings settings;
        private final IndexValidator validator;
        private final IndexScopedSettings indexScopedSettings;
        private final NodeContext nodeContext;
        private final IndexNameExpressionResolver indexNameExpressionResolver;

        IndexCreationTask(Logger logger,
                          AllocationService allocationService,
                          CreateIndexClusterStateUpdateRequest request,
                          ActionListener<ClusterStateUpdateResponse> listener,
                          IndicesService indicesService,
                          AliasValidator aliasValidator,
                          NamedXContentRegistry xContentRegistry,
                          Settings settings,
                          IndexValidator validator,
                          IndexScopedSettings indexScopedSettings,
                          NodeContext nodeContext,
                          IndexNameExpressionResolver indexNameExpressionResolver) {
            super(Priority.URGENT, request, listener);
            this.request = request;
            this.logger = logger;
            this.allocationService = allocationService;
            this.indicesService = indicesService;
            this.aliasValidator = aliasValidator;
            this.xContentRegistry = xContentRegistry;
            this.settings = settings;
            this.validator = validator;
            this.indexScopedSettings = indexScopedSettings;
            this.nodeContext = nodeContext;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        @Override
        protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
            return new ClusterStateUpdateResponse(acknowledged);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            Index createdIndex = null;
            String removalExtraInfo = null;
            IndexRemovalReason removalReason = IndexRemovalReason.FAILURE;
            try {
                validator.validate(request, currentState);

                for (Alias alias : request.aliases()) {
                    aliasValidator.validateAlias(alias, request.index(), currentState.metadata());
                }

                // we only find a template when its an API call (a new index)
                // find templates, highest order are better matching
                List<IndexTemplateMetadata> templates =
                        MetadataIndexTemplateService.findTemplates(currentState.metadata(), request.index());

                // add the request mapping
                Map<String, Map<String, Object>> mappings = new HashMap<>();

                Map<String, AliasMetadata> templatesAliases = new HashMap<>();

                List<String> templateNames = new ArrayList<>();

                for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
                    mappings.put(entry.getKey(), MapperService.parseMapping(xContentRegistry, entry.getValue()));
                }

                final Index recoverFromIndex = request.recoverFrom();

                if (recoverFromIndex == null) {
                    // apply templates, merging the mappings into the request mapping if exists
                    for (IndexTemplateMetadata template : templates) {
                        templateNames.add(template.getName());
                        for (ObjectObjectCursor<String, CompressedXContent> cursor : template.mappings()) {
                            String mappingString = cursor.value.string();
                            if (mappings.containsKey(cursor.key)) {
                                XContentHelper.mergeDefaults(mappings.get(cursor.key),
                                    MapperService.parseMapping(xContentRegistry, mappingString));
                            } else {
                                mappings.put(cursor.key,
                                    MapperService.parseMapping(xContentRegistry, mappingString));
                            }
                        }
                        //handle aliases
                        for (ObjectObjectCursor<String, AliasMetadata> cursor : template.aliases()) {
                            AliasMetadata aliasMetadata = cursor.value;
                            //if an alias with same name came with the create index request itself,
                            // ignore this one taken from the index template
                            if (request.aliases().contains(new Alias(aliasMetadata.alias()))) {
                                continue;
                            }
                            //if an alias with same name was already processed, ignore this one
                            if (templatesAliases.containsKey(cursor.key)) {
                                continue;
                            }

                            //Allow templatesAliases to be templated by replacing a token with the name of the index that we are applying it to
                            if (aliasMetadata.alias().contains("{index}")) {
                                String templatedAlias = aliasMetadata.alias().replace("{index}", request.index());
                                aliasMetadata = AliasMetadata.newAliasMetadata(aliasMetadata, templatedAlias);
                            }

                            aliasValidator.validateAliasMetadata(aliasMetadata, request.index(), currentState.metadata());
                            templatesAliases.put(aliasMetadata.alias(), aliasMetadata);
                        }
                    }
                }
                Settings.Builder indexSettingsBuilder = Settings.builder();
                if (recoverFromIndex == null) {
                    // apply templates, here, in reverse order, since first ones are better matching
                    for (int i = templates.size() - 1; i >= 0; i--) {
                        indexSettingsBuilder.put(templates.get(i).settings());
                    }
                }
                // now, put the request settings, so they override templates
                indexSettingsBuilder.put(request.settings());
                if (indexSettingsBuilder.get(SETTING_NUMBER_OF_SHARDS) == null) {
                    throw new IllegalArgumentException("Number of shards must be supplied");
                }
                if (indexSettingsBuilder.get(SETTING_NUMBER_OF_REPLICAS) == null) {
                    indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                }
                if (settings.get(SETTING_AUTO_EXPAND_REPLICAS) != null && indexSettingsBuilder.get(SETTING_AUTO_EXPAND_REPLICAS) == null) {
                    indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, settings.get(SETTING_AUTO_EXPAND_REPLICAS));
                }

                if (indexSettingsBuilder.get(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey()) == null) {
                    final DiscoveryNodes nodes = currentState.nodes();
                    final Version createdVersion = Version.min(Version.CURRENT, nodes.getSmallestNonClientNodeVersion());
                    indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), createdVersion);
                }

                if (indexSettingsBuilder.get(SETTING_CREATION_DATE) == null) {
                    indexSettingsBuilder.put(SETTING_CREATION_DATE, Instant.now().toEpochMilli());
                }
                indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, request.getProvidedName());
                indexSettingsBuilder.put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
                final IndexMetadata.Builder tmpImdBuilder = IndexMetadata.builder(request.index());

                final int routingNumShards;
                if (recoverFromIndex == null) {
                    Settings idxSettings = indexSettingsBuilder.build();
                    routingNumShards = IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(idxSettings);
                } else {
                    assert IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(indexSettingsBuilder.build()) == false
                        : "index.number_of_routing_shards should be present on the target index on resize";
                    final IndexMetadata sourceMetadata = currentState.metadata().getIndexSafe(recoverFromIndex);
                    routingNumShards = sourceMetadata.getRoutingNumShards();
                }
                // remove the setting it's temporary and is only relevant once we create the index
                indexSettingsBuilder.remove(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey());
                tmpImdBuilder.setRoutingNumShards(routingNumShards);

                if (recoverFromIndex != null) {
                    assert request.resizeType() != null;
                    prepareResizeIndexSettings(
                            currentState,
                            mappings.keySet(),
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
                    final IndexMetadata sourceMetadata = currentState.metadata().getIndexSafe(recoverFromIndex);
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
                final IndexService indexService = indicesService.createIndex(tmpImd, Collections.emptyList(), false);
                createdIndex = indexService.index();
                // now add the mappings

                MapperService mapperService = indexService.mapperService();
                if (mappings.isEmpty() == false) {
                    assert mappings.size() == 1;
                    var entry = mappings.entrySet().iterator().next();
                    try {
                        mapperService.merge(entry.getKey(), entry.getValue(), MergeReason.MAPPING_UPDATE);
                    } catch (Exception e) {
                        removalExtraInfo = "failed on parsing default mapping/mappings on index creation";
                        throw e;
                    }
                }

                // now, update the mappings with the actual source
                Map<String, MappingMetadata> mappingsMetadata = new HashMap<>();
                DocumentMapper mapper = mapperService.documentMapper();
                if (mapper != null) {
                    MappingMetadata mappingMd = new MappingMetadata(mapper);
                    mappingsMetadata.put(mapper.type(), mappingMd);
                }

                final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(request.index())
                    .settings(actualIndexSettings)
                    .setRoutingNumShards(routingNumShards);

                for (int shardId = 0; shardId < tmpImd.getNumberOfShards(); shardId++) {
                    indexMetadataBuilder.primaryTerm(shardId, tmpImd.primaryTerm(shardId));
                }

                for (MappingMetadata mappingMd : mappingsMetadata.values()) {
                    indexMetadataBuilder.putMapping(mappingMd);
                }

                for (AliasMetadata aliasMetadata : templatesAliases.values()) {
                    indexMetadataBuilder.putAlias(aliasMetadata);
                }
                for (Alias alias : request.aliases()) {
                    AliasMetadata aliasMetadata = AliasMetadata.builder(alias.name()).filter(alias.filter())
                        .indexRouting(alias.indexRouting()).searchRouting(alias.searchRouting()).writeIndex(alias.writeIndex()).build();
                    indexMetadataBuilder.putAlias(aliasMetadata);
                }

                indexMetadataBuilder.state(request.state());

                final IndexMetadata indexMetadata;
                try {
                    indexMetadata = indexMetadataBuilder.build();
                } catch (Exception e) {
                    removalExtraInfo = "failed to build index metadata";
                    throw e;
                }

                indexService.getIndexEventListener().beforeIndexAddedToCluster(indexMetadata.getIndex(),
                    indexMetadata.getSettings());

                final Metadata newMetadata = Metadata.builder(currentState.metadata())
                    .put(indexMetadata, false)
                    .build();

                logger.info("[{}] creating index, cause [{}], templates {}, shards [{}]/[{}], mappings {}",
                    request.index(), request.cause(), templateNames, indexMetadata.getNumberOfShards(),
                    indexMetadata.getNumberOfReplicas(), mappings.keySet());

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                if (!request.blocks().isEmpty()) {
                    for (ClusterBlock block : request.blocks()) {
                        blocks.addIndexBlock(request.index(), block);
                    }
                }
                blocks.updateBlocks(indexMetadata);

                ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metadata(newMetadata).build();

                if (request.state() == State.OPEN) {
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
                        .addAsNew(updatedState.metadata().index(request.index()));
                    updatedState = allocationService.reroute(
                        ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(),
                        "index [" + request.index() + "] created");
                }
                removalExtraInfo = "cleaning up after validating index on master";
                removalReason = IndexRemovalReason.NO_LONGER_ASSIGNED;

                ensureTableInfoCanBeCreated(request.index(), updatedState);

                return updatedState;
            } finally {
                if (createdIndex != null) {
                    // Index was already partially created - need to clean up
                    indicesService.removeIndex(createdIndex, removalReason, removalExtraInfo);
                }
            }
        }

        private void ensureTableInfoCanBeCreated(String index, ClusterState updatedState) {
            if (IndexParts.isDangling(index)) {
                // temporary index for resize or shrink operation is always allowed
                return;
            }
            var relationName = RelationName.fromIndexName(request.index());
            var builder = new DocTableInfoBuilder(nodeContext, relationName, updatedState, indexNameExpressionResolver);
            builder.build();
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
        validateIndexName(request.index(), state);
        validateIndexSettings(request.index(), request.settings(), state, forbidPrivateIndexSettings);
        shardLimitValidator.validateShardLimit(request.settings(), state);
    }

    public void validateIndexSettings(String indexName, final Settings settings, final ClusterState clusterState,
                                      final boolean forbidPrivateIndexSettings) throws IndexCreationException {
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
        if (Strings.isEmpty(customPath) == false && env.sharedDataFile() == null) {
            validationErrors.add("path.shared_data must be set in order to use custom data paths");
        } else if (Strings.isEmpty(customPath) == false) {
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
                                            Set<String> targetIndexMappingsTypes,
                                            String targetIndexName,
                                            Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexMappingsTypes, targetIndexName, targetIndexSettings);
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

    static void validateSplitIndex(ClusterState state, String sourceIndex,
                                   Set<String> targetIndexMappingsTypes, String targetIndexName,
                                   Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexMappingsTypes, targetIndexName, targetIndexSettings);
        IndexMetadata.selectSplitShard(0, sourceMetadata, IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static IndexMetadata validateResize(ClusterState state, String sourceIndex,
                                        Set<String> targetIndexMappingsTypes, String targetIndexName,
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

        if (targetIndexMappingsTypes.size() > 1) {
            throw new IllegalArgumentException(
                "mappings are not allowed when resizing indices" +
                ", all mappings are copied from the source index");
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
            final Set<String> mappingKeys,
            final Settings.Builder indexSettingsBuilder,
            final Index resizeSourceIndex,
            final String resizeIntoName,
            final ResizeType type,
            final boolean copySettings,
            final IndexScopedSettings indexScopedSettings) {
        final IndexMetadata sourceMetadata = currentState.metadata().index(resizeSourceIndex.getName());
        if (type == ResizeType.SHRINK) {
            final List<String> nodesToAllocateOn = validateShrinkIndex(currentState, resizeSourceIndex.getName(),
                mappingKeys, resizeIntoName, indexSettingsBuilder.build());
            indexSettingsBuilder
                // we use "i.r.a.initial_recovery" rather than "i.r.a.require|include" since we want the replica to allocate right away
                // once we are allocated.
                .put(IndexMetadata.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getKey() + "_id",
                     Strings.arrayToCommaDelimitedString(nodesToAllocateOn.toArray()))
                // we add the legacy way of specifying it here for BWC. We can remove this once it's backported to 6.x
                .put(IndexMetadata.INDEX_SHRINK_SOURCE_NAME.getKey(), resizeSourceIndex.getName())
                .put(IndexMetadata.INDEX_SHRINK_SOURCE_UUID.getKey(), resizeSourceIndex.getUUID());
        } else if (type == ResizeType.SPLIT) {
            validateSplitIndex(currentState, resizeSourceIndex.getName(), mappingKeys, resizeIntoName, indexSettingsBuilder.build());
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
}
