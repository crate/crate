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
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_OLD_NAME;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.Metadata.Builder;
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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexCreationException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;

import io.crate.common.collections.Lists;
import io.crate.common.unit.TimeValue;
import io.crate.execution.ddl.tables.AlterTableClient;
import io.crate.execution.ddl.tables.CreateBlobTableRequest;
import io.crate.execution.ddl.tables.CreateTableResponse;
import io.crate.execution.ddl.tables.MappingUtil;
import io.crate.metadata.DocReferences;
import io.crate.metadata.IndexReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.table.SchemaInfo;

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
    private final ShardLimitValidator shardLimitValidator;

    public MetadataCreateIndexService(NodeContext nodeContext,
                                      ClusterService clusterService,
                                      IndicesService indicesService,
                                      AllocationService allocationService,
                                      ShardLimitValidator shardLimitValidator,
                                      Environment env,
                                      IndexScopedSettings indexScopedSettings) {
        this.nodeContext = nodeContext;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.env = env;
        this.indexScopedSettings = indexScopedSettings;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService);
        this.shardLimitValidator = shardLimitValidator;
    }

    /**
     * @param createResponse params: (clusterStateAcknowledged, shardsAcknowledged)
     **/
    public <T> ActionListener<ClusterStateUpdateResponse> withWaitForShards(ActionListener<T> listener,
                                                                            RelationName relationName,
                                                                            ActiveShardCount waitForActiveShards,
                                                                            TimeValue ackTimeout,
                                                                            BiFunction<Boolean, Boolean, T> createResponse) {
        return ActionListener.wrap(
            resp -> {
                if (resp.isAcknowledged()) {
                    List<String> indexUUIDs = clusterService.state().metadata().getIndices(relationName, List.of(), false, IndexMetadata::getIndexUUID);
                    activeShardsObserver.waitForActiveShards(
                        indexUUIDs.toArray(new String[0]),
                        waitForActiveShards,
                        ackTimeout,
                        shardsAcknowledged -> {
                            if (shardsAcknowledged == false) {
                                LOGGER.debug(
                                    "[{}] index created, but the operation timed out waiting for enough shards to be started.",
                                    indexUUIDs
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

    public CompletableFuture<ResizeResponse> resizeIndex(ResizeRequest request, IndicesStatsResponse indicesStats) {
        String sourceIndexUUID = clusterService.state().metadata().getIndex(request.table(), request.partitionValues(), true, IndexMetadata::getIndexUUID);
        IndexStats indexStats = indicesStats.getIndex(sourceIndexUUID);
        Map<Integer, IndexShardStats> indexShards = indexStats.getIndexShards();
        ResizeIndexTask resizeIndexTask = new ResizeIndexTask(
            allocationService,
            request,
            sourceIndexUUID,
            indicesService,
            shardId -> {
                IndexShardStats indexShardStats = indexShards.get(shardId.id());
                if (indexShardStats == null) {
                    return 0;
                }
                DocsStats docs = indexShardStats.getPrimary().getDocs();
                return docs == null ? 0 : docs.getCount();
            },
            shardLimitValidator,
            indexScopedSettings
        );
        String source = "resize[" + request.table() + "-" + request.partitionValues() + "]";
        clusterService.submitStateUpdateTask(source, resizeIndexTask);
        return resizeIndexTask.completionFuture().thenCompose(resp -> {
            if (resp.isAcknowledged()) {
                String[] indexNames = new String[] { resizeIndexTask.resizedIndex() };
                return activeShardsObserver.waitForActiveShards(
                    indexNames,
                    ActiveShardCount.DEFAULT,
                    request.ackTimeout()
                ).thenApply(shardsAcked -> new ResizeResponse(resp.isAcknowledged(), shardsAcked));
            } else {
                ResizeResponse resizeResponse = new ResizeResponse(false, false);
                return CompletableFuture.completedFuture(resizeResponse);
            }
        });
    }

    public void addBlobTable(CreateBlobTableRequest request, ActionListener<CreateTableResponse> listener) {
        RelationName relationName = request.name();
        ActionListener<ClusterStateUpdateResponse> stateUpdateListener = withWaitForShards(
            listener,
            relationName,
            ActiveShardCount.DEFAULT,
            request.ackTimeout(),
            (stateAcked, shardsAcked) -> new CreateTableResponse(stateAcked && shardsAcked)
        );
        clusterService.submitStateUpdateTask(
            "create-blob-table",
            new CreateBlobTableTask(
                request,
                stateUpdateListener,
                indicesService,
                allocationService,
                nodeContext
            )
        );
    }

    static class CreateBlobTableTask extends AckedClusterStateUpdateTask<ClusterStateUpdateResponse> {

        private final IndicesService indicesService;
        private final CreateBlobTableRequest request;
        private final AllocationService allocationService;
        private final NodeContext nodeContext;

        public CreateBlobTableTask(CreateBlobTableRequest request,
                                   ActionListener<ClusterStateUpdateResponse> listener,
                                   IndicesService indicesService,
                                   AllocationService allocationService,
                                   NodeContext nodeContext) {
            super(Priority.HIGH, request, listener);
            this.request = request;
            this.indicesService = indicesService;
            this.allocationService = allocationService;
            this.nodeContext = nodeContext;
        }

        @Override
        protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
            return new ClusterStateUpdateResponse(acknowledged);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            RelationName relationName = request.name();
            String indexName = relationName.indexNameOrAlias();
            Version versionCreated = currentState.nodes().getSmallestNonClientNodeVersion();
            String indexUUID = UUIDs.randomBase64UUID();
            Settings settings = Settings.builder()
                .put(request.settings())
                .put(SETTING_INDEX_UUID, indexUUID)
                .put(SETTING_CREATION_DATE, Instant.now().toEpochMilli())
                .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), versionCreated)
                .build();
            int numShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);
            IndexMetadata indexMetadata = IndexMetadata.builder(indexUUID)
                .indexName(indexName)
                .settings(settings)
                .build();

            return indicesService.withTempIndexService(indexMetadata, indexService -> {
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata())
                    .setBlobTable(relationName, indexUUID, settings, State.OPEN);
                ClusterState updatedState = addIndex(
                    allocationService,
                    indexService,
                    currentState,
                    mdBuilder,
                    indexUUID,
                    indexMetadata,
                    new MappingMetadata(Map.of()),
                    List.of(),
                    calculateNumRoutingShards(numShards, versionCreated)
                );
                // ensure table can be parsed
                SchemaInfo blobSchemaInfo = nodeContext.schemas().getSchemaInfo(BlobSchemaInfo.NAME);
                assert blobSchemaInfo != null : "BlobSchemaInfo should be available";
                blobSchemaInfo.create(request.name(), updatedState.metadata());
                return updatedState;
            });
        }
    }

    static class ResizeIndexTask extends AckedClusterStateUpdateTask<ClusterStateUpdateResponse> {

        private final IndicesService indicesService;
        private final ResizeRequest request;
        private final AllocationService allocationService;
        private final IndexScopedSettings indexScopedSettings;

        private final String sourceIndexUUID;
        private final String resizedIndexUUID;
        private final ShardLimitValidator validator;
        private final ToLongFunction<ShardId> getNumDocs;

        ResizeIndexTask(AllocationService allocationService,
                        ResizeRequest request,
                        String sourceIndexUUID,
                        IndicesService indicesService,
                        ToLongFunction<ShardId> getNumDocs,
                        ShardLimitValidator validator,
                        IndexScopedSettings indexScopedSettings) {
            super(Priority.URGENT, request);
            this.request = request;
            this.allocationService = allocationService;
            this.indicesService = indicesService;
            this.getNumDocs = getNumDocs;
            this.validator = validator;
            this.indexScopedSettings = indexScopedSettings;

            this.sourceIndexUUID = sourceIndexUUID;
            this.resizedIndexUUID = UUIDs.randomBase64UUID();
        }

        public String resizedIndex() {
            return resizedIndexUUID;
        }

        @Override
        protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
            return new ClusterStateUpdateResponse(acknowledged);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            Metadata metadata = currentState.metadata();
            IndexMetadata sourceIndex = metadata.index(sourceIndexUUID);
            if (sourceIndex == null) {
                throw new UnsupportedOperationException("Cannot resize missing index: " + sourceIndexUUID);
            }
            if (metadata.hasIndex(resizedIndexUUID)) {
                throw new ResourceAlreadyExistsException(resizedIndexUUID);
            }
            if (!currentState.blocks().indexBlocked(ClusterBlockLevel.WRITE, sourceIndexUUID)) {
                throw new IllegalStateException("index " + sourceIndex + " must be read-only to resize index. use \"index.blocks.write=true\"");
            }

            String resizedIndexName = AlterTableClient.RESIZE_PREFIX + sourceIndex.getIndex().getName();

            final int routingNumShards;
            Settings sourceSettings = sourceIndex.getSettings();
            Version indexVersionCreated = currentState.nodes().getSmallestNonClientNodeVersion();
            int newNumShards = request.newNumShards();
            if (sourceIndex.getNumberOfShards() == 1) {
                routingNumShards = IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(sourceSettings)
                    ? IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(sourceSettings)
                    : calculateNumRoutingShards(newNumShards, indexVersionCreated);
            } else {
                routingNumShards = sourceIndex.getRoutingNumShards();
            }

            Settings.Builder indexSettingsBuilder = Settings.builder();
            for (final String key : sourceSettings.keySet()) {
                final Setting<?> setting = indexScopedSettings.get(key);
                if (setting == null) {
                    assert indexScopedSettings.isPrivateSetting(key) : key;
                } else if (setting.getProperties().contains(Setting.Property.NotCopyableOnResize)) {
                    continue;
                }
                indexSettingsBuilder.copy(key, sourceSettings);
            }
            indexSettingsBuilder
                .put(SETTING_INDEX_UUID, resizedIndexUUID)
                .put(SETTING_OLD_NAME, sourceIndex.getIndex().getName())
                .put(SETTING_CREATION_DATE, Instant.now().toEpochMilli())
                .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersionCreated)
                .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), sourceIndex.getIndex().getName())
                .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.getKey(), sourceIndex.getIndexUUID())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, newNumShards);

            // this method applies all necessary checks ie. if the target shards are less than the source shards
            // of if the source shards are divisible by the number of target shards
            IndexMetadata.getRoutingFactor(sourceIndex.getNumberOfShards(), newNumShards);
            boolean shrink = sourceIndex.getNumberOfShards() > newNumShards;
            if (shrink) {
                List<String> nodesToAllocateOn = getShrinkAllocationNodes(
                    currentState,
                    sourceIndexUUID,
                    sourceIndex
                );
                indexSettingsBuilder.put(
                    IndexMetadata.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getKey() + "_id",
                    Lists.joinOn(",", nodesToAllocateOn, x -> x)
                );
            }
            for (int i = 0; i < newNumShards; i++) {
                if (shrink) {
                    Set<ShardId> shardIds = IndexMetadata.selectShrinkShards(i, sourceIndex, newNumShards);
                    long count = 0;
                    for (ShardId id : shardIds) {
                        count += getNumDocs.applyAsLong(id);
                        if (count > IndexWriter.MAX_DOCS) {
                            throw new IllegalStateException("Can't merge index with more than [" + IndexWriter.MAX_DOCS
                                + "] docs - too many documents in shards " + shardIds);
                        }
                    }
                } else {
                    // we just execute this to ensure we get the right exceptions if the number of shards is wrong or less then etc.
                    Objects.requireNonNull(IndexMetadata.selectSplitShard(i, sourceIndex, newNumShards));
                }
            }

            IndexMetadata.Builder tmpImdBuilder = IndexMetadata.builder(resizedIndexUUID)
                .indexName(sourceIndex.getIndex().getName())
                .settings(indexSettingsBuilder)
                .indexName(resizedIndexName)
                .partitionValues(sourceIndex.partitionValues())
                .setRoutingNumShards(routingNumShards);

            assert tmpImdBuilder.numberOfShards() == newNumShards : "number of shards must be set";

            /*
             * We need to arrange that the primary term on all the shards in the shrunken index is at least as large as
             * the maximum primary term on all the shards in the source index. This ensures that we have correct
             * document-level semantics regarding sequence numbers in the shrunken index.
             */
            final long primaryTerm =
                IntStream
                    .range(0, sourceIndex.getNumberOfShards())
                    .mapToLong(sourceIndex::primaryTerm)
                    .max()
                    .getAsLong();
            for (int shardId = 0; shardId < tmpImdBuilder.numberOfShards(); shardId++) {
                tmpImdBuilder.primaryTerm(shardId, primaryTerm);
            }

            Builder metadataBuilder = Metadata.builder(metadata);
            RelationMetadata.Table table = metadata.getRelation(request.table());
            if (table == null) {
                throw new IllegalArgumentException("Cannot resize index for missing table: " + request.table());
            }

            Settings settings = table.settings();
            if (request.partitionValues().isEmpty()) {
                settings = Settings.builder()
                    .put(table.settings())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, newNumShards)
                    .build();
            }
            List<String> indexUUIDs = new ArrayList<>(table.indexUUIDs());
            indexUUIDs.add(resizedIndexUUID);

            metadataBuilder.setTable(
                table.name(),
                table.columns(),
                settings,
                table.routingColumn(),
                table.columnPolicy(),
                table.pkConstraintName(),
                table.checkConstraints(),
                table.primaryKeys(),
                table.partitionedBy(),
                table.state(),
                indexUUIDs,
                table.tableVersion() + 1
            );
            IndexMetadata tmpImd = tmpImdBuilder.build();

            validator.validateShardLimit(tmpImd.getSettings(), currentState);
            return indicesService.withTempIndexService(tmpImd, indexService -> addIndex(
                allocationService,
                indexService,
                currentState,
                metadataBuilder,
                resizedIndexUUID,
                indexService.getMetadata(),
                sourceIndex.mapping(),
                request.partitionValues().isEmpty() ? List.of() : List.of(new Alias(request.table().indexNameOrAlias())),
                routingNumShards
            ));
        }
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
                    assert indexScopedSettings.isPrivateSetting(key) : key + " must be a private setting if it is missing";
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
    static List<String> getShrinkAllocationNodes(ClusterState state,
                                                 String sourceIndexUUID,
                                                 IndexMetadata sourceIndex) {

        // now check that index is all on one node
        final IndexRoutingTable table = state.routingTable().index(sourceIndexUUID);
        Map<String, AtomicInteger> nodesToNumRouting = new HashMap<>();
        int numShards = sourceIndex.getNumberOfShards();
        for (ShardRouting routing : table.shardsWithState(ShardRoutingState.STARTED)) {
            AtomicInteger counter = nodesToNumRouting.computeIfAbsent(routing.currentNodeId(), _ -> new AtomicInteger(0));
            counter.incrementAndGet();
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
                            RelationMetadata.Table table,
                            String newIndexUUID,
                            List<String> partitionValues,
                            Settings concreteIndexSettings) throws IOException {
        RelationName tableName = table.name();
        PartitionName partitionName = new PartitionName(tableName, partitionValues);
        String indexName;
        if (partitionValues.isEmpty()) {
            indexName = tableName.indexNameOrAlias();
        } else {
            indexName = partitionName.asIndexName();
        }

        List<IndexMetadata> existingIndices = currentState.metadata().getIndices(tableName, partitionValues, true, im -> im);
        if (!existingIndices.isEmpty()) {
            throw new ResourceAlreadyExistsException(existingIndices.getFirst().getIndex().getName());
        }

        validateIndexSettings(indexName, concreteIndexSettings, true);

        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        final MappingMetadata mapping = new MappingMetadata(Map.of("default", MappingUtil.createMapping(
            MappingUtil.AllocPosition.forNewTable(),
            table.pkConstraintName(),
            DocReferences.applyOid(table.columns(), metadataBuilder.columnOidSupplier()),
            table.primaryKeys(),
            table.checkConstraints(),
            table.partitionedBy(),
            table.columnPolicy(),
            table.routingColumn()
        )));

        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(table.settings())
            .put(concreteIndexSettings)
            .put(SETTING_INDEX_UUID, newIndexUUID)
            .put(SETTING_CREATION_DATE, Instant.now().toEpochMilli());

        final Settings idxSettings = indexSettingsBuilder.build();
        shardLimitValidator.validateShardLimit(idxSettings, currentState);

        final int routingNumShards = IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(idxSettings)
            ? IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(idxSettings)
            : calculateNumRoutingShards(
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(idxSettings),
                idxSettings.getAsVersion(IndexMetadata.SETTING_VERSION_CREATED, null)
            );
        // remove the setting it's temporary and is only relevant once we create the index
        indexSettingsBuilder.remove(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey());

        // Set up everything, now locally create the index to see that things are ok, and apply
        final IndexMetadata tmpImd = IndexMetadata.builder(newIndexUUID)
            .settings(indexSettingsBuilder.build())
            .setRoutingNumShards(routingNumShards)
            .indexName(indexName)
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
            ensureUsedAnalyzersExist(indexAnalyzers, table.columns());
            ClusterState updatedState = addIndex(
                allocationService,
                indexService,
                currentState,
                metadataBuilder,
                newIndexUUID,
                tmpImd,
                mapping,
                List.of(),
                routingNumShards
            );
            SchemaInfo docSchemaInfo = nodeContext.schemas().getOrCreateSchemaInfo(tableName.schema());
            docSchemaInfo.create(tableName, updatedState.metadata());
            return updatedState;
        });
    }

    private static ClusterState addIndex(AllocationService allocationService,
                                         IndexService indexService,
                                         ClusterState currentState,
                                         Metadata.Builder metadataBuilder,
                                         String indexUUID,
                                         IndexMetadata tmpImd,
                                         MappingMetadata mapping,
                                         Iterable<Alias> aliases,
                                         int routingNumShards) {
        final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexUUID)
            .indexName(tmpImd.getIndex().getName())
            .settings(tmpImd.getSettings())
            .setRoutingNumShards(routingNumShards)
            .partitionValues(tmpImd.partitionValues())
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
            "[{}/{}] creating index, cause [create-table], shards [{}]/[{}]",
            indexMetadata.getIndex().getName(),
            indexUUID,
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
                    .addAsNew(newMetadata.index(indexUUID))
                    .build())
            .build();

        return allocationService.reroute(newState, "index [" + indexUUID + "] created");
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
