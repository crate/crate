/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.dml.upsert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.Term;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.IndexResult;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.engine.Engine.Result.Type;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.common.collections.Lists;
import io.crate.common.exceptions.Exceptions;
import io.crate.execution.ddl.tables.AddColumnRequest;
import io.crate.execution.ddl.tables.TransportAddColumn;
import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.Indexer;
import io.crate.execution.dml.RawIndexer;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.TransportShardAction;
import io.crate.execution.dml.UpsertReplicaRequest;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.engine.collect.PKLookupOperation;
import io.crate.execution.jobs.TasksService;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;

/**
 * Realizes Upserts of tables which either results in an Insert or an Update.
 */
@Singleton
public class TransportShardUpsertAction extends TransportShardAction<
        ShardUpsertRequest,
        UpsertReplicaRequest,
        ShardUpsertRequest.Item,
        UpsertReplicaRequest.Item> {

    private static final int MAX_RETRY_LIMIT = 100_000; // upper bound to prevent unlimited retries on unexpected states
    private final Schemas schemas;
    private final NodeContext nodeCtx;
    private final TransportAddColumn addColumnAction;

    @Inject
    public TransportShardUpsertAction(Settings settings,
                                      ThreadPool threadPool,
                                      ClusterService clusterService,
                                      TransportService transportService,
                                      TransportAddColumn addColumnAction,
                                      TasksService tasksService,
                                      IndicesService indicesService,
                                      ShardStateAction shardStateAction,
                                      CircuitBreakerService circuitBreakerService,
                                      NodeContext nodeCtx) {
        super(
            settings,
            ShardUpsertAction.NAME,
            transportService,
            clusterService,
            indicesService,
            tasksService,
            threadPool,
            shardStateAction,
            circuitBreakerService,
            ShardUpsertRequest::new,
            UpsertReplicaRequest::readFrom
        );
        this.nodeCtx = nodeCtx;
        this.schemas = nodeCtx.schemas();
        this.addColumnAction = addColumnAction;
        tasksService.addListener(this);
    }


    @Override
    protected WritePrimaryResult<UpsertReplicaRequest, ShardResponse> processRequestItems(IndexShard indexShard,
                                                                                          ShardUpsertRequest request,
                                                                                          AtomicBoolean killed) {
        String indexUUID = indexShard.shardId().getIndexUUID();
        Metadata metadata = clusterService.state().metadata();
        RelationMetadata relationMetadata = metadata.getRelation(indexUUID);
        if (relationMetadata == null) {
            throw new IndexNotFoundException(indexShard.shardId().getIndex());
        }
        DocTableInfo tableInfo = schemas.getTableInfo(relationMetadata.name());
        IndexMetadata indexMetadata = metadata.index(indexUUID);
        assert indexMetadata != null : "IndexMetadata for index " + indexUUID + " not found in cluster state";
        List<String> partitionValues = indexMetadata.partitionValues();
        TransactionContext txnCtx = TransactionContext.of(request.sessionSettings());

        // Refresh insertColumns References from table, they could be stale (dynamic references already added)
        List<Reference> insertColumns = new ArrayList<>();
        if (request.insertColumns() != null) {
            for (var ref : request.insertColumns()) {
                Reference updatedRef = tableInfo.getReference(ref.column());
                insertColumns.add(updatedRef == null ? ref : updatedRef);
            }
        }

        Indexer indexer = new Indexer(
            partitionValues,
            tableInfo,
            indexShard.getVersionCreated(),
            txnCtx,
            nodeCtx,
            insertColumns,
            request.updateColumns(),
            request.returnValues()
        );
        ColumnIdent firstColumnIdent = indexer.columns().getFirst().column();

        RawIndexer rawIndexer = null;
        if (firstColumnIdent.equals(SysColumns.RAW)) {
            rawIndexer = new RawIndexer(
                partitionValues,
                tableInfo,
                indexShard.getVersionCreated(),
                txnCtx,
                nodeCtx,
                request.returnValues(),
                List.of() // Non deterministic synthetics is not needed on primary
            );
        }

        ShardResponse shardResponse = new ShardResponse(request.returnValues());
        Translog.Location translogLocation = null;
        List<UpsertReplicaRequest.Item> replicaItems = new ArrayList<>();
        UpsertReplicaRequest replicaRequest = new UpsertReplicaRequest(
            request.shardId(),
            request.jobId(),
            request.sessionSettings(),
            // Copy because indexer.insertColumns can be mutated during indexing
            // to refine types. (undefined[] -> long[], with values being integer[])
            // Using the refined types can break streaming for the replica
            // See `test_dynamic_null_array_overridden_to_integer_becomes_null`
            List.copyOf(indexer.insertColumns()),
            replicaItems
        );
        for (ShardUpsertRequest.Item item : request.items()) {
            if (shardResponse.failure() != null) {
                // Skip all remaining items on replica
                continue;
            }
            int location = item.location();
            if (killed.get()) {
                // set failure on response and skip all next items (on primary and on replica)
                // this way replica operation will be executed, but only items with a valid source (= was processed on primary)
                // will be processed on the replica
                shardResponse.failure(new InterruptedException());
                continue;
            }
            try {
                IndexItemResult indexItemResult = indexItem(
                    indexer,
                    request,
                    item,
                    indexShard,
                    tableInfo,
                    partitionValues,
                    rawIndexer
                );
                if (indexItemResult != null) {
                    IndexResult result = indexItemResult.result;
                    if (result.getTranslogLocation() != null) {
                        shardResponse.add(location);
                        translogLocation = result.getTranslogLocation();
                    }
                    if (indexItemResult.returnValues != null) {
                        shardResponse.addResultRows(indexItemResult.returnValues);
                    }
                    UpsertReplicaRequest.Item replicaItem = new UpsertReplicaRequest.Item(
                        item.id(),
                        indexItemResult.replicaInsertValues(),
                        item.pkValues(),
                        result.getSeqNo(),
                        result.getTerm(),
                        result.getVersion()
                    );
                    replicaItems.add(replicaItem);
                }
            } catch (Exception e) {
                if (retryPrimaryException(e)) {
                    throw Exceptions.toRuntimeException(e);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Failed to execute upsert on nodeName={}, shardId={} id={} error={}",
                        clusterService.localNode().getName(),
                        request.shardId(),
                        item.id(),
                        e
                    );
                }
                if (!request.continueOnError()) {
                    shardResponse.failure(e);
                    break;
                }
                shardResponse.add(
                    location,
                    item.id(),
                    e,
                    (e instanceof VersionConflictEngineException)
                );
            } catch (AssertionError e) {
                // Shouldn't happen in production but helps during development
                // where bugs may trigger assertions
                // Otherwise tests could get stuck
                shardResponse.failure(Exceptions.toException(e));
                break;
            }
        }
        return new WritePrimaryResult<>(replicaRequest, shardResponse, translogLocation, indexShard);
    }

    @Override
    protected WriteReplicaResult processRequestItemsOnReplica(IndexShard indexShard, UpsertReplicaRequest request) throws IOException {
        List<Reference> columns = request.columns();
        Translog.Location location = null;
        String indexUUID = indexShard.shardId().getIndexUUID();
        boolean traceEnabled = logger.isTraceEnabled();

        Metadata metadata = clusterService.state().metadata();
        RelationMetadata relationMetadata = metadata.getRelation(indexUUID);
        if (relationMetadata == null) {
            throw new IndexNotFoundException(indexShard.shardId().getIndex());
        }
        DocTableInfo tableInfo = schemas.getTableInfo(relationMetadata.name());
        IndexMetadata indexMetadata = metadata.index(indexUUID);
        assert indexMetadata != null : "IndexMetadata for index " + indexUUID + " not found in cluster state";
        List<String> partitionValues = indexMetadata.partitionValues();

        TransactionContext txnCtx = TransactionContext.of(request.sessionSettings());

        // Refresh insertColumns References from cluster state because ObjectType
        // may have new children due to dynamic cluster state updates
        // Not doing this would result in indefinite `Mappings are not available on the replica yet` errors below
        List<Reference> targetColumns = Lists.map(columns,
            ref -> {
                Reference updatedRef = tableInfo.getReference(ref.column());
                return updatedRef == null ? ref : updatedRef;
            });

        RawIndexer rawIndexer;
        Indexer indexer;
        if (columns.get(0).column().equals(SysColumns.RAW)) {
            // Even if insertColumns supposed to have a single column _raw,
            // insertColumns can be expanded to add non-deterministic synthetics.
            // We must not check that insertColumns.length is 1
            // in order not to fall back to regular Indexer which cannot handle _raw and persists it as String.
            indexer = null;
            rawIndexer = new RawIndexer(
                partitionValues,
                tableInfo,
                indexShard.getVersionCreated(),
                txnCtx,
                nodeCtx,
                null,
                targetColumns.subList(1, targetColumns.size()) // expanded refs (non-deterministic synthetics)
            );
        } else {
            rawIndexer = null;
            indexer = new Indexer(
                partitionValues,
                tableInfo,
                indexShard.getVersionCreated(),
                txnCtx,
                nodeCtx,
                targetColumns,
                null,
                null
            );
        }
        for (UpsertReplicaRequest.Item item : request.items()) {

            // For BWC
            if (item.seqNo() == SequenceNumbers.SKIP_ON_REPLICA) {
                if (traceEnabled) {
                    logger.trace(
                        "[{} (R)] Document with id={}, marked as skip_on_replica",
                        indexShard.shardId(),
                        item.id()
                    );
                }
                continue;
            }

            long startTime = System.nanoTime();
            List<Reference> newColumns = rawIndexer != null ? rawIndexer.collectSchemaUpdates(item) : indexer.collectSchemaUpdates(item);

            if (!newColumns.isEmpty()) {
                // Even though the primary waits on all nodes to ack the mapping changes to the master
                // (see MappingUpdatedAction.updateMappingOnMaster) we still need to protect against missing mappings
                // and wait for them. The reason is concurrent requests. Request r1 which has new field f triggers a
                // mapping update. Assume that that update is first applied on the primary, and only later on the replica
                // (it’s happening concurrently). Request r2, which now arrives on the primary and which also has the new
                // field f might see the updated mapping (on the primary), and will therefore proceed to be replicated
                // to the replica. When it arrives on the replica, there’s no guarantee that the replica has already
                // applied the new mapping, so there is no other option than to wait.
                logger.trace("Mappings are not available on the replica columns={}", newColumns);
                throw new TransportReplicationAction.RetryOnReplicaException(indexShard.shardId(),
                    "Mappings are not available on the replica yet, triggered update: " + newColumns);
            }

            ParsedDocument parsedDoc = rawIndexer != null ? rawIndexer.index() : indexer.index(item);
            Term uid = new Term(SysColumns.Names.ID, Uid.encodeId(item.id()));
            boolean isRetry = false;
            Engine.Index index = new Engine.Index(
                uid,
                parsedDoc,
                item.seqNo(),
                item.primaryTerm(),
                item.version(),
                null, // versionType
                Origin.REPLICA,
                startTime,
                Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                isRetry,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM
            );
            IndexResult result = indexShard.index(index);
            if (result.getResultType() != Type.SUCCESS) {
                assert false : "doc-level index failure must not happen on replica";
                throw Exceptions.toRuntimeException(result.getFailure());
            }
            assert result.getSeqNo() == item.seqNo() : "Result of replica index operation must have item seqNo";
            location = locationToSync(location, result.getTranslogLocation());
        }
        return new WriteReplicaResult(location, indexShard);
    }

    /**
     * @param indexer is constantly used for:
     * <ul>
     *  <li>INSERT</li>
     *  <li>INSERT... ON CONFLICT DO NOTHING</li>
     *  <li></li>
     * </ul>
     * <p>
     */
    @Nullable
    private IndexItemResult indexItem(Indexer indexer,
                                      ShardUpsertRequest request,
                                      ShardUpsertRequest.Item item,
                                      IndexShard indexShard,
                                      DocTableInfo tableInfo,
                                      List<String> partitionValues,
                                      @Nullable RawIndexer rawIndexer) throws Exception {
        Object[] insertValues = item.insertValues();
        boolean isInsert = insertValues != null;
        return isInsert ? indexItemForInsert(indexer, request, item, indexShard, tableInfo, partitionValues, rawIndexer) :
            indexItemForUpdate(indexer, request, item, indexShard, tableInfo, partitionValues);
    }

    @Nullable
    private IndexItemResult indexItemForInsert(Indexer indexer,
                                               ShardUpsertRequest request,
                                               ShardUpsertRequest.Item item,
                                               IndexShard indexShard,
                                               DocTableInfo tableInfo,
                                               List<String> partitionValues,
                                               @Nullable RawIndexer rawIndexer) throws Exception {
        VersionConflictEngineException lastException = null;
        boolean fallBackToUpdate = false;
        boolean hasUpdate = item.updateAssignments() != null && item.updateAssignments().length > 0;
        long seqNo = item.seqNo();
        long primaryTerm = item.primaryTerm();
        IndexItem indexItem = item;
        for (int retryCount = 0; retryCount < MAX_RETRY_LIMIT; retryCount++) {
            try {
                boolean isRetry = retryCount > 0 || request.isRetry();
                AtomicLong version = new AtomicLong();
                if (!fallBackToUpdate) {
                    version.setPlain(request.duplicateKeyAction() == DuplicateKeyAction.OVERWRITE
                        ? Versions.MATCH_ANY
                        : Versions.MATCH_DELETED);
                } else {
                    indexItem = lookupAndConvertToInsert(
                        indexer, item, indexShard, tableInfo, partitionValues, seqNo, primaryTerm, isRetry, version);
                }
                return insert(
                    tableInfo.ident(),
                    indexer,
                    request,
                    indexItem,
                    indexShard,
                    isRetry,
                    rawIndexer,
                    version.getPlain(),
                    item.autoGeneratedTimestamp()
                );
            } catch (VersionConflictEngineException e) {
                lastException = e;
                if (request.duplicateKeyAction() == DuplicateKeyAction.IGNORE) {
                    // on conflict do nothing
                    return null;
                }
                if (hasUpdate) {
                    if (!fallBackToUpdate) {
                        // insert failed, document already exists, try update
                        fallBackToUpdate = true;
                        if (logger.isTraceEnabled()) {
                            logger.trace("[{}] Insert conflict on id={}, falling back to UPDATE", indexShard.shardId(), item.id());
                        }
                        continue;
                    } else if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO && item.version() == Versions.MATCH_ANY) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("[{}] VersionConflict, retrying operation for document id={}, version={} retryCount={}",
                                indexShard.shardId(), item.id(), item.version(), retryCount);
                        }
                        continue;
                    }
                }
                throw e;
            }
        }
        logger.warn(
            "[{}] VersionConflict for document id={}, version={} exceeded retry limit of {}, will stop retrying",
            indexShard.shardId(),
            item.id(),
            item.version(),
            MAX_RETRY_LIMIT
        );
        throw lastException;
    }

    @Nullable
    private IndexItemResult indexItemForUpdate(Indexer indexer,
                                               ShardUpsertRequest request,
                                               ShardUpsertRequest.Item item,
                                               IndexShard indexShard,
                                               DocTableInfo tableInfo,
                                               List<String> partitionValues) throws Exception {
        VersionConflictEngineException lastException = null;
        assert item.updateAssignments() != null && item.updateAssignments().length > 0;
        long seqNo = item.seqNo();
        long primaryTerm = item.primaryTerm();
        IndexItem indexItem;
        for (int retryCount = 0; retryCount < MAX_RETRY_LIMIT; retryCount++) {
            try {
                boolean isRetry = retryCount > 0 || request.isRetry();
                AtomicLong version = new AtomicLong();
                indexItem = lookupAndConvertToInsert(
                    indexer, item, indexShard, tableInfo, partitionValues, seqNo, primaryTerm, isRetry, version);
                return insert(
                    indexer,
                    request,
                    indexItem,
                    indexShard,
                    isRetry,
                    null,
                    version.getPlain(),
                    item.autoGeneratedTimestamp()
                );
            } catch (VersionConflictEngineException e) {
                lastException = e;
                if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO && item.version() == Versions.MATCH_ANY) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("[{}] VersionConflict, retrying operation for document id={}, version={} retryCount={}",
                            indexShard.shardId(), item.id(), item.version(), retryCount);
                    }
                    continue;
                }
                throw e;
            }
        }
        logger.warn(
            "[{}] VersionConflict for document id={}, version={} exceeded retry limit of {}, will stop retrying",
            indexShard.shardId(),
            item.id(),
            item.version(),
            MAX_RETRY_LIMIT
        );
        throw lastException;
    }

    private IndexItem lookupAndConvertToInsert(Indexer indexer,
                                               ShardUpsertRequest.Item item,
                                               IndexShard shard,
                                               DocTableInfo table,
                                               List<String> partitionValues,
                                               long seqNo,
                                               long primaryTerm,
                                               boolean isRetry,
                                               AtomicLong version) {
        // Get most-recent table info, could have changed (new columns, dropped columns)
        DocTableInfo actual = isRetry ? schemas.getTableInfo(table.ident()) : table;
        String id = item.id();
        Object[] excluded = item.insertValues();
        return PKLookupOperation.withDoc(
            shard, id, item.version(), VersionType.INTERNAL,
            seqNo, primaryTerm, actual, partitionValues, null,
            doc -> {
                if (doc == null) throw new DocumentMissingException(shard.shardId(), id);
                version.setPlain(doc.getVersion());
                return indexer.update(doc, item.updateAssignments(), excluded);
            }
        );
    }

    public record IndexItemResult(IndexResult result,
                                  Object[] replicaInsertValues,
                                  @Nullable Object[] returnValues) {}

    @VisibleForTesting
    protected IndexItemResult insert(RelationName tableName,
                                     Indexer indexer,
                                     ShardUpsertRequest request,
                                     IndexItem item,
                                     IndexShard indexShard,
                                     boolean isRetry,
                                     @Nullable RawIndexer rawIndexer,
                                     long version,
                                     long autoGeneratedTimestamp) throws Exception {
        final long startTime = System.nanoTime();
        List<Reference> newColumns = rawIndexer == null
            ? indexer.collectSchemaUpdates(item)
            : rawIndexer.collectSchemaUpdates(item);
        if (newColumns.isEmpty() == false) {
            var addColumnRequest = new AddColumnRequest(
                tableName,
                newColumns,
                Map.of(),
                new IntArrayList(0)
            );
            addColumnAction.execute(addColumnRequest).get();
            DocTableInfo actualTable = schemas.getTableInfo(tableName);
            if (rawIndexer == null) {
                indexer.updateTargets(actualTable::getReference);
            } else {
                rawIndexer.updateTargets(actualTable::getReference);
            }
        }

        ParsedDocument parsedDoc = rawIndexer == null ? indexer.index(item) : rawIndexer.index();
        Term uid = new Term(SysColumns.Names.ID, Uid.encodeId(item.id()));
        assert VersionType.INTERNAL.validateVersionForWrites(version);
        Engine.Index index = new Engine.Index(
            uid,
            parsedDoc,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            indexShard.getOperationPrimaryTerm(),
            version,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            startTime,
            autoGeneratedTimestamp,
            isRetry,
            item.seqNo(),
            item.primaryTerm()
        );
        IndexResult result = indexShard.index(index);
        switch (result.getResultType()) {
            case SUCCESS:
                Object[] replicaInsertValues = rawIndexer == null
                    ? indexer.addGeneratedValues(item)
                    : rawIndexer.addGeneratedValues(item);

                // returnValues need to be generated based on updated item to get access to seqNo/term
                Object[] returnValues = indexer.hasReturnValues()
                    ? indexer.returnValues(new IndexItem.StaticItem(
                        item.id(),
                        item.pkValues(),
                        replicaInsertValues,
                        result.getSeqNo(),
                        result.getTerm()))
                    : null;
                return new IndexItemResult(result, replicaInsertValues, returnValues);

            case FAILURE:
                Exception failure = result.getFailure();
                assert failure != null : "Failure must not be null if resultType is FAILURE";
                throw failure;

            case MAPPING_UPDATE_REQUIRED:
                throw new ReplicationOperation.RetryOnPrimaryException(
                    indexShard.shardId(),
                    "Dynamic mappings are not available on the node that holds the primary yet"
                );
            default:
                throw new AssertionError("IndexResult must either succeed or fail. Required mapping updates must have been handled.");
        }
    }
}
