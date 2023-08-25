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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Term;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.IndexResult;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.Constants;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.ddl.SchemaUpdateClient;
import io.crate.execution.ddl.tables.AddColumnRequest;
import io.crate.execution.ddl.tables.TransportAddColumnAction;
import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.Indexer;
import io.crate.execution.dml.RawIndexer;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.TransportShardAction;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.engine.collect.PKLookupOperation;
import io.crate.execution.jobs.TasksService;
import io.crate.expression.reference.Doc;
import io.crate.expression.reference.doc.lucene.SourceParser;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;

/**
 * Realizes Upserts of tables which either results in an Insert or an Update.
 */
@Singleton
public class TransportShardUpsertAction extends TransportShardAction<ShardUpsertRequest, ShardUpsertRequest.Item> {

    private static final int MAX_RETRY_LIMIT = 100_000; // upper bound to prevent unlimited retries on unexpected states

    private final Schemas schemas;
    private final NodeContext nodeCtx;
    private final TransportAddColumnAction addColumnAction;


    @Inject
    public TransportShardUpsertAction(Settings settings,
                                      ThreadPool threadPool,
                                      ClusterService clusterService,
                                      TransportService transportService,
                                      SchemaUpdateClient schemaUpdateClient,
                                      TransportAddColumnAction addColumnAction,
                                      TasksService tasksService,
                                      IndicesService indicesService,
                                      ShardStateAction shardStateAction,
                                      NodeContext nodeCtx,
                                      Schemas schemas) {
        super(
            settings,
            ShardUpsertAction.NAME,
            transportService,
            clusterService,
            indicesService,
            tasksService,
            threadPool,
            shardStateAction,
            ShardUpsertRequest::new,
            schemaUpdateClient
        );
        this.schemas = schemas;
        this.nodeCtx = nodeCtx;
        this.addColumnAction = addColumnAction;
        tasksService.addListener(this);
    }

    @Override
    protected WritePrimaryResult<ShardUpsertRequest, ShardResponse> processRequestItems(IndexShard indexShard,
                                                                                        ShardUpsertRequest request,
                                                                                        AtomicBoolean killed) {
        ShardResponse shardResponse = new ShardResponse(request.returnValues());
        String indexName = request.index();
        DocTableInfo tableInfo = schemas.getTableInfo(RelationName.fromIndexName(indexName), Operation.INSERT);
        var mapperService = indexShard.mapperService();
        Function<ColumnIdent, FieldType> getFieldType = column -> mapperService.getLuceneFieldType(column.fqn());
        TransactionContext txnCtx = TransactionContext.of(request.sessionSettings());
        UpdateToInsert updateToInsert = request.updateColumns() == null || request.updateColumns().length == 0
            ? null
            : new UpdateToInsert(
                nodeCtx,
                txnCtx,
                tableInfo,
                request.updateColumns(),
                request.insertColumns()
            );
        List<Reference> insertColumns;
        if (updateToInsert != null) {
            insertColumns = updateToInsert.columns();

            // updateToInsert.columns() must have an exact overlap with the insert
            // target-column-list because the supplied values
            // will be in that order.
            //
            // Example where it adds columns:
            //
            // INSERT INTO tbl (x) VALUES (1)
            //  ON CONFLICT (x) DO UPDATE SET y = 20
            //
            //  Would need to have [x, y, ...]
            assert request.insertColumns() == null || insertColumns.subList(0, request.insertColumns().length).equals(Arrays.asList(request.insertColumns()))
                : "updateToInsert.columns() must be a superset of insertColumns where the start is an exact overlap. It may only add new columns at the end";
        } else {
            insertColumns = List.of(request.insertColumns());
        }
        Indexer indexer = new Indexer(
            indexName,
            tableInfo,
            txnCtx,
            nodeCtx,
            getFieldType,
            insertColumns,
            request.returnValues()
        );
        if (indexer.hasUndeterministicSynthetics()) {
            // This change also applies for RawIndexer if it's used.
            // RawIndexer adds non-deterministic generated columns in addition to _raw and uses same request.
            request.insertColumns(indexer.insertColumns(insertColumns));
        }
        RawIndexer rawIndexer = null;
        if (insertColumns.get(0).column().equals(DocSysColumns.RAW)) {
            rawIndexer = new RawIndexer(
                indexName,
                tableInfo,
                txnCtx,
                nodeCtx,
                getFieldType,
                request.returnValues(),
                List.of() // Non deterministic synthetics is not needed on primary
            );
        }

        Translog.Location translogLocation = null;
        for (ShardUpsertRequest.Item item : request.items()) {
            int location = item.location();
            if (killed.get()) {
                // set failure on response and skip all next items.
                // this way replica operation will be executed, but only items with a valid source (= was processed on primary)
                // will be processed on the replica
                shardResponse.failure(new InterruptedException());
                break;
            }
            try {
                IndexItemResponse indexItemResponse = indexItem(
                    indexer,
                    request,
                    item,
                    indexShard,
                    tableInfo,
                    updateToInsert,
                    rawIndexer
                );
                if (indexItemResponse != null) {
                    if (indexItemResponse.translog != null) {
                        shardResponse.add(location);
                        translogLocation = indexItemResponse.translog;
                    }
                    if (indexItemResponse.returnValues != null) {
                        shardResponse.addResultRows(indexItemResponse.returnValues);
                    }
                }
            } catch (Exception e) {
                if (retryPrimaryException(e)) {
                    throw Exceptions.toRuntimeException(e);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Failed to execute upsert on nodeName={}, shardId={} id={} error={}", clusterService.localNode().getName(), request.shardId(), item.id(), e);
                }

                // *mark* the item as failed by setting the source to null
                // to prevent the replica operation from processing this concrete item
                item.source(null);
                item.seqNo(SequenceNumbers.SKIP_ON_REPLICA);

                if (!request.continueOnError()) {
                    shardResponse.failure(e);
                    break;
                }
                shardResponse.add(location,
                    new ShardResponse.Failure(
                        item.id(),
                        getExceptionMessage(e),
                        (e instanceof VersionConflictEngineException)));
            } catch (AssertionError e) {
                // Shouldn't happen in production but helps during development
                // where bugs may trigger assertions
                // Otherwise tests could get stuck
                shardResponse.failure(Exceptions.toException(e));
                break;
            }
        }
        return new WritePrimaryResult<>(request, shardResponse, translogLocation, null, indexShard);
    }

    private static String getExceptionMessage(Throwable e) {
        if (SQLExceptions.isDocumentAlreadyExistsException(e)) {
            return "A document with the same primary key exists already";
        }
        var message = e.getMessage();
        return message != null ? message : e.getClass().getName();
    }

    private WriteReplicaResult<ShardUpsertRequest> legacyReplicaOp(IndexShard indexShard, ShardUpsertRequest request) throws IOException {
        boolean traceEnabled = logger.isTraceEnabled();
        Translog.Location location = null;
        for (ShardUpsertRequest.Item item : request.items()) {
            if (item.seqNo() == SequenceNumbers.SKIP_ON_REPLICA || item.source() == null) {
                if (traceEnabled) {
                    logger.trace(
                        "[{} (R)] Document with id={}, marked as skip_on_replica",
                        indexShard.shardId(),
                        item.id()
                    );
                }
                continue;
            }
            SourceToParse sourceToParse = new SourceToParse(
                request.index(),
                item.id(),
                item.source(),
                XContentType.JSON
            );
            IndexResult result = indexShard.applyIndexOperationOnReplica(
                item.seqNo(),
                item.primaryTerm(),
                item.version(),
                Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                false,
                sourceToParse
            );
            if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                // Even though the primary waits on all nodes to ack the mapping changes to the master
                // (see MappingUpdatedAction.updateMappingOnMaster) we still need to protect against missing mappings
                // and wait for them. The reason is concurrent requests. Request r1 which has new field f triggers a
                // mapping update. Assume that that update is first applied on the primary, and only later on the replica
                // (it’s happening concurrently). Request r2, which now arrives on the primary and which also has the new
                // field f might see the updated mapping (on the primary), and will therefore proceed to be replicated
                // to the replica. When it arrives on the replica, there’s no guarantee that the replica has already
                // applied the new mapping, so there is no other option than to wait.
                throw new TransportReplicationAction.RetryOnReplicaException(indexShard.shardId(),
                    "Mappings are not available on the replica yet, triggered update: " + result.getRequiredMappingUpdate());
            }
            location = result.getTranslogLocation();
        }
        return new WriteReplicaResult<>(location, null, indexShard);
    }

    @Override
    protected WriteReplicaResult<ShardUpsertRequest> processRequestItemsOnReplica(IndexShard indexShard, ShardUpsertRequest request) throws IOException {
        Reference[] insertColumns = request.insertColumns();
        if (insertColumns == null) {
            return legacyReplicaOp(indexShard, request);
        }
        // If an item has a source, parse it instead of using Indexer
        // The request may come from an older node in a mixed cluster.
        // In that case the insertColumns/values won't always include undeterministic generated columns
        // and we'd risk generating new diverging values on the replica
        for (var item : request.items()) {
            if (item.source() != null) {
                return legacyReplicaOp(indexShard, request);
            }
        }


        Translog.Location location = null;
        String indexName = request.index();
        boolean traceEnabled = logger.isTraceEnabled();
        RelationName relationName = RelationName.fromIndexName(indexName);
        DocTableInfo tableInfo = schemas.getTableInfo(relationName, Operation.INSERT);
        var mapperService = indexShard.mapperService();
        Function<ColumnIdent, FieldType> getFieldType = column -> mapperService.getLuceneFieldType(column.fqn());
        TransactionContext txnCtx = TransactionContext.of(request.sessionSettings());

        // Refresh insertColumns References from cluster state because ObjectType
        // may have new children due to dynamic cluster state updates
        // Not doing this would result in indefinite `Mappings are not available on the replica yet` errors below
        List<Reference> targetColumns = Stream.of(insertColumns)
            .map(ref -> {
                Reference updatedRef = tableInfo.getReference(ref.column());
                return updatedRef == null ? ref : updatedRef;
            })
            .toList();

        RawIndexer rawIndexer;
        Indexer indexer;
        if (insertColumns[0].column().equals(DocSysColumns.RAW)) {
            // Even if insertColumns supposed to have a single column _raw,
            // insertColumns can be expanded to add non-deterministic synthetics.
            // We must not check that insertColumns.length is 1
            // in order not to fall back to regular Indexer which cannot handle _raw and persists it as String.
            indexer = null;
            rawIndexer = new RawIndexer(
                indexName,
                tableInfo,
                txnCtx,
                nodeCtx,
                getFieldType,
                null,
                targetColumns.subList(1, targetColumns.size()) // expanded refs (non-deterministic synthetics)
            );
        } else {
            rawIndexer = null;
            indexer = new Indexer(
                indexName,
                tableInfo,
                txnCtx,
                nodeCtx,
                getFieldType,
                targetColumns,
                null
            );
        }
        for (ShardUpsertRequest.Item item : request.items()) {
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
            ParsedDocument parsedDoc = indexer == null ? rawIndexer.index(item) : indexer.index(item);
            if (!parsedDoc.newColumns().isEmpty()) {
                // this forces clearing the cache
                schemas.tableExists(relationName);

                // Even though the primary waits on all nodes to ack the mapping changes to the master
                // (see MappingUpdatedAction.updateMappingOnMaster) we still need to protect against missing mappings
                // and wait for them. The reason is concurrent requests. Request r1 which has new field f triggers a
                // mapping update. Assume that that update is first applied on the primary, and only later on the replica
                // (it’s happening concurrently). Request r2, which now arrives on the primary and which also has the new
                // field f might see the updated mapping (on the primary), and will therefore proceed to be replicated
                // to the replica. When it arrives on the replica, there’s no guarantee that the replica has already
                // applied the new mapping, so there is no other option than to wait.
                logger.trace("Mappings are not available on the replica columns={}", parsedDoc.newColumns());
                throw new TransportReplicationAction.RetryOnReplicaException(indexShard.shardId(),
                    "Mappings are not available on the replica yet, triggered update: " + parsedDoc.newColumns());
            }
            Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(item.id()));
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
            assert result.getResultType() != Engine.Result.Type.MAPPING_UPDATE_REQUIRED
                : "If parsedDoc.newColumns is empty there must be no mapping update requirement";
            location = result.getTranslogLocation();
        }
        return new WriteReplicaResult<>(location, null, indexShard);
    }

    @Nullable
    private IndexItemResponse indexItem(Indexer indexer,
                                        ShardUpsertRequest request,
                                        ShardUpsertRequest.Item item,
                                        IndexShard indexShard,
                                        DocTableInfo tableInfo,
                                        @Nullable UpdateToInsert updateToInsert,
                                        @Nullable RawIndexer rawIndexer) throws Exception {
        VersionConflictEngineException lastException = null;
        Object[] insertValues = item.insertValues();
        boolean tryInsertFirst = insertValues != null;
        for (int retryCount = 0; retryCount < MAX_RETRY_LIMIT; retryCount++) {
            try {
                boolean isRetry = retryCount > 0 || request.isRetry();
                long version;
                if (tryInsertFirst) {
                    version = request.duplicateKeyAction() == DuplicateKeyAction.OVERWRITE
                        ? Versions.MATCH_ANY
                        : Versions.MATCH_DELETED;
                } else {
                    SourceParser sourceParser;
                    if (item.updateAssignments() != null && item.updateAssignments().length > 0) {
                        // Use the source parser without registering any concrete column to get the complete
                        // source which is required to write a new document with the updated values
                        sourceParser = new SourceParser(tableInfo.droppedColumns());
                    } else {
                        // No source is required for simple inserts and duplicate detection
                        sourceParser = null;
                    }
                    Doc doc = getDocument(
                        indexShard,
                        item.id(),
                        item.version(),
                        item.seqNo(),
                        item.primaryTerm(),
                        sourceParser);
                    version = doc.getVersion();
                    IndexItem indexItem = updateToInsert.convert(doc, item.updateAssignments(), insertValues);
                    item.pkValues(indexItem.pkValues());
                    item.insertValues(indexItem.insertValues());
                    request.insertColumns(indexer.insertColumns(updateToInsert.columns()));
                }
                return insert(
                    indexer,
                    request,
                    item,
                    indexShard,
                    isRetry,
                    rawIndexer,
                    version
                );
            } catch (VersionConflictEngineException e) {
                lastException = e;
                if (request.duplicateKeyAction() == DuplicateKeyAction.IGNORE) {
                    // on conflict do nothing
                    item.seqNo(SequenceNumbers.SKIP_ON_REPLICA);
                    item.source(null);
                    return null;
                }
                Symbol[] updateAssignments = item.updateAssignments();
                if (updateAssignments != null && updateAssignments.length > 0) {
                    if (tryInsertFirst) {
                        // insert failed, document already exists, try update
                        tryInsertFirst = false;
                        continue;
                    } else if (item.retryOnConflict()) {
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
        logger.warn("[{}] VersionConflict for document id={}, version={} exceeded retry limit of {}, will stop retrying",
            indexShard.shardId(), item.id(), item.version(), MAX_RETRY_LIMIT);
        throw lastException;
    }

    static class IndexItemResponse {
        @Nullable
        final Translog.Location translog;
        @Nullable
        final Object[] returnValues;

        IndexItemResponse(@Nullable Translog.Location translog, @Nullable Object[] returnValues) {
            this.translog = translog;
            this.returnValues = returnValues;
        }
    }

    @VisibleForTesting
    protected IndexItemResponse insert(Indexer indexer,
                                       ShardUpsertRequest request,
                                       ShardUpsertRequest.Item item,
                                       IndexShard indexShard,
                                       boolean isRetry,
                                       @Nullable RawIndexer rawIndexer,
                                       long version) throws Exception {
        final long startTime = System.nanoTime();
        ParsedDocument parsedDoc = rawIndexer == null ? indexer.index(item) : rawIndexer.index(item);

        // Replica must use the same values for undeterministic defaults/generated columns
        if (rawIndexer == null && indexer.hasUndeterministicSynthetics()) {
            item.insertValues(indexer.addGeneratedValues(item));
        } else if (rawIndexer != null && rawIndexer.hasUndeterministicSynthetics()) {
            item.insertValues(rawIndexer.addGeneratedValues(item));
        }

        if (!parsedDoc.newColumns().isEmpty()) {
            var addColumnRequest = new AddColumnRequest(
                RelationName.fromIndexName(indexShard.shardId().getIndexName()),
                parsedDoc.newColumns(),
                Map.of(),
                new IntArrayList(0)
            );
            addColumnAction.execute(addColumnRequest).get();
        }

        Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(item.id()));
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
            item.autoGeneratedTimestamp(),
            isRetry,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );
        IndexResult result = indexShard.index(index);
        switch (result.getResultType()) {
            case SUCCESS:
                item.seqNo(result.getSeqNo());
                item.version(result.getVersion());
                item.primaryTerm(result.getTerm());
                return new IndexItemResponse(result.getTranslogLocation(), indexer.returnValues(item));

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

    private static Doc getDocument(IndexShard indexShard,
                                   String id,
                                   long version,
                                   long seqNo,
                                   long primaryTerm,
                                   @Nullable SourceParser sourceParser) {
        // when sequence versioning is used, this lookup will throw VersionConflictEngineException
        Doc doc = PKLookupOperation.lookupDoc(
                indexShard, id, Versions.MATCH_ANY, VersionType.INTERNAL, seqNo, primaryTerm, sourceParser);
        if (doc == null) {
            throw new DocumentMissingException(indexShard.shardId(), Constants.DEFAULT_MAPPING_TYPE, id);
        }
        if (doc.getSource() == null) {
            throw new DocumentSourceMissingException(indexShard.shardId(), Constants.DEFAULT_MAPPING_TYPE, id);
        }
        if (version != Versions.MATCH_ANY && version != doc.getVersion()) {
            throw new VersionConflictEngineException(
                indexShard.shardId(),
                id,
                "Requested version: " + version + " but got version: " + doc.getVersion());
        }
        return doc;
    }
}
