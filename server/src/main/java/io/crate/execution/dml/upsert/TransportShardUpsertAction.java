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

import static io.crate.execution.dml.upsert.InsertSourceGen.SOURCE_WRITERS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Term;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.IndexResult;
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

import com.carrotsearch.hppc.IntArrayList;

import io.crate.Constants;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.ddl.SchemaUpdateClient;
import io.crate.execution.ddl.tables.AddColumnRequest;
import io.crate.execution.ddl.tables.TransportAddColumnAction;
import io.crate.execution.dml.Indexer;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.TransportShardAction;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.engine.collect.PKLookupOperation;
import io.crate.execution.jobs.TasksService;
import io.crate.expression.reference.Doc;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
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
        Reference[] insertColumns = request.insertColumns();
        var mapperService = indexShard.mapperService();
        Function<ColumnIdent, FieldType> getFieldType = column -> mapperService.getLuceneFieldType(column.fqn());
        TransactionContext txnCtx = TransactionContext.of(request.sessionSettings());
        Indexer indexer = insertColumns == null
            ? null
            : new Indexer(
                indexName,
                tableInfo,
                txnCtx,
                nodeCtx,
                getFieldType,
                List.of(insertColumns),
                request.returnValues()
            );

        InsertSourceGen insertSourceGen = insertColumns != null && insertColumns.length == 1 && insertColumns[0].column().equals(DocSysColumns.RAW)
            ? InsertSourceGen.of(txnCtx, nodeCtx, tableInfo, indexName, request.validation(), Arrays.asList(insertColumns))
            : null;


        UpdateSourceGen updateSourceGen = request.updateColumns() == null
            ? null
            : new UpdateSourceGen(txnCtx,
                                  nodeCtx,
                                  tableInfo,
                                  request.updateColumns());

        ReturnValueGen returnValueGen = request.returnValues() == null
            ? null
            : new ReturnValueGen(txnCtx, nodeCtx, tableInfo, request.returnValues());

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
                    updateSourceGen,
                    insertSourceGen,
                    returnValueGen
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

    @Override
    protected WriteReplicaResult<ShardUpsertRequest> processRequestItemsOnReplica(IndexShard indexShard, ShardUpsertRequest request) throws IOException {
        Translog.Location location = null;
        for (ShardUpsertRequest.Item item : request.items()) {
            if (item.seqNo() == SequenceNumbers.SKIP_ON_REPLICA || item.source() == null) {
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "[{} (R)] Document with id {}, has no source, primary operation must have failed",
                        indexShard.shardId(),
                        item.id()
                    );
                }
                continue;
            }
            SourceToParse sourceToParse = new SourceToParse(
                indexShard.shardId().getIndexName(),
                item.id(),
                item.source(),
                XContentType.JSON
            );

            Engine.IndexResult indexResult = indexShard.applyIndexOperationOnReplica(
                item.seqNo(),
                item.primaryTerm(),
                item.version(),
                Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                false,
                sourceToParse
            );
            if (indexResult.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                // Even though the primary waits on all nodes to ack the mapping changes to the master
                // (see MappingUpdatedAction.updateMappingOnMaster) we still need to protect against missing mappings
                // and wait for them. The reason is concurrent requests. Request r1 which has new field f triggers a
                // mapping update. Assume that that update is first applied on the primary, and only later on the replica
                // (it’s happening concurrently). Request r2, which now arrives on the primary and which also has the new
                // field f might see the updated mapping (on the primary), and will therefore proceed to be replicated
                // to the replica. When it arrives on the replica, there’s no guarantee that the replica has already
                // applied the new mapping, so there is no other option than to wait.
                throw new TransportReplicationAction.RetryOnReplicaException(indexShard.shardId(),
                    "Mappings are not available on the replica yet, triggered update: " + indexResult.getRequiredMappingUpdate());
            }
            location = indexResult.getTranslogLocation();
        }
        return new WriteReplicaResult<>(request, location, null, indexShard, logger);
    }

    @Nullable
    private IndexItemResponse indexItem(Indexer indexer,
                                        ShardUpsertRequest request,
                                        ShardUpsertRequest.Item item,
                                        IndexShard indexShard,
                                        @Nullable UpdateSourceGen updateSourceGen,
                                        @Nullable InsertSourceGen insertSourceGen,
                                        @Nullable ReturnValueGen returnValueGen) throws Exception {
        VersionConflictEngineException lastException = null;
        boolean tryInsertFirst = item.insertValues() != null;
        boolean isRetry;
        for (int retryCount = 0; retryCount < MAX_RETRY_LIMIT; retryCount++) {
            try {
                isRetry = retryCount > 0 || request.isRetry();
                if (tryInsertFirst) {
                    return insert(indexer, request, item, indexShard, isRetry, returnValueGen, insertSourceGen);
                } else {
                    return update(item, indexShard, isRetry, returnValueGen, updateSourceGen);
                }
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
                                       @Nullable ReturnValueGen returnGen,
                                       InsertSourceGen insertSourceGen) throws Exception {
        if (insertSourceGen instanceof RawInsertSource
                || insertSourceGen instanceof ValidatedRawInsertSource) {
            return legacyInsert(request, item, indexShard, isRetry, returnGen, insertSourceGen);
        }
        final long version = request.duplicateKeyAction() == DuplicateKeyAction.OVERWRITE
            ? Versions.MATCH_ANY
            : Versions.MATCH_DELETED;

        long startTime = System.nanoTime();
        ParsedDocument parsedDoc = indexer.index(item);

        // Remove this once replica uses indexer as well
        item.source(parsedDoc.source());

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

    private IndexItemResponse legacyInsert(ShardUpsertRequest request,
                                           ShardUpsertRequest.Item item,
                                           IndexShard indexShard,
                                           boolean isRetry,
                                           ReturnValueGen returnGen,
                                           InsertSourceGen insertSourceGen) throws Exception, IOException {
        assert insertSourceGen != null : "InsertSourceGen must not be null";
        BytesReference rawSource;
        Map<String, Object> source = null;
        try {
            // This optimizes for the case where the insert value is already string-based, so we can take directly
            // the rawSource
            if (insertSourceGen instanceof RawInsertSource) {
                rawSource = insertSourceGen.generateSourceAndCheckConstraintsAsBytesReference(item.insertValues());
            } else {
                source = insertSourceGen.generateSourceAndCheckConstraints(item.insertValues(), item.pkValues());
                rawSource = BytesReference.bytes(XContentFactory.jsonBuilder().map(source, SOURCE_WRITERS));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        item.source(rawSource);

        long version = request.duplicateKeyAction() == DuplicateKeyAction.OVERWRITE
            ? Versions.MATCH_ANY
            : Versions.MATCH_DELETED;
        long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

        Engine.IndexResult indexResult = index(item, indexShard, isRetry, seqNo, primaryTerm, version);
        Object[] returnvalues = null;
        if (returnGen != null) {
            // This optimizes for the case where the insert value is already string-based, so only parse the source
            // when return values are requested
            if (source == null) {
                source = JsonXContent.JSON_XCONTENT.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.toBytes(rawSource)).map();
            }
            returnvalues = returnGen.generateReturnValues(
                // return -1 as docId, the docId can only be retrieved by fetching the inserted document again, which
                // we want to avoid. The docId is anyway just valid with the lifetime of a searcher and can change afterwards.
                new Doc(
                    -1,
                    indexShard.shardId().getIndexName(),
                    item.id(),
                    indexResult.getVersion(),
                    indexResult.getSeqNo(),
                    indexResult.getTerm(),
                    source,
                    rawSource::utf8ToString
                )
            );
        }
        return new IndexItemResponse(indexResult.getTranslogLocation(), returnvalues);
    }

    protected IndexItemResponse update(ShardUpsertRequest.Item item,
                                       IndexShard indexShard,
                                       boolean isRetry,
                                       @Nullable ReturnValueGen returnGen,
                                       UpdateSourceGen updateSourceGen) throws Exception {
        assert updateSourceGen != null : "UpdateSourceGen must not be null";
        Doc fetchedDoc = getDocument(indexShard, item.id(), item.version(), item.seqNo(), item.primaryTerm());
        LinkedHashMap<String, Object> source = updateSourceGen.generateSource(
            fetchedDoc,
            item.updateAssignments(),
            item.insertValues()
        );
        BytesReference rawSource = BytesReference.bytes(XContentFactory.jsonBuilder().map(source, SOURCE_WRITERS));
        item.source(rawSource);
        long seqNo = item.seqNo();
        long primaryTerm = item.primaryTerm();
        long version = Versions.MATCH_ANY;

        Engine.IndexResult indexResult = index(item, indexShard, isRetry, seqNo, primaryTerm, version);
        Object[] returnvalues = null;
        if (returnGen != null) {
            returnvalues = returnGen.generateReturnValues(
                new Doc(
                    fetchedDoc.docId(),
                    fetchedDoc.getIndex(),
                    fetchedDoc.getId(),
                    indexResult.getVersion(),
                    indexResult.getSeqNo(),
                    indexResult.getTerm(),
                    source,
                    rawSource::utf8ToString
                )
            );
        }
        return new IndexItemResponse(indexResult.getTranslogLocation(), returnvalues);
    }

    private Engine.IndexResult index(ShardUpsertRequest.Item item,
                                     IndexShard indexShard,
                                     boolean isRetry,
                                     long seqNo,
                                     long primaryTerm,
                                     long version) throws Exception {
        SourceToParse sourceToParse = new SourceToParse(
            indexShard.shardId().getIndexName(),
            item.id(),
            item.source(),
            XContentType.JSON
        );

        Engine.IndexResult indexResult = executeOnPrimaryHandlingMappingUpdate(
            indexShard.shardId(),
            () -> indexShard.applyIndexOperationOnPrimary(
                version,
                VersionType.INTERNAL,
                sourceToParse,
                seqNo,
                primaryTerm,
                item.autoGeneratedTimestamp(),
                isRetry
            ),
            e -> indexShard.getFailedIndexResult(e, Versions.MATCH_ANY)
        );

        switch (indexResult.getResultType()) {
            case SUCCESS:
                // update the seqNo and version on request for the replicas
                if (logger.isTraceEnabled()) {
                    logger.trace("SUCCESS - id={}, primary_term={}, seq_no={}",
                                 item.id(),
                                 primaryTerm,
                                 indexResult.getSeqNo());
                }
                item.seqNo(indexResult.getSeqNo());
                item.version(indexResult.getVersion());
                item.primaryTerm(indexResult.getTerm());
                return indexResult;

            case FAILURE:
                Exception failure = indexResult.getFailure();
                if (logger.isTraceEnabled()) {
                    logger.trace("FAILURE - id={}, primary_term={}, seq_no={}",
                                item.id(),
                                primaryTerm,
                                indexResult.getSeqNo());
                }
                assert failure != null : "Failure must not be null if resultType is FAILURE";
                throw failure;

            case MAPPING_UPDATE_REQUIRED:
            default:
                throw new AssertionError("IndexResult must either succeed or fail. Required mapping updates must have been handled.");
        }
    }

    private static Doc getDocument(IndexShard indexShard, String id, long version, long seqNo, long primaryTerm) {
        // when sequence versioning is used, this lookup will throw VersionConflictEngineException
        Doc doc = PKLookupOperation.lookupDoc(indexShard, id, Versions.MATCH_ANY, VersionType.INTERNAL, seqNo, primaryTerm);
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

    public static Collection<ColumnIdent> getNotUsedNonGeneratedColumns(Reference[] targetColumns,
                                                                        DocTableInfo tableInfo) {
        Set<String> targetColumnsSet = new HashSet<>();
        Collection<ColumnIdent> columnsNotUsed = new ArrayList<>();

        if (targetColumns != null) {
            for (Reference targetColumn : targetColumns) {
                targetColumnsSet.add(targetColumn.column().fqn());
            }
        }

        for (var reference : tableInfo.columns()) {
            if (!reference.isNullable() && !(reference instanceof GeneratedReference || reference.defaultExpression() != null)) {
                if (!targetColumnsSet.contains(reference.column().fqn())) {
                    columnsNotUsed.add(reference.column());
                }
            }
        }
        return columnsNotUsed;
    }
}
