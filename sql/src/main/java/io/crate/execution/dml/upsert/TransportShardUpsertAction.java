/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.dml.upsert;

import com.google.common.annotations.VisibleForTesting;
import io.crate.Constants;
import io.crate.execution.ddl.SchemaUpdateClient;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.TransportShardAction;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.engine.collect.PKLookupOperation;
import io.crate.execution.jobs.TasksService;
import io.crate.expression.reference.Doc;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.crate.exceptions.SQLExceptions.userFriendlyCrateExceptionTopOnly;

/**
 * Realizes Upserts of tables which either results in an Insert or an Update.
 */
@Singleton
public class TransportShardUpsertAction extends TransportShardAction<ShardUpsertRequest, ShardUpsertRequest.Item> {

    private static final String ACTION_NAME = "internal:crate:sql/data/write";
    private static final int MAX_RETRY_LIMIT = 100_000; // upper bound to prevent unlimited retries on unexpected states

    private final Schemas schemas;
    private final Functions functions;

    @Inject
    public TransportShardUpsertAction(Settings settings,
                                      ThreadPool threadPool,
                                      ClusterService clusterService,
                                      TransportService transportService,
                                      SchemaUpdateClient schemaUpdateClient,
                                      ActionFilters actionFilters,
                                      TasksService tasksService,
                                      IndicesService indicesService,
                                      ShardStateAction shardStateAction,
                                      Functions functions,
                                      Schemas schemas,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, indexNameExpressionResolver, clusterService,
            indicesService, threadPool, shardStateAction, actionFilters, ShardUpsertRequest::new, schemaUpdateClient);
        this.schemas = schemas;
        this.functions = functions;
        tasksService.addListener(this);
    }

    @Override
    protected WritePrimaryResult<ShardUpsertRequest, ShardResponse> processRequestItems(IndexShard indexShard,
                                                                                        ShardUpsertRequest request,
                                                                                        AtomicBoolean killed) {
        ShardResponse shardResponse = new ShardResponse();
        DocTableInfo tableInfo = schemas.getTableInfo(RelationName.fromIndexName(request.index()), Operation.INSERT);
        Reference[] insertColumns = request.insertColumns();
        GeneratedColumns.Validation valueValidation = request.validateConstraints()
            ? GeneratedColumns.Validation.VALUE_MATCH
            : GeneratedColumns.Validation.NONE;

        TransactionContext txnCtx = TransactionContext.of(request.userName(), SearchPath.createSearchPathFrom(request.currentSchema()));
        InsertSourceGen insertSourceGen = insertColumns == null
            ? null
            : InsertSourceGen.of(txnCtx, functions, tableInfo, valueValidation, Arrays.asList(insertColumns));

        UpdateSourceGen updateSourceGen = request.updateColumns() == null
            ? null
            : new UpdateSourceGen(functions, txnCtx, tableInfo, request.updateColumns());

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
                translogLocation = indexItem(
                    request,
                    item,
                    indexShard,
                    item.insertValues() != null, // try insert first
                    updateSourceGen,
                    insertSourceGen
                );
                if (translogLocation != null) {
                    shardResponse.add(location);
                }
            } catch (Exception e) {
                if (retryPrimaryException(e)) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }
                    throw new RuntimeException(e);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("shardId={} failed to execute upsert for id={}", e, request.shardId(), item.id());
                }

                // *mark* the item as failed by setting the source to null
                // to prevent the replica operation from processing this concrete item
                item.source(null);

                if (!request.continueOnError()) {
                    shardResponse.failure(e);
                    break;
                }
                shardResponse.add(location,
                    new ShardResponse.Failure(
                        item.id(),
                        userFriendlyCrateExceptionTopOnly(e),
                        (e instanceof VersionConflictEngineException)));
            }
        }
        return new WritePrimaryResult<>(request, shardResponse, translogLocation, null, indexShard, logger);
    }

    @Override
    protected WriteReplicaResult<ShardUpsertRequest> processRequestItemsOnReplica(IndexShard indexShard, ShardUpsertRequest request) throws IOException {
        Translog.Location location = null;
        for (ShardUpsertRequest.Item item : request.items()) {
            if (item.source() == null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{} (R)] Document with id {}, has no source, primary operation must have failed",
                        indexShard.shardId(), item.id());
                }
                continue;
            }
            SourceToParse sourceToParse = SourceToParse.source(request.index(),
                Constants.DEFAULT_MAPPING_TYPE, item.id(), item.source(), XContentType.JSON);

            Engine.IndexResult indexResult = indexShard.applyIndexOperationOnReplica(
                item.seqNo(),
                item.version(),
                VersionType.EXTERNAL,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
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
    private Translog.Location indexItem(ShardUpsertRequest request,
                                        ShardUpsertRequest.Item item,
                                        IndexShard indexShard,
                                        boolean tryInsertFirst,
                                        @Nullable UpdateSourceGen updateSourceGen,
                                        @Nullable InsertSourceGen insertSourceGen) throws Exception {
        VersionConflictEngineException lastException = null;
        for (int retryCount = 0; retryCount < MAX_RETRY_LIMIT; retryCount++) {
            try {
                return indexItem(
                    request,
                    item,
                    indexShard,
                    tryInsertFirst,
                    updateSourceGen,
                    insertSourceGen,
                    retryCount > 0
                );
            } catch (VersionConflictEngineException e) {
                lastException = e;
                if (request.duplicateKeyAction() == DuplicateKeyAction.IGNORE) {
                    // on conflict do nothing
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

    @VisibleForTesting
    @Nullable
    protected Translog.Location indexItem(ShardUpsertRequest request,
                                          ShardUpsertRequest.Item item,
                                          IndexShard indexShard,
                                          boolean tryInsertFirst,
                                          UpdateSourceGen updateSourceGen,
                                          InsertSourceGen insertSourceGen,
                                          boolean isRetry) throws Exception {
        long version;
        // try insert first without fetching the document
        if (tryInsertFirst) {
            // set version so it will fail if already exists (will be overwritten for updates, see below)
            version = Versions.MATCH_DELETED;
            try {
                insertSourceGen.checkConstraints(item.insertValues());
                item.source(insertSourceGen.generateSource(item.insertValues()));
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
            if (request.duplicateKeyAction() == DuplicateKeyAction.OVERWRITE) {
                version = Versions.MATCH_ANY;
            }
        } else {
            Doc currentDoc = getDocument(indexShard, item.id(), item.version());
            BytesReference updatedSource = updateSourceGen.generateSource(
                currentDoc,
                item.updateAssignments(),
                item.insertValues()
            );
            item.source(updatedSource);
            version = currentDoc.getVersion();
        }

        long finalVersion = version;
        SourceToParse sourceToParse = SourceToParse.source(
            request.index(), Constants.DEFAULT_MAPPING_TYPE, item.id(), item.source(), XContentType.JSON);
        Engine.IndexResult indexResult = executeOnPrimaryHandlingMappingUpdate(
            indexShard.shardId(),
            () -> indexShard.applyIndexOperationOnPrimary(
                finalVersion,
                VersionType.INTERNAL,
                sourceToParse,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                isRetry
            ),
            e -> indexShard.getFailedIndexResult(e, finalVersion)
        );
        switch (indexResult.getResultType()) {
            case SUCCESS:
                // update the seqNo and version on request for the replicas
                item.seqNo(indexResult.getSeqNo());
                item.version(indexResult.getVersion());
                return indexResult.getTranslogLocation();

            case FAILURE:
                Exception failure = indexResult.getFailure();
                assert failure != null : "Failure must not be null if resultType is FAILURE";
                throw failure;

            case MAPPING_UPDATE_REQUIRED:
            default:
                throw new AssertionError("IndexResult must either succeed or fail. Required mapping updates must have been handled.");
        }
    }

    private static Doc getDocument(IndexShard indexShard, String id, long version) {
        Doc doc = PKLookupOperation.lookupDoc(indexShard, id, Versions.MATCH_ANY, VersionType.INTERNAL);
        if (doc == null) {
            throw new DocumentMissingException(indexShard.shardId(), Constants.DEFAULT_MAPPING_TYPE, id);
        }
        if (doc.getSource() == null) {
            throw new DocumentSourceMissingException(indexShard.shardId(), Constants.DEFAULT_MAPPING_TYPE, id);
        }
        if (version != Versions.MATCH_ANY && version != doc.getVersion()) {
            throw new VersionConflictEngineException(
                indexShard.shardId(),
                Constants.DEFAULT_MAPPING_TYPE,
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

        for (Reference reference : tableInfo.columns()) {
            if (!(reference instanceof GeneratedReference) && !reference.isNullable()) {
                if (!targetColumnsSet.contains(reference.column().fqn())) {
                    columnsNotUsed.add(reference.column());
                }
            }
        }
        return columnsNotUsed;
    }
}
