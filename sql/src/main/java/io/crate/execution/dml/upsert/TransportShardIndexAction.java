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

import io.crate.Constants;
import io.crate.execution.ddl.SchemaUpdateClient;
import io.crate.execution.dml.ShardRequest;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.TransportShardAction;
import io.crate.execution.engine.collect.PKLookupOperation;
import io.crate.execution.jobs.TasksService;
import io.crate.expression.reference.Doc;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.uid.Versions;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static io.crate.exceptions.SQLExceptions.userFriendlyCrateExceptionTopOnly;

@Singleton
public abstract class TransportShardIndexAction<Request extends ShardRequest<Request, Item> & TransportShardIndexAction.HasReturnValues,
    Item extends ShardRequest.Item & TransportShardIndexAction.HasSource, Context extends TransportShardIndexAction.ContextValues>
    extends TransportShardAction<Request, Item> {

    private static final String ACTION_NAME = "internal:crate:sql/data/update";
    private static final int MAX_RETRY_LIMIT = 100_000; // upper bound to prevent unlimited retries on unexpected states

    private final Schemas schemas;
    private final Functions functions;

    interface HasReturnValues {

        Symbol[] returnValues();

        boolean continueOnError();

    }

    interface HasSource {

        void source(BytesReference source);

        BytesReference source();
    }

    interface ContextValues {

        IndexShard indexShard();

        void retry(boolean retry);

        boolean retry();
    }

    public abstract Context generateContext(IndexShard indexShard, Request request);

    public abstract IndexItemResponse process(Item item, Context context) throws Exception;

    @Inject
    public TransportShardIndexAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        SchemaUpdateClient schemaUpdateClient,
        TasksService tasksService,
        IndicesService indicesService,
        ShardStateAction shardStateAction,
        Functions functions,
        Schemas schemas,
        Writeable.Reader<Request> reader,
        IndexNameExpressionResolver indexNameExpressionResolver) {
        super(
            actionName,
            transportService,
            indexNameExpressionResolver,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            reader,
            schemaUpdateClient
        );
        this.schemas = schemas;
        this.functions = functions;
        tasksService.addListener(this);
    }

    // can be generic
    @Override
    protected WritePrimaryResult<Request, ShardResponse> processRequestItems(IndexShard indexShard,
                                                                                        Request request,
                                                                                        AtomicBoolean killed) {
        ShardResponse shardResponse = new ShardResponse(request.returnValues());
        Context updateContext = generateContext(indexShard, request);

        Translog.Location translogLocation = null;
        for (Item item : request.items()) {
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
                    item,
                    updateContext
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
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }
                    throw new RuntimeException(e);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Failed to execute upsert shardId={} id={} error={}", request.shardId(), item.id(), e);
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
        return new WritePrimaryResult<>(request, shardResponse, translogLocation, null, indexShard);
    }

    // can be generic
    @Override
    protected WriteReplicaResult<Request> processRequestItemsOnReplica(IndexShard indexShard, Request request) throws IOException {
        Translog.Location location = null;
        for (Item item : request.items()) {
            if (item.source() == null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{} (R)] Document with id {}, has no source, primary operation must have failed",
                                 indexShard.shardId(), item.id());
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
    private IndexItemResponse indexItem(Item item, Context context) throws Exception {
        VersionConflictEngineException lastException = null;
        boolean isRetry;
        for (int retryCount = 0; retryCount < MAX_RETRY_LIMIT; retryCount++) {
            try {
                context.retry(retryCount > 0);
                return process(item, context);
            } catch (VersionConflictEngineException e) {
                lastException = e;
                /*
                if (context.request.duplicateKeyAction() == ShardUpdateRequest.DuplicateKeyAction.IGNORE) {
                     on conflict do nothing
                    item.source(null);
                    return null;
                }
                */
            }
        }
        logger.warn("[{}] VersionConflict for document id={}, version={} exceeded retry limit of {}, will stop retrying",
                    context.indexShard().shardId(), item.id(), item.version(), MAX_RETRY_LIMIT);
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

    protected Engine.IndexResult index(String id,
                                     BytesReference source,
                                     IndexShard indexShard,
                                     boolean isRetry,
                                     long seqNo,
                                     long primaryTerm,
                                     long version) throws Exception {
        SourceToParse sourceToParse = new SourceToParse(
            indexShard.shardId().getIndexName(),
            id,
            source,
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
                Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                isRetry
            ),
            e -> indexShard.getFailedIndexResult(e, Versions.MATCH_ANY)
        );

        switch (indexResult.getResultType()) {
            case SUCCESS:
                // update the seqNo and version on request for the replicas
                if (logger.isTraceEnabled()) {
                    logger.trace("SUCCESS - id={}, primary_term={}, seq_no={}",
                                 id,
                                 primaryTerm,
                                 indexResult.getSeqNo());
                }
                return indexResult;

            case FAILURE:
                Exception failure = indexResult.getFailure();
                if (logger.isTraceEnabled()) {
                    logger.trace("FAILURE - id={}, primary_term={}, seq_no={}",
                                 id,
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

    static Doc getDocument(IndexShard indexShard, String id, long version, long seqNo, long primaryTerm) {
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
}
