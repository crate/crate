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
import io.crate.execution.dml.upsert.ShardWriteRequest.DuplicateKeyAction;
import io.crate.execution.jobs.TasksService;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.crate.exceptions.SQLExceptions.userFriendlyCrateExceptionTopOnly;


@Singleton
public class TransportShardInsertAction extends TransportShardAction<ShardInsertRequest, ShardInsertRequest.Item> {

    private static final String ACTION_NAME = "internal:crate:sql/data/insert";

    private final Schemas schemas;
    private final Functions functions;

    @Inject
    public TransportShardInsertAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        SchemaUpdateClient schemaUpdateClient,
        TasksService tasksService,
        IndicesService indicesService,
        ShardStateAction shardStateAction,
        Functions functions,
        Schemas schemas,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ACTION_NAME,
            transportService,
            indexNameExpressionResolver,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            ShardInsertRequest::new,
            schemaUpdateClient
        );
        this.schemas = schemas;
        this.functions = functions;
        tasksService.addListener(this);
    }

    @Override
    protected WritePrimaryResult<ShardInsertRequest, ShardResponse> processRequestItems(IndexShard indexShard,
                                                                                        ShardInsertRequest request,
                                                                                        AtomicBoolean killed) {
        ShardResponse shardResponse = new ShardResponse();
        String indexName = request.index();
        DocTableInfo tableInfo = schemas.getTableInfo(RelationName.fromIndexName(indexName), Operation.INSERT);
        Reference[] insertColumns = request.insertColumns();
        GeneratedColumns.Validation valueValidation = request.validateConstraints()
            ? GeneratedColumns.Validation.VALUE_MATCH
            : GeneratedColumns.Validation.NONE;

        TransactionContext txnCtx = TransactionContext.of(request.sessionSettings());

        InsertSourceGen insertSourceGen = InsertSourceGen.of(txnCtx,
                                                             functions,
                                                             tableInfo,
                                                             indexName,
                                                             valueValidation,
                                                             Arrays.asList(insertColumns));


        Translog.Location translogLocation = null;
        for (ShardInsertRequest.Item item : request.items()) {
            int location = item.location();
            if (killed.get()) {
                // set failure on response and skip all next items.
                // this way replica operation will be executed, but only items with a valid source (= was processed on primary)
                // will be processed on the replica
                shardResponse.failure(new InterruptedException());
                break;
            }
            try {
                Translog.Location translog = insert(request, item, indexShard, insertSourceGen);
                if (translog != null) {
                    shardResponse.add(location);
                    translogLocation = translog;
                }
            } catch (Exception e) {
                if (retryPrimaryException(e)) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }
                    throw new RuntimeException(e);
                }
                // *mark* the item as failed by setting the source to null
                // to prevent the replica operation from processing this concrete item
                item.source(null);

                if (e instanceof VersionConflictEngineException) {
                    if (request.duplicateKeyAction() == DuplicateKeyAction.IGNORE) {
                        continue;
                    }
                }

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

    @VisibleForTesting
    protected Translog.Location insert(ShardInsertRequest request,
                                       ShardInsertRequest.Item item,
                                       IndexShard indexShard,
                                       InsertSourceGen insertSourceGen) throws Exception {
        BytesReference rawSource;
        Map<String, Object> source = null;
        try {
            // This optimizes for the case where the insert value is already string-based, so we can take directly
            // the rawSource
            if (insertSourceGen instanceof FromRawInsertSource) {
                rawSource = insertSourceGen.generateSourceAndCheckConstraintsAsBytesReference(item.insertValues());
            } else {
                source = insertSourceGen.generateSourceAndCheckConstraints(item.insertValues());
                rawSource = BytesReference.bytes(XContentFactory.jsonBuilder().map(source));
            }
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
        item.source(rawSource);

        long version = request.duplicateKeyAction() == DuplicateKeyAction.OVERWRITE ? Versions.MATCH_ANY : Versions.MATCH_DELETED;
        long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

        SourceToParse sourceToParse = new SourceToParse(
            indexShard.shardId().getIndexName(),
            item.id(),
            item.source(),
            XContentType.JSON
        );

        Engine.IndexResult result;

        result = indexShard.applyIndexOperationOnPrimary(
            version,
            VersionType.INTERNAL,
            sourceToParse,
            seqNo,
            primaryTerm,
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            false
        );

        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
            mappingUpdate.updateMappings(result.getRequiredMappingUpdate(),
                                         indexShard.shardId(),
                                         Constants.DEFAULT_MAPPING_TYPE);

            result = indexShard.applyIndexOperationOnPrimary(
                version,
                VersionType.INTERNAL,
                sourceToParse,
                seqNo,
                primaryTerm,
                Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                false
            );
            if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                throw new ReplicationOperation.RetryOnPrimaryException(indexShard.shardId(),
                                                                       "Dynamic mappings are not available on the node that holds the primary yet");

            }
        }

        assert result.getFailure() instanceof ReplicationOperation.RetryOnPrimaryException == false :
            "IndexShard shouldn't use RetryOnPrimaryException. got " + result.getFailure();

        switch (result.getResultType()) {
            case SUCCESS:
                // update the seqNo and version on request for the replicas
                item.seqNo(result.getSeqNo());
                item.version(result.getVersion());
                return result.getTranslogLocation();

            case FAILURE:
                Exception failure = result.getFailure();
                assert failure != null : "Failure must not be null if resultType is FAILURE";
                throw failure;

            case MAPPING_UPDATE_REQUIRED:
            default:
                throw new AssertionError(
                    "IndexResult must either succeed or fail. Required mapping updates must have been handled.");
        }
    }

    @Override
    protected WriteReplicaResult<ShardInsertRequest> processRequestItemsOnReplica(IndexShard indexShard, ShardInsertRequest request) throws IOException {
        Translog.Location location = null;
        for (ShardInsertRequest.Item item : request.items()) {
            if (item.source() == null) {
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
        return new WriteReplicaResult<>(request, location, null, indexShard);
    }
}
