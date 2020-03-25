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

import io.crate.execution.ddl.SchemaUpdateClient;
import io.crate.execution.jobs.TasksService;
import io.crate.expression.reference.Doc;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.util.Map;


public class TransportShardUpdateAction extends TransportShardIndexAction<ShardUpdateRequest, ShardUpdateRequest.Item, TransportShardUpdateAction.Context> {

    private static final String ACTION_NAME = "internal:crate:sql/data/update";

    private final Schemas schemas;
    private final Functions functions;

    class Context {

        final UpdateSourceGen updateSourceGen;

        @Nullable
        final ReturnValueGen returnValueGen;

        public Context(UpdateSourceGen updateSourceGen, ReturnValueGen returnValueGen) {
            this.updateSourceGen = updateSourceGen;
            this.returnValueGen = returnValueGen;
        }
    }

    @Inject
    public TransportShardUpdateAction(ThreadPool threadPool,
                                      ClusterService clusterService,
                                      TransportService transportService,
                                      SchemaUpdateClient schemaUpdateClient,
                                      TasksService tasksService,
                                      IndicesService indicesService,
                                      ShardStateAction shardStateAction,
                                      Functions functions,
                                      Schemas schemas,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            schemaUpdateClient,
            tasksService,
            indicesService,
            shardStateAction,
            ShardUpdateRequest::new,
            indexNameExpressionResolver
        );
        this.schemas = schemas;
        this.functions = functions;
        tasksService.addListener(this);
    }

    public Context buildContext(ShardUpdateRequest request) {
        String indexName = request.index();
        DocTableInfo tableInfo = schemas.getTableInfo(RelationName.fromIndexName(indexName), Operation.INSERT);
        TransactionContext txnCtx = TransactionContext.of(request.sessionSettings());
        UpdateSourceGen updateSourceGen = new UpdateSourceGen(functions, txnCtx, tableInfo, request.updateColumns());
        ReturnValueGen returnValueGen = request.returnValues() == null
            ? null
            : new ReturnValueGen(functions, txnCtx, tableInfo, request.returnValues());

        return new Context(updateSourceGen, returnValueGen);
    }

    @Override
    IndexItemResponse processItem(ShardUpdateRequest.Item item, Context context, IndexShard indexShard) throws Exception {
        VersionConflictEngineException lastException = null;
        for (int retryCount = 0; retryCount < MAX_RETRY_LIMIT; retryCount++) {
            Doc fetchedDoc = getDocument(indexShard,
                                         item.id(),
                                         item.version(),
                                         item.seqNo(),
                                         item.primaryTerm());
            Map<String, Object> source = context.updateSourceGen.generateSource(
                fetchedDoc,
                item.updateAssignments(),
                new Object[0]
            );
            BytesReference rawSource = BytesReference.bytes(XContentFactory.jsonBuilder().map(source));
            item.source(rawSource);
            long seqNo = item.seqNo();
            long primaryTerm = item.primaryTerm();
            long version = Versions.MATCH_ANY;

            try {
                Engine.IndexResult indexResult = index(item.id(), item.source(), indexShard, retryCount > 0, seqNo, primaryTerm, version);
                // update the seqNo and version on request for the replicas
                item.seqNo(indexResult.getSeqNo());
                item.version(indexResult.getVersion());
                Object[] returnvalues = null;
                if (context.returnValueGen != null) {
                    returnvalues = context.returnValueGen.generateReturnValues(
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
            } catch (VersionConflictEngineException e) {
                lastException = e;
            }
        }
        logger.warn("[{}] VersionConflict for document id={}, version={} exceeded retry limit of {}, will stop retrying", indexShard.shardId(), item.id(), item.version(), MAX_RETRY_LIMIT);
        throw lastException;
    }
}
