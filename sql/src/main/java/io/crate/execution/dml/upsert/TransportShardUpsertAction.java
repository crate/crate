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
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.jobs.TasksService;
import io.crate.expression.reference.Doc;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Realizes Upserts of tables which either results in an Insert or an Update.
 */
@Singleton
public class TransportShardUpsertAction extends TransportShardIndexAction<ShardUpsertRequest, ShardUpsertRequest.Item, TransportShardUpsertAction.Context> {

    private static final String ACTION_NAME = "internal:crate:sql/data/write";
    private static final int MAX_RETRY_LIMIT = 100_000; // upper bound to prevent unlimited retries on unexpected states

    private final Schemas schemas;
    private final Functions functions;

    class Context {

        final GeneratedColumns.Validation valueValidation;
        final InsertSourceGen insertSourceGen;
        final UpdateSourceGen updateSourceGen;
        final ReturnValueGen returnValueGen;
        final DuplicateKeyAction duplicateKeyAction;

        public Context(GeneratedColumns.Validation valueValidation,
                       InsertSourceGen insertSourceGen,
                       UpdateSourceGen updateSourceGen,
                       ReturnValueGen returnValueGen,
                       DuplicateKeyAction duplicateKeyAction) {
            this.valueValidation = valueValidation;
            this.insertSourceGen = insertSourceGen;
            this.updateSourceGen = updateSourceGen;
            this.returnValueGen = returnValueGen;
            this.duplicateKeyAction = duplicateKeyAction;
        }
    }

    @Inject
    public TransportShardUpsertAction(ThreadPool threadPool,
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
            ShardUpsertRequest::new,
            indexNameExpressionResolver
            );
        this.schemas = schemas;
        this.functions = functions;
        tasksService.addListener(this);
    }

    @Override
    public Context buildContext(ShardUpsertRequest request) {
        String indexName = request.index();
        DocTableInfo tableInfo = schemas.getTableInfo(RelationName.fromIndexName(indexName), Operation.INSERT);
        Reference[] insertColumns = request.insertColumns();
        GeneratedColumns.Validation valueValidation = request.validateConstraints()
            ? GeneratedColumns.Validation.VALUE_MATCH
            : GeneratedColumns.Validation.NONE;

        TransactionContext txnCtx = TransactionContext.of(request.sessionSettings());
        InsertSourceGen insertSourceGen = insertColumns == null
            ? null
            : InsertSourceGen.of(txnCtx, functions, tableInfo, indexName, valueValidation, Arrays.asList(insertColumns));

        UpdateSourceGen updateSourceGen = request.updateColumns() == null
            ? null
            : new UpdateSourceGen(functions,
                                  txnCtx,
                                  tableInfo,
                                  request.updateColumns());

        ReturnValueGen returnValueGen = request.returnValues() == null
            ? null
            : new ReturnValueGen(functions, txnCtx, tableInfo, request.returnValues());

        return new Context(valueValidation, insertSourceGen, updateSourceGen, returnValueGen, request.duplicateKeyAction());
    }

    @Nullable
    TransportShardIndexAction.IndexItemResponse processItem(ShardUpsertRequest.Item item, Context context, IndexShard indexShard) throws Exception {
        VersionConflictEngineException lastException = null;
        boolean tryInsertFirst = item.insertValues() != null;
        boolean isRetry;
        for (int retryCount = 0; retryCount < MAX_RETRY_LIMIT; retryCount++) {
            try {
                isRetry = retryCount > 0;
                if (tryInsertFirst) {
                    return insert(item, indexShard, isRetry, context);
                } else {
                    return update(item, indexShard, isRetry, context);
                }
            } catch (VersionConflictEngineException e) {
                lastException = e;
                if (context.duplicateKeyAction == DuplicateKeyAction.IGNORE) {
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

    protected IndexItemResponse insert(ShardUpsertRequest.Item item,
                                       IndexShard indexShard,
                                       boolean isRetry,
                                       Context context) throws Exception {
        assert context.insertSourceGen != null : "InsertSourceGen must not be null";
        BytesReference rawSource;
        Map<String, Object> source = null;
        try {
            // This optimizes for the case where the insert value is already string-based, so we can take directly
            // the rawSource
            if (context.insertSourceGen instanceof FromRawInsertSource) {
                rawSource = context.insertSourceGen.generateSourceAndCheckConstraintsAsBytesReference(item.insertValues());
            } else {
                source = context.insertSourceGen.generateSourceAndCheckConstraints(item.insertValues());
                rawSource = BytesReference.bytes(XContentFactory.jsonBuilder().map(source));
            }
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
        item.source(rawSource);

        long version = context.duplicateKeyAction == DuplicateKeyAction.OVERWRITE ? Versions.MATCH_ANY : Versions.MATCH_DELETED;
        long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

        Engine.IndexResult indexResult = index(item.id(), item.source(), indexShard, isRetry, seqNo, primaryTerm, version);
        item.seqNo(indexResult.getSeqNo());
        item.version(indexResult.getVersion());
        Object[] returnvalues = null;
        if (context.returnValueGen != null) {
            // This optimizes for the case where the insert value is already string-based, so only parse the source
            // when return values are requested
            if (source == null) {
                source = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.toBytes(rawSource)).map();
            }
            returnvalues = context.returnValueGen.generateReturnValues(
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
                                       Context context) throws Exception {
        assert context.updateSourceGen != null : "UpdateSourceGen must not be null";
        Doc fetchedDoc = getDocument(indexShard, item.id(), item.version(), item.seqNo(), item.primaryTerm());
        Map<String, Object> source = context.updateSourceGen.generateSource(
            fetchedDoc,
            item.updateAssignments(),
            item.insertValues()
        );
        BytesReference rawSource = BytesReference.bytes(XContentFactory.jsonBuilder().map(source));
        item.source(rawSource);
        long seqNo = item.seqNo();
        long primaryTerm = item.primaryTerm();
        long version = Versions.MATCH_ANY;
        Engine.IndexResult indexResult = index(item.id(), item.source(), indexShard, isRetry, seqNo, primaryTerm, version);
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
    }

}
