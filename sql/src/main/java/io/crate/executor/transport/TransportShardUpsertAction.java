/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.crate.Constants;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Reference;
import io.crate.executor.transport.task.elasticsearch.FieldExtractor;
import io.crate.executor.transport.task.elasticsearch.FieldExtractorFactory;
import io.crate.executor.transport.task.elasticsearch.SymbolToFieldExtractor;
import io.crate.jobs.JobContextService;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class TransportShardUpsertAction extends TransportShardAction<ShardUpsertRequest> {

    private final static String ACTION_NAME = "indices:crate/data/write/upsert";
    private final static SymbolToFieldExtractor<GetResult> SYMBOL_TO_FIELD_EXTRACTOR =
            new SymbolToFieldExtractor<>(new GetResultFieldExtractorFactory());

    private final IndicesService indicesService;
    private final Functions functions;
    private final Schemas schemas;

    @Inject
    public TransportShardUpsertAction(Settings settings,
                                      ThreadPool threadPool,
                                      ClusterService clusterService,
                                      TransportService transportService,
                                      ActionFilters actionFilters,
                                      JobContextService jobContextService,
                                      IndicesService indicesService,
                                      ShardStateAction shardStateAction,
                                      Functions functions,
                                      Schemas schemas,
                                      MappingUpdatedAction mappingUpdatedAction,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, mappingUpdatedAction, indexNameExpressionResolver, clusterService,
                indicesService, threadPool, shardStateAction, actionFilters, ShardUpsertRequest.class);
        this.indicesService = indicesService;
        this.functions = functions;
        this.schemas = schemas;
        jobContextService.addListener(this);
    }

    @Override
    protected boolean checkWriteConsistency() {
        return false;
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
        IndexRoutingTable routingTable = clusterState.routingTable().index(request.concreteIndex());
        if (routingTable == null) {
            throw new IndexNotFoundException(request.concreteIndex());
        }
        return routingTable.shard(request.request().shardId().id()).shardsIt();
    }

    @Override
    protected ShardResponse processRequestItems(ShardId shardId,
                                                ShardUpsertRequest request,
                                                AtomicBoolean killed) {
        ShardResponse shardResponse = new ShardResponse();
        DocTableInfo tableInfo = schemas.getWritableTable(TableIdent.fromIndexName(request.index()));
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.shardSafe(shardId.id());

        Translog.Location translogLocation = null;
        for (int i = 0; i < request.itemIndices().size(); i++) {
            int location = request.itemIndices().get(i);
            ShardUpsertRequest.Item item = request.items().get(i);
            if (killed.get()) {
                throw new CancellationException();
            }
            try {
                translogLocation = indexItem(
                        tableInfo,
                        request,
                        item,
                        indexShard,
                        item.insertValues() != null, // try insert first
                        0);
                shardResponse.add(location);
            } catch (Throwable t) {
                if (retryPrimaryException(t)) {
                    Throwables.propagate(t);
                }
                logger.debug("{} failed to execute upsert for [{}]/[{}]",
                        t, request.shardId(), request.type(), item.id());
                if (!request.continueOnError()) {
                    shardResponse.failure(t);
                    break;
                }
                shardResponse.add(location,
                        new ShardResponse.Failure(
                                item.id(),
                                ExceptionsHelper.detailedMessage(t),
                                (t instanceof VersionConflictEngineException)));
            }
        }
        if (indexShard.getTranslogDurability() == Translog.Durabilty.REQUEST && translogLocation != null) {
            indexShard.sync(translogLocation);
        }
        return shardResponse;
    }

    @Override
    protected void processRequestItemsOnReplica(ShardId shardId, ShardUpsertRequest request, AtomicBoolean killed) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.shardSafe(shardId.id());
        for (int i = 0; i < request.itemIndices().size(); i++) {
            ShardUpsertRequest.Item item = request.items().get(i);
            if (item.source() == null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{} (R)] Document with id {}, has no source, primary operation must have failed",
                            indexShard.shardId(), item.id());
                }
                continue;
            }
            shardIndexOperationOnReplica(request, item, indexShard);
        }
    }

    protected Translog.Location indexItem(DocTableInfo tableInfo,
                                          ShardUpsertRequest request,
                                          ShardUpsertRequest.Item item,
                                          IndexShard indexShard,
                                          boolean tryInsertFirst,
                                          int retryCount) throws Throwable {
        try {
            long version = item.version();
            if (tryInsertFirst) {
                // try insert first without fetching the document
                try {
                    item.source(prepareInsert(tableInfo, request, item));
                } catch (IOException e) {
                    throw ExceptionsHelper.convertToElastic(e);
                }
                if (!request.overwriteDuplicates()) {
                    item.opType(IndexRequest.OpType.CREATE);
                } else {
                    item.opType(IndexRequest.OpType.INDEX);
                }
            } else {
                item.opType(IndexRequest.OpType.INDEX);
                SourceAndVersion sourceAndVersion = prepareUpdate(tableInfo, request, item, indexShard);
                item.source(sourceAndVersion.source);
                version = sourceAndVersion.version;
            }
            return shardIndexOperation(request, item, version, indexShard);
        } catch (VersionConflictEngineException e) {
            if (item.retryOnConflict()) {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] VersionConflict, retrying operation for document id {}, retry count: {}",
                            indexShard.shardId(), item.id(), retryCount);
                }
                return indexItem(tableInfo, request, item, indexShard, false, retryCount + 1);
            }
            throw e;
        } catch (DocumentAlreadyExistsException e) {
            if (tryInsertFirst && item.updateAssignments() != null) {
                // insert failed, document already exists, try update
                return indexItem(tableInfo, request, item, indexShard, false, 0);
            }
            throw e;
        }
    }

    /**
     * Prepares an update request by converting it into an index request.
     * <p/>
     * TODO: detect a NOOP and return an update response if true
     */
    @SuppressWarnings("unchecked")
    public SourceAndVersion prepareUpdate(DocTableInfo tableInfo,
                                          ShardUpsertRequest request,
                                          ShardUpsertRequest.Item item,
                                          IndexShard indexShard) throws ElasticsearchException {
        final GetResult getResult = indexShard.getService().get(request.type(), item.id(),
                new String[]{RoutingFieldMapper.NAME, ParentFieldMapper.NAME, TTLFieldMapper.NAME},
                true, Versions.MATCH_ANY, VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE, false);

        if (!getResult.isExists()) {
            throw new DocumentMissingException(new ShardId(request.index(), request.shardId().id()), request.type(), item.id());
        }

        if (getResult.internalSourceRef() == null) {
            // no source, we can't do nothing, through a failure...
            throw new DocumentSourceMissingException(new ShardId(request.index(), request.shardId().id()), request.type(), item.id());
        }

        if (item.version() != Versions.MATCH_ANY && item.version() != getResult.getVersion()) {
            throw new VersionConflictEngineException(
                    indexShard.shardId(), Constants.DEFAULT_MAPPING_TYPE, item.id(), getResult.getVersion(), item.version());
        }

        Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final Map<String, Object> updatedSourceAsMap;
        final XContentType updateSourceContentType = sourceAndContent.v1();

        updatedSourceAsMap = sourceAndContent.v2();

        SymbolToFieldExtractorContext ctx = new SymbolToFieldExtractorContext(functions, item.insertValues());

        Map<String, Object> pathsToUpdate = new LinkedHashMap<>();
        Map<String, Object> updatedGeneratedColumns = new LinkedHashMap<>();
        for (int i = 0; i < request.updateColumns().length; i++) {
            /**
             * NOTE: mapping isn't applied. So if an Insert was done using the ES Rest Endpoint
             * the data might be returned in the wrong format (date as string instead of long)
             */
            String columnPath = request.updateColumns()[i];
            Object value = SYMBOL_TO_FIELD_EXTRACTOR.convert(item.updateAssignments()[i], ctx).extract(getResult);
            ReferenceInfo referenceInfo = tableInfo.getReferenceInfo(ColumnIdent.fromPath(columnPath));
            if (referenceInfo instanceof GeneratedReferenceInfo) {
                updatedGeneratedColumns.put(columnPath, value);

            } else {
                pathsToUpdate.put(columnPath, value);
            }
        }

        processGeneratedColumns(tableInfo, pathsToUpdate, updatedGeneratedColumns, request.validateGeneratedColumns(), getResult);

        updateSourceByPaths(updatedSourceAsMap, pathsToUpdate);

        try {
            XContentBuilder builder = XContentFactory.contentBuilder(updateSourceContentType);
            builder.map(updatedSourceAsMap);
            return new SourceAndVersion(builder.bytes(), getResult.getVersion());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + updatedSourceAsMap + "]", e);
        }
    }

    private BytesReference prepareInsert(DocTableInfo tableInfo,
                                         ShardUpsertRequest request,
                                         ShardUpsertRequest.Item item) throws IOException {
        List<GeneratedReferenceInfo> generatedReferencesWithValue = new ArrayList<>();
        BytesReference source;
        if (request.isRawSourceInsert()) {
            assert item.insertValues().length > 0 : "empty insert values array";
            source = new BytesArray((BytesRef) item.insertValues()[0]);
        } else {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            for (int i = 0; i < item.insertValues().length; i++) {
                Reference ref = request.insertColumns()[i];
                if (ref.info().granularity() == RowGranularity.DOC) {
                    // don't include values for partitions in the _source
                    // ideally columns with partition granularity shouldn't be part of the request
                    builder.field(ref.ident().columnIdent().fqn(), item.insertValues()[i]);
                    if (ref.info() instanceof GeneratedReferenceInfo) {
                        generatedReferencesWithValue.add((GeneratedReferenceInfo) ref.info());
                    }
                }
            }
            source = builder.bytes();
        }

        int generatedColumnSize = 0;
        for (GeneratedReferenceInfo generatedReferenceInfo : tableInfo.generatedColumns()) {
            if (!tableInfo.partitionedByColumns().contains(generatedReferenceInfo)) {
                generatedColumnSize++;
            }
        }

        int numMissingGeneratedColumns = generatedColumnSize - generatedReferencesWithValue.size();
        if (numMissingGeneratedColumns > 0 ||
            (generatedReferencesWithValue.size() > 0 && request.validateGeneratedColumns())) {
            // we need to evaluate some generated column expressions
            Map<String, Object> sourceMap = processGeneratedColumnsOnInsert(tableInfo, request.insertColumns(), item.insertValues(),
                    request.isRawSourceInsert(), request.validateGeneratedColumns());
            source = XContentFactory.jsonBuilder().map(sourceMap).bytes();
        }

        return source;
    }

    private Engine.IndexingOperation prepareIndexOnPrimary(IndexShard indexShard,
                                                           long version,
                                                           ShardUpsertRequest request,
                                                           ShardUpsertRequest.Item item) {
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, item.source())
                .type(request.type())
                .id(item.id())
                .routing(request.routing());

        if (logger.isTraceEnabled()) {
            logger.trace("[{}] shard operation with opType={} id={} version={}  source={}",
                    indexShard.shardId(), item.opType(), item.id(), version, item.source().toUtf8());
        }
        if (item.opType() == IndexRequest.OpType.INDEX) {
            return indexShard.prepareIndex(sourceToParse, version, item.versionType(),
                    Engine.Operation.Origin.PRIMARY, request.canHaveDuplicates());
        }
        return indexShard.prepareCreate(sourceToParse, version, item.versionType(), Engine.Operation.Origin.PRIMARY,
                request.canHaveDuplicates(), false);
    }

    private Translog.Location shardIndexOperation(ShardUpsertRequest request,
                                                  ShardUpsertRequest.Item item,
                                                  long version,
                                                  IndexShard indexShard) throws Throwable {
        Engine.IndexingOperation operation = prepareIndexOnPrimary(indexShard, version, request, item);
        operation = updateMappingIfRequired(request, item, version, indexShard, operation);
        operation.execute(indexShard);

        // update the version on request so it will happen on the replicas
        item.versionType(item.versionType().versionTypeForReplicationAndRecovery());
        item.version(operation.version());

        assert item.versionType().validateVersionForWrites(item.version());

        return operation.getTranslogLocation();
    }

    private Engine.IndexingOperation updateMappingIfRequired(ShardUpsertRequest request,
                                                             ShardUpsertRequest.Item item,
                                                             long version,
                                                             IndexShard indexShard,
                                                             Engine.IndexingOperation operation) throws Throwable {
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        if (update != null) {
            mappingUpdatedAction.updateMappingOnMasterSynchronously(
                    request.shardId().getIndex(), request.type(), update);

            operation = prepareIndexOnPrimary(indexShard, version, request, item);
            if (operation.parsedDoc().dynamicMappingsUpdate() != null) {
                throw new RetryOnPrimaryException(request.shardId(),
                        "Dynamics mappings are not available on the node that holds the primary yet");
            }
        }
        return operation;
    }

    private void shardIndexOperationOnReplica(ShardUpsertRequest request,
                                              ShardUpsertRequest.Item item,
                                              IndexShard indexShard) {
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, item.source())
                .type(request.type())
                .id(item.id())
                .routing(request.routing());

        try {
            if (item.opType() == IndexRequest.OpType.INDEX) {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{} (R)] Updating document with id {}, source: {}", indexShard.shardId(), item.id(), item.source().toUtf8());
                }
                Engine.Index index = indexShard.prepareIndex(sourceToParse, item.version(), item.versionType(),
                        Engine.Operation.Origin.REPLICA, request.canHaveDuplicates());
                indexShard.index(index);
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{} (R)] Creating document with id {}, source: {}", indexShard.shardId(), item.id(), item.source().toUtf8());
                }
                Engine.Create create = indexShard.prepareCreate(sourceToParse, item.version(), item.versionType(),
                        Engine.Operation.Origin.REPLICA, request.canHaveDuplicates(), false);
                indexShard.create(create);
            }
        } catch (Throwable t) {
            // if its not an ignore replica failure, we need to make sure to bubble up the failure
            // so we will fail the shard
            if (!ignoreReplicaException(t)) {
                throw t;
            }
        }
    }

    private Map<String, Object> processGeneratedColumnsOnInsert(DocTableInfo tableInfo,
                                                                Reference[] insertColumns,
                                                                Object[] insertValues,
                                                                boolean isRawSourceInsert,
                                                                boolean validateExpressionValue) {
        Map<String, Object> sourceAsMap = buildMapFromSource(insertColumns, insertValues, isRawSourceInsert);
        processGeneratedColumns(tableInfo, sourceAsMap, sourceAsMap, validateExpressionValue);
        return sourceAsMap;
    }

    @VisibleForTesting
    Map<String, Object> buildMapFromSource(Reference[] insertColumns,
                                           Object[] insertValues,
                                           boolean isRawSourceInsert) {
        Map<String, Object> sourceAsMap;
        if (isRawSourceInsert) {
            BytesRef source = (BytesRef) insertValues[0];
            sourceAsMap = XContentHelper.convertToMap(new BytesArray(source), true).v2();
        } else {
            sourceAsMap = new LinkedHashMap<>(insertColumns.length);
            for (int i = 0; i < insertColumns.length; i++) {
                sourceAsMap.put(insertColumns[i].ident().columnIdent().fqn(), insertValues[i]);
            }
        }
        return sourceAsMap;
    }

    @VisibleForTesting
    void processGeneratedColumns(final DocTableInfo tableInfo,
                                 Map<String, Object> updatedColumns,
                                 Map<String, Object> updatedGeneratedColumns,
                                 boolean validateExpressionValue) {
        processGeneratedColumns(tableInfo, updatedColumns, updatedGeneratedColumns, validateExpressionValue, null);
    }

    private void processGeneratedColumns(final DocTableInfo tableInfo,
                                         Map<String, Object> updatedColumns,
                                         Map<String, Object> updatedGeneratedColumns,
                                         boolean validateExpressionValue,
                                         @Nullable GetResult getResult) {
        SymbolToFieldExtractorContext ctx = new SymbolToFieldExtractorContext(functions, updatedColumns);

        for (GeneratedReferenceInfo referenceInfo : tableInfo.generatedColumns()) {
            // partitionedBy columns cannot be updated
            if (!tableInfo.partitionedByColumns().contains(referenceInfo)) {
                Object givenValue = updatedGeneratedColumns.get(referenceInfo.ident().columnIdent().fqn());
                if ((givenValue != null && validateExpressionValue)
                    ||
                    generatedExpressionEvaluationNeeded(referenceInfo.referencedReferenceInfos(), updatedColumns.keySet())) {
                    // at least one referenced column was updated, need to evaluate expression and update column
                    FieldExtractor<GetResult> extractor = SYMBOL_TO_FIELD_EXTRACTOR.convert(referenceInfo.generatedExpression(), ctx);
                    Object value = extractor.extract(getResult);
                    if (givenValue == null) {
                        // add column & value
                        updatedColumns.put(referenceInfo.ident().columnIdent().fqn(), value);
                    } else if (validateExpressionValue && !givenValue.equals(value)) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                "Given value %s for generated column does not match defined generated expression value %s",
                                givenValue, value));
                    }
                }
            }
        }

    }

    private boolean generatedExpressionEvaluationNeeded(List<ReferenceInfo> referencedReferenceInfos,
                                                        Collection<String> updatedColumns) {
        for (ReferenceInfo referenceInfo : referencedReferenceInfos) {
            for (String columnName : updatedColumns) {
                if (referenceInfo.ident().columnIdent().fqn().equals(columnName)
                    || referenceInfo.ident().columnIdent().isChildOf(ColumnIdent.fromPath(columnName))) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Overwrite given values on the source. If the value is a map,
     * it will not be merged but overwritten. The keys of the changes map representing a path of
     * the source map tree.
     * If the path doesn't exists, a new tree will be inserted.
     * <p/>
     * TODO: detect NOOP
     */
    @SuppressWarnings("unchecked")
    static void updateSourceByPaths(@Nonnull Map<String, Object> source, @Nonnull Map<String, Object> changes) {
        for (Map.Entry<String, Object> changesEntry : changes.entrySet()) {
            String key = changesEntry.getKey();
            int dotIndex = key.indexOf(".");
            if (dotIndex > -1) {
                // sub-path detected, dive recursive to the wanted tree element
                String currentKey = key.substring(0, dotIndex);
                if (!source.containsKey(currentKey)) {
                    // insert parent tree element
                    source.put(currentKey, new HashMap<String, Object>());
                }
                Map<String, Object> subChanges = new HashMap<>();
                subChanges.put(key.substring(dotIndex + 1, key.length()), changesEntry.getValue());

                Map<String, Object> innerSource = (Map<String, Object>) source.get(currentKey);
                if (innerSource == null) {
                    throw new NullPointerException(String.format(Locale.ENGLISH,
                            "Object %s is null, cannot write %s onto it", currentKey, subChanges));
                }
                updateSourceByPaths(innerSource, subChanges);
            } else {
                // overwrite or insert the field
                source.put(key, changesEntry.getValue());
            }
        }
    }

    static class SymbolToFieldExtractorContext extends SymbolToFieldExtractor.Context {

        private final Object[] insertValues;
        private final Map<String, Object> updatedColumnValues;

        private SymbolToFieldExtractorContext(Functions functions,
                                              int size,
                                              @Nullable Object[] insertValues,
                                              @Nullable Map<String, Object> updatedColumnValues) {
            super(functions, size);
            this.insertValues = insertValues;
            this.updatedColumnValues = updatedColumnValues;

        }

        public SymbolToFieldExtractorContext(Functions functions, Object[] insertValues) {
            this(functions, insertValues != null ? insertValues.length : 0, insertValues, null);
        }

        public SymbolToFieldExtractorContext(Functions functions, Map<String, Object> updatedColumnValues) {
            this(functions, updatedColumnValues.size(), null, updatedColumnValues);
        }

        @Override
        public Object inputValueFor(InputColumn inputColumn) {
            assert insertValues != null : "insertValues must not be null";
            return insertValues[inputColumn.index()];
        }

        @Nullable
        @Override
        public Object referenceValue(Reference reference) {
            if (updatedColumnValues == null) {
                return super.referenceValue(reference);
            }

            Object value = updatedColumnValues.get(reference.ident().columnIdent().fqn());
            if (value == null && !reference.ident().isColumn()) {
                value = XContentMapValues.extractValue(reference.ident().columnIdent().fqn(), updatedColumnValues);
            }
            return reference.valueType().value(value);
        }
    }

    static class GetResultFieldExtractorFactory implements FieldExtractorFactory<GetResult, SymbolToFieldExtractor.Context> {
        @Override
        public FieldExtractor<GetResult> build(final Reference reference, SymbolToFieldExtractor.Context context) {
            return new FieldExtractor<GetResult>() {
                @Override
                public Object extract(GetResult getResult) {
                    if (getResult == null) {
                        return null;
                    }
                    return reference.valueType().value(XContentMapValues.extractValue(
                            reference.info().ident().columnIdent().fqn(), getResult.sourceAsMap()));
                }
            };
        }
    }

    static class SourceAndVersion {

        final BytesReference source;
        final long version;

        public SourceAndVersion(BytesReference source, long version) {
            this.source = source;
            this.version = version;
        }
    }
}
