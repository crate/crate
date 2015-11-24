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
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolType;
import io.crate.executor.transport.kill.KillableCallable;
import io.crate.executor.transport.task.elasticsearch.FieldExtractor;
import io.crate.executor.transport.task.elasticsearch.FieldExtractorFactory;
import io.crate.executor.transport.task.elasticsearch.SymbolToFieldExtractor;
import io.crate.jobs.JobContextService;
import io.crate.jobs.KillAllListener;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.Input;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.operation.plain.Preference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class TransportShardUpsertAction
        extends TransportShardReplicationOperationAction<ShardUpsertRequest, ShardUpsertRequest, ShardUpsertResponse>
        implements KillAllListener {

    private final static String ACTION_NAME = "indices:crate/data/write/upsert_symbol_based";
    private final static SymbolToFieldExtractor SYMBOL_TO_FIELD_EXTRACTOR = new SymbolToFieldExtractor(new GetResultFieldExtractorFactory());
    private final static ReferenceToLiteralConverter TO_LITERAL_CONVERTER = new ReferenceToLiteralConverter();
    private final static Function<ReferenceInfo, Reference> REFERENCE_INFO_TO_REFERENCE = new Function<ReferenceInfo, Reference>() {
        @Nullable
        @Override
        public Reference apply(@Nullable ReferenceInfo input) {
            if (input == null) {
                return null;
            }
            return new Reference(input);
        }
    };

    private final TransportIndexAction indexAction;
    private final IndicesService indicesService;
    private final Functions functions;
    private final Schemas schemas;
    private final Multimap<UUID, KillableCallable> activeOperations = Multimaps.synchronizedMultimap(HashMultimap.<UUID, KillableCallable>create());

    @Inject
    public TransportShardUpsertAction(Settings settings,
                                      ThreadPool threadPool,
                                      ClusterService clusterService,
                                      TransportService transportService,
                                      ActionFilters actionFilters,
                                      JobContextService jobContextService,
                                      TransportIndexAction indexAction,
                                      IndicesService indicesService,
                                      ShardStateAction shardStateAction,
                                      Functions functions,
                                      Schemas schemas) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters);
        this.indexAction = indexAction;
        this.indicesService = indicesService;
        this.functions = functions;
        this.schemas = schemas;
        jobContextService.addListener(this);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.BULK;
    }

    @Override
    protected ShardUpsertRequest newRequestInstance() {
        return new ShardUpsertRequest();
    }

    @Override
    protected ShardUpsertRequest newReplicaRequestInstance() {
        return new ShardUpsertRequest();
    }

    @Override
    protected ShardUpsertResponse newResponseInstance() {
        return new ShardUpsertResponse();
    }

    @Override
    protected boolean resolveIndex() {
        return true;
    }

    @Override
    protected boolean checkWriteConsistency() {
        return false;
    }

    @Override
    protected boolean ignoreReplicas() {
        return true;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting()
                .getShards(state, request.request().index(), request.request().shardId(), Preference.PRIMARY.type());
    }

    @Override
    protected Tuple<ShardUpsertResponse, ShardUpsertRequest> shardOperationOnPrimary(ClusterState clusterState, final PrimaryOperationRequest shardRequest) {
        KillableCallable<Tuple> callable = new KillableCallable<Tuple>() {

            private AtomicBoolean killed = new AtomicBoolean(false);

            @Override
            public void kill() {
                killed.getAndSet(true);
            }

            @Override
            public Tuple call() throws Exception {
                ShardUpsertResponse shardUpsertResponse = processRequestItems(shardRequest.shardId, shardRequest.request, killed);
                return new Tuple<>(shardUpsertResponse, shardRequest.request);
            }
        };
        activeOperations.put(shardRequest.request.jobId(), callable);
        Tuple<ShardUpsertResponse, ShardUpsertRequest> response;
        try {
            //noinspection unchecked
            response = callable.call();
        } catch (Throwable e) {
            throw Throwables.propagate(e);
        } finally {
            activeOperations.remove(shardRequest.request.jobId(), callable);
        }
        return response;
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
    }

    protected ShardUpsertResponse processRequestItems(ShardId shardId,
                                                      ShardUpsertRequest request,
                                                      AtomicBoolean killed) {
        ShardUpsertResponse shardUpsertResponse = new ShardUpsertResponse();
        DocTableInfo tableInfo = schemas.getWritableTable(TableIdent.fromIndexName(request.index()));
        for (int i = 0; i < request.itemIndices().size(); i++) {
            int location = request.itemIndices().get(i);
            ShardUpsertRequest.Item item = request.items().get(i);
            if (killed.get()) {
                throw new CancellationException();
            }
            try {
                indexItem(
                        tableInfo,
                        request,
                        item,
                        shardId,
                        item.insertValues() != null, // try insert first
                        0);
                shardUpsertResponse.add(location);
            } catch (Throwable t) {
                if (!TransportActions.isShardNotAvailableException(t) && !request.continueOnError()) {
                    throw t;
                } else {
                    logger.debug("{} failed to execute upsert for [{}]/[{}]",
                            t, request.shardId(), request.type(), item.id());
                    shardUpsertResponse.add(location,
                            new ShardUpsertResponse.Failure(
                                    item.id(),
                                    ExceptionsHelper.detailedMessage(t),
                                    (t instanceof VersionConflictEngineException)));
                }
            }
        }

        return shardUpsertResponse;
    }

    protected IndexResponse indexItem(DocTableInfo tableInfo,
                                      ShardUpsertRequest request,
                                      ShardUpsertRequest.Item item,
                                      ShardId shardId,
                                      boolean tryInsertFirst,
                                      int retryCount) throws ElasticsearchException {

        try {
            IndexRequest indexRequest;
            if (tryInsertFirst) {
                // try insert first without fetching the document
                try {
                    indexRequest = new IndexRequest(prepareInsert(tableInfo, request, item), request);
                } catch (IOException e) {
                    throw ExceptionsHelper.convertToElastic(e);
                }
            } else {
                indexRequest = new IndexRequest(prepareUpdate(tableInfo, request, item, shardId), request);
            }
            return indexAction.execute(indexRequest).actionGet();
        } catch (Throwable t) {
            if (t instanceof VersionConflictEngineException
                    && retryCount < item.retryOnConflict()) {
                return indexItem(tableInfo, request, item, shardId, false, retryCount + 1);
            } else if (tryInsertFirst && item.updateAssignments() != null
                    && t instanceof DocumentAlreadyExistsException) {
                // insert failed, document already exists, try update
                return indexItem(tableInfo, request, item, shardId, false, 0);
            } else {
                throw t;
            }
        }
    }



    /**
     * Prepares an update request by converting it into an index request.
     *
     * TODO: detect a NOOP and return an update response if true
     */
    @SuppressWarnings("unchecked")
    public IndexRequest prepareUpdate(DocTableInfo tableInfo,
                                      ShardUpsertRequest request,
                                      ShardUpsertRequest.Item item,
                                      ShardId shardId) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.shardSafe(shardId.id());
        final GetResult getResult = indexShard.getService().get(request.type(), item.id(),
                new String[]{RoutingFieldMapper.NAME, ParentFieldMapper.NAME, TTLFieldMapper.NAME},
                true, item.version(), VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE, false);

        if (!getResult.isExists()) {
            throw new DocumentMissingException(new ShardId(request.index(), request.shardId()), request.type(), item.id());
        }

        if (getResult.internalSourceRef() == null) {
            // no source, we can't do nothing, through a failure...
            throw new DocumentSourceMissingException(new ShardId(request.index(), request.shardId()), request.type(), item.id());
        }

        Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final Map<String, Object> updatedSourceAsMap;
        final XContentType updateSourceContentType = sourceAndContent.v1();
        String routing = getResult.getFields().containsKey(RoutingFieldMapper.NAME) ? getResult.field(RoutingFieldMapper.NAME).getValue().toString() : null;
        String parent = getResult.getFields().containsKey(ParentFieldMapper.NAME) ? getResult.field(ParentFieldMapper.NAME).getValue().toString() : null;

        updatedSourceAsMap = sourceAndContent.v2();

        final SymbolToFieldExtractorContext ctx = new SymbolToFieldExtractorContext(
                functions, item.updateAssignments().length, item.insertValues());
        Map<String, FieldExtractor> extractors = new HashMap<>(item.updateAssignments().length);
        for (int i = 0; i < request.updateColumns().length; i++) {
            extractors.put(request.updateColumns()[i], SYMBOL_TO_FIELD_EXTRACTOR.convert(item.updateAssignments()[i], ctx));
        }

        Map<String, Object> pathsToUpdate = new HashMap<>(extractors.size());
        for (Map.Entry<String, FieldExtractor> entry : extractors.entrySet()) {
            /**
             * NOTE: mapping isn't applied. So if an Insert was done using the ES Rest Endpoint
             * the data might be returned in the wrong format (date as string instead of long)
             */
            pathsToUpdate.put(entry.getKey(), entry.getValue().extract(getResult));
        }

        processGeneratedColumns(tableInfo, pathsToUpdate);

        updateSourceByPaths(updatedSourceAsMap, pathsToUpdate);

        final IndexRequest indexRequest = Requests.indexRequest(request.index())
                .type(request.type())
                .id(item.id())
                .routing(routing)
                .parent(parent)
                .source(updatedSourceAsMap, updateSourceContentType)
                .version(getResult.getVersion());
        indexRequest.operationThreaded(false);
        return indexRequest;
    }

    private IndexRequest prepareInsert(DocTableInfo tableInfo, ShardUpsertRequest request, ShardUpsertRequest.Item item) throws IOException {
        List<GeneratedReferenceInfo> generatedReferencesWithValue = new ArrayList<>();
        BytesReference source;
        if (request.isRawSourceInsert()) {
            assert item.insertValues().length > 0 : "empty insert values array";
            source = new BytesArray((BytesRef) item.insertValues()[0]);
        } else {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            for (int i = 0; i < item.insertValues().length; i++) {
                Reference ref = request.insertColumns()[i];
                builder.field(ref.ident().columnIdent().fqn(), item.insertValues()[i]);
                if (ref.info() instanceof GeneratedReferenceInfo) {
                    generatedReferencesWithValue.add((GeneratedReferenceInfo) ref.info());
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

        if (generatedReferencesWithValue.size() < generatedColumnSize) {
            // we need to evaluate some generated column expressions
            Map<String, Object> sourceMap = processGeneratedColumnsOnInsert(tableInfo, request.insertColumns(), item.insertValues(),
                    request.isRawSourceInsert());
            source = XContentFactory.jsonBuilder().map(sourceMap).bytes();
        }

        IndexRequest indexRequest = Requests.indexRequest(request.index())
                .type(request.type())
                .id(item.id())
                .routing(item.routing())
                .source(source)
                .create(!request.overwriteDuplicates())
                .operationThreaded(false);
        if (logger.isTraceEnabled()) {
            logger.trace("Inserting document with id {}, source: {}", item.id(), indexRequest.source().toUtf8());
        }
        return indexRequest;
    }

    private Map<String, Object> processGeneratedColumnsOnInsert(DocTableInfo tableInfo,
                                                        Reference[] insertColumns,
                                                        Object[] insertValues,
                                                        boolean isRawSourceInsert) {
        Map<String, Object> sourceAsMap = buildMapFromSource(insertColumns, insertValues, isRawSourceInsert);
        processGeneratedColumns(tableInfo, sourceAsMap, false);
        return sourceAsMap;
    }

    @VisibleForTesting
    Map<String, Object> buildMapFromSource(Reference[] insertColumns,
                                           Object[] insertValues,
                                           boolean isRawSourceInsert) {
        Map<String, Object> sourceAsMap;
        if (isRawSourceInsert) {
            BytesRef source = (BytesRef) insertValues[0];
            sourceAsMap = XContentHelper.convertToMap(source.bytes, true).v2();
        } else {
            sourceAsMap = new LinkedHashMap<>(insertColumns.length);
            for (int i = 0; i < insertColumns.length; i++) {
                sourceAsMap.put(insertColumns[i].ident().columnIdent().fqn(), insertValues[i]);
            }
        }
        return sourceAsMap;
    }

    @VisibleForTesting
    void processGeneratedColumns(DocTableInfo tableInfo, Map<String, Object> updatedColumns) {
        processGeneratedColumns(tableInfo, updatedColumns, true);
    }

    @VisibleForTesting
    void processGeneratedColumns(final DocTableInfo tableInfo,
                                         Map<String, Object> updatedColumns,
                                         boolean validateExpressionValue) {
        List<String> updatedColumnNames = Lists.newArrayList(updatedColumns.keySet());
        List<ReferenceInfo> updatedReferenceInfos = Lists.transform(updatedColumnNames, new Function<String, ReferenceInfo>() {
            @Nullable
            @Override
            public ReferenceInfo apply(@Nullable String input) {
                if (input == null) {
                    return null;
                }
                return tableInfo.getReferenceInfo(ColumnIdent.fromPath(input));
            }
        });
        List<Reference> updatedReferences = Lists.transform(updatedReferenceInfos, REFERENCE_INFO_TO_REFERENCE);
        ReferenceToLiteralConverter.Context toLiteralContext = new ReferenceToLiteralConverter.Context(
                updatedReferences, updatedColumns.values().toArray());
        ExpressionAnalyzer expressionAnalyzer =
                new ExpressionAnalyzer(functions, null, null, null, null, null);

        for (GeneratedReferenceInfo referenceInfo : tableInfo.generatedColumns()) {
            // partitionedBy columns cannot be updated
            if (!tableInfo.partitionedByColumns().contains(referenceInfo)) {
                if (generatedExpressionEvaluationNeeded(referenceInfo.referencedReferenceInfos(), updatedReferenceInfos)) {
                    // at least one referenced column was updated, need to evaluate expression and update column
                    Symbol valueSymbol = TO_LITERAL_CONVERTER.process(referenceInfo.generatedExpression(), toLiteralContext);
                    valueSymbol = expressionAnalyzer.normalize(valueSymbol);
                    if (valueSymbol.symbolType() == SymbolType.LITERAL) {
                        Object value = ((Input) valueSymbol).value();
                        Object givenValue = updatedColumns.get(referenceInfo.ident().columnIdent().fqn());
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
    }

    private boolean generatedExpressionEvaluationNeeded(List<ReferenceInfo> referencedReferenceInfos,
                                                        List<ReferenceInfo> updatedReferenceInfos) {
        for (ReferenceInfo referenceInfo : referencedReferenceInfos) {
            for (ReferenceInfo updatedReferenceInfo : updatedReferenceInfos) {
                if (referenceInfo.equals(updatedReferenceInfo)
                    || referenceInfo.ident().columnIdent().isChildOf(updatedReferenceInfo.ident().columnIdent())) {
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
     *
     * TODO: detect NOOP
     */
    @SuppressWarnings("unchecked")
    private void updateSourceByPaths(Map<String, Object> source, Map<String, Object> changes) {
        for (Map.Entry<String, Object> changesEntry : changes.entrySet()) {
            if (changesEntry.getKey().contains(".")) {
                // sub-path detected, dive recursive to the wanted tree element
                List<String> path = Splitter.on(".").splitToList(changesEntry.getKey());
                String currentKey = path.get(0);
                if (!source.containsKey(currentKey)) {
                    // insert parent tree element
                    source.put(currentKey, new HashMap<String, Object>());
                }
                Map<String, Object> subChanges = new HashMap<>();
                subChanges.put(Joiner.on(".").join(path.subList(1, path.size())),
                        changesEntry.getValue());
                updateSourceByPaths((Map<String, Object>) source.get(currentKey), subChanges);
            } else {
                // overwrite or insert the field
                source.put(changesEntry.getKey(), changesEntry.getValue());
            }
        }
    }

    @Override
    public void killAllJobs(long timestamp) {
        synchronized (activeOperations) {
            for(KillableCallable callable : activeOperations.values()) {
                callable.kill();
            }
            activeOperations.clear();
        }
    }

    @Override
    public void killJob(UUID jobId) {
        synchronized (activeOperations) {
            Collection<KillableCallable> operations = activeOperations.get(jobId);
            for(KillableCallable callable : operations) {
                callable.kill();
            }
            activeOperations.removeAll(jobId);
        }
    }

    static class SymbolToFieldExtractorContext extends SymbolToFieldExtractor.Context {

        private final Object[] insertValues;

        public SymbolToFieldExtractorContext(Functions functions, int size, Object[] insertValues) {
            super(functions, size);
            this.insertValues = insertValues;
        }

        @Override
        public Object inputValueFor(InputColumn inputColumn) {
            return insertValues[inputColumn.index()];
        }
    }



    static class GetResultFieldExtractorFactory implements FieldExtractorFactory<GetResult, SymbolToFieldExtractor.Context> {
        @Override
        public FieldExtractor<GetResult> build(final Reference reference, SymbolToFieldExtractor.Context context) {
            return new FieldExtractor<GetResult>() {
                @Override
                public Object extract(GetResult getResult) {
                    return reference.valueType().value(XContentMapValues.extractValue(
                            reference.info().ident().columnIdent().fqn(), getResult.sourceAsMap()));
                }
            };
        }
    }

}