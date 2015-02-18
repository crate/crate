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


import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.crate.executor.transport.task.elasticsearch.FieldExtractor;
import io.crate.executor.transport.task.elasticsearch.FieldExtractorFactory;
import io.crate.executor.transport.task.elasticsearch.SymbolToFieldExtractor;
import io.crate.metadata.Functions;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SymbolBasedTransportShardUpsertAction extends TransportShardReplicationOperationAction<SymbolBasedShardUpsertRequest, SymbolBasedShardUpsertRequest, ShardUpsertResponse> {

    private final static String ACTION_NAME = "indices:crate/data/write/upsert_symbol_based";
    private final static SymbolToFieldExtractor SYMBOL_TO_FIELD_EXTRACTOR = new SymbolToFieldExtractor(new GetResultFieldExtractorFactory());

    private final TransportIndexAction indexAction;
    private final IndicesService indicesService;
    private final Functions functions;

    @Inject
    public SymbolBasedTransportShardUpsertAction(Settings settings,
                                                 ThreadPool threadPool,
                                                 ClusterService clusterService,
                                                 TransportService transportService,
                                                 ActionFilters actionFilters,
                                                 TransportIndexAction indexAction,
                                                 IndicesService indicesService,
                                                 ShardStateAction shardStateAction,
                                                 Functions functions) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters);
        this.indexAction = indexAction;
        this.indicesService = indicesService;
        this.functions = functions;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.BULK;
    }

    @Override
    protected SymbolBasedShardUpsertRequest newRequestInstance() {
        return new SymbolBasedShardUpsertRequest();
    }

    @Override
    protected SymbolBasedShardUpsertRequest newReplicaRequestInstance() {
        return new SymbolBasedShardUpsertRequest();
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
    protected PrimaryResponse<ShardUpsertResponse, SymbolBasedShardUpsertRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        ShardUpsertResponse shardUpsertResponse = new ShardUpsertResponse(shardRequest.shardId.getIndex());
        SymbolBasedShardUpsertRequest request = shardRequest.request;
        for (int i = 0; i < request.locations().size(); i++) {
            int location = request.locations().get(i);
            SymbolBasedShardUpsertRequest.Item item = request.items().get(i);
            try {
                IndexResponse indexResponse = indexItem(
                        request,
                        item, shardRequest.shardId,
                        item.insertValues() != null, // try insert first
                        0);
                shardUpsertResponse.add(location,
                        new ShardUpsertResponse.Response(
                                item.id(),
                                indexResponse.getVersion(),
                                indexResponse.isCreated()));
            } catch (Throwable t) {
                if (TransportActions.isShardNotAvailableException(t) || !request.continueOnError()) {
                    throw t;
                } else {
                    logger.debug("{} failed to execute update for [{}]/[{}]",
                            t, request.shardId(), request.type(), item.id());
                    shardUpsertResponse.add(location,
                            new ShardUpsertResponse.Failure(
                                    item.id(),
                                    ExceptionsHelper.detailedMessage(t),
                                    (t instanceof VersionConflictEngineException)));
                }
            }
        }
        return new PrimaryResponse<>(shardRequest.request, shardUpsertResponse, null);
    }


    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {

    }

    public IndexResponse indexItem(SymbolBasedShardUpsertRequest request,
                          SymbolBasedShardUpsertRequest.Item item,
                          ShardId shardId,
                          boolean tryInsertFirst,
                          int retryCount) throws ElasticsearchException {

        try {
            IndexRequest indexRequest;
            if (tryInsertFirst) {
                // try insert first without fetching the document
                try {
                    indexRequest = new IndexRequest(prepareInsert(request, item), request);
                } catch (IOException e) {
                    throw ExceptionsHelper.convertToElastic(e);
                }
            } else {
                indexRequest = new IndexRequest(prepareUpdate(request, item, shardId), request);
            }
            return indexAction.execute(indexRequest).actionGet();
        } catch (Throwable t) {
            if (t instanceof VersionConflictEngineException
                    && retryCount < item.retryOnConflict()) {
                return indexItem(request, item, shardId, false, retryCount + 1);
            } else if (tryInsertFirst && item.updateAssignments() != null
                    && t instanceof DocumentAlreadyExistsException) {
                // insert failed, document already exists, try update
                return indexItem(request, item, shardId, false, 0);
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
    public IndexRequest prepareUpdate(SymbolBasedShardUpsertRequest request, SymbolBasedShardUpsertRequest.Item item, ShardId shardId) throws ElasticsearchException {
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

        final SymbolToFieldExtractorContext ctx = new SymbolToFieldExtractorContext(functions, item.updateAssignments().length);
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

    private IndexRequest prepareInsert(SymbolBasedShardUpsertRequest request, SymbolBasedShardUpsertRequest.Item item) throws IOException {
        BytesRef rawSource = null;
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        for (int i = 0; i < item.insertValues().length; i++) {
            Reference ref = request.insertColumns()[i];
            if (ref.info().ident().columnIdent().equals(DocSysColumns.RAW)) {
                rawSource = (BytesRef)item.insertValues()[i];
                break;
            }
            builder.field(ref.ident().columnIdent().fqn(), item.insertValues()[i]);
        }
        IndexRequest indexRequest = Requests.indexRequest(request.index()).type(request.type()).id(item.id()).routing(item.routing())
                .create(!request.overwriteDuplicates()).operationThreaded(false);
        if (rawSource != null) {
            indexRequest.source(rawSource.bytes);
        } else {
            indexRequest.source(builder.bytes(), false);
        }
        return indexRequest;
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

    static class SymbolToFieldExtractorContext extends SymbolToFieldExtractor.Context {

        public SymbolToFieldExtractorContext(Functions functions, int size) {
            super(functions, size);
        }

        @Override
        public Object inputValueFor(InputColumn inputColumn) {
            throw new UnsupportedOperationException("SymbolToFieldExtractorContext does not support resolving InputColumn");
        }
    }



    static class GetResultFieldExtractorFactory implements FieldExtractorFactory<GetResult, SymbolToFieldExtractor.Context> {
        @Override
        public FieldExtractor<GetResult> build(final Reference reference, SymbolToFieldExtractor.Context context) {
            return new FieldExtractor<GetResult>() {
                @Override
                public Object extract(GetResult getResult) {
                    return XContentMapValues.extractValue(
                            reference.info().ident().columnIdent().fqn(), getResult.sourceAsMap());
                }
            };
        }
    }

}
