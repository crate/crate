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
import com.google.common.collect.ImmutableList;
import io.crate.executor.transport.task.ShardUpdateResponse;
import io.crate.executor.transport.task.elasticsearch.FieldExtractor;
import io.crate.executor.transport.task.elasticsearch.FieldExtractorFactory;
import io.crate.executor.transport.task.elasticsearch.SymbolToFieldExtractor;
import io.crate.metadata.Functions;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.instance.TransportInstanceSingleOperationAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.VersionType;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportShardUpdateAction extends TransportInstanceSingleOperationAction<ShardUpdateRequest, ShardUpdateResponse> {

    private final static String ACTION_NAME = "indices:crate/data/write/update";
    private final static SymbolToFieldExtractor SYMBOL_TO_FIELD_EXTRACTOR = new SymbolToFieldExtractor(new GetResultFieldExtractorFactory());

    private final TransportIndexAction indexAction;
    private final TransportUpdateAction updateAction;
    private final IndicesService indicesService;
    private final Functions functions;

    @Inject
    public TransportShardUpdateAction(Settings settings,
                                      ThreadPool threadPool,
                                      ClusterService clusterService,
                                      TransportService transportService,
                                      ActionFilters actionFilters,
                                      TransportIndexAction indexAction,
                                      TransportUpdateAction updateAction,
                                      IndicesService indicesService,
                                      Functions functions) {
        super(settings, ACTION_NAME, threadPool, clusterService, transportService, actionFilters);
        this.indexAction = indexAction;
        this.updateAction = updateAction;
        this.indicesService = indicesService;
        this.functions = functions;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected ShardUpdateRequest newRequest() {
        return new ShardUpdateRequest();
    }

    @Override
    protected ShardUpdateResponse newResponse() {
        return new ShardUpdateResponse();
    }

    @Override
    protected boolean retryOnFailure(Throwable e) {
        return TransportActions.isShardNotAvailableException(e);
    }

    @Override
    protected boolean resolveRequest(ClusterState state, InternalRequest request, ActionListener<ShardUpdateResponse> listener) {
        request.request().routing((state.metaData().resolveIndexRouting(request.request().routing(), request.request().index())));
        // Fail fast on the node that received the request, rather than failing when translating on the index or delete request.
        if (request.request().routing() == null && state.getMetaData().routingRequired(request.concreteIndex(), request.request().type())) {
            throw new RoutingMissingException(request.concreteIndex(), request.request().type(), request.request().id());
        }
        return true;
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) throws ElasticsearchException {
        if (request.request().shardId() != -1) {
            return clusterState.routingTable().index(request.concreteIndex()).shard(request.request().shardId()).primaryShardIt();
        }
        ShardIterator shardIterator = clusterService.operationRouting()
                .indexShards(clusterState, request.concreteIndex(), request.request().type(), request.request().id(), request.request().routing());
        ShardRouting shard;
        while ((shard = shardIterator.nextOrNull()) != null) {
            if (shard.primary()) {
                return new PlainShardIterator(shardIterator.shardId(), ImmutableList.of(shard));
            }
        }
        return new PlainShardIterator(shardIterator.shardId(), ImmutableList.<ShardRouting>of());
    }

    @Override
    protected void shardOperation(InternalRequest request, ActionListener<ShardUpdateResponse> listener) throws ElasticsearchException {
        shardOperation(request, listener, 0);
    }

    protected void shardOperation(final InternalRequest request, final ActionListener<ShardUpdateResponse> listener, final int retryCount) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.concreteIndex());
        IndexShard indexShard = indexService.shardSafe(request.request().shardId());

        IndexRequest indexRequest = new IndexRequest(prepare(request.request(), indexShard), request.request());
        indexAction.execute(indexRequest, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse response) {
                ShardUpdateResponse update = new ShardUpdateResponse(response.getIndex(), response.getId(), response.getVersion(), response.isCreated());
                listener.onResponse(update);
            }

            @Override
            public void onFailure(Throwable e) {
                e = ExceptionsHelper.unwrapCause(e);
                if (e instanceof VersionConflictEngineException) {
                    if (retryCount < request.request().retryOnConflict()) {
                        try {
                            threadPool.executor(executor()).execute(new ActionRunnable<ShardUpdateResponse>(listener) {
                                @Override
                                protected void doRun() {
                                    shardOperation(request, listener, retryCount + 1);
                                }
                            });
                        } catch (EsRejectedExecutionException ex) {
                            logger.debug("Can not run threaded action, execution rejected for listener [{}] running on current thread", listener);
                            listener.onFailure(e);
                        }
                        return;
                    }
                }
                listener.onFailure(e);
            }
        });
    }

    /**
     * Prepares an update request by converting it into an index request.
     *
     * TODO: detect a NOOP and return an update response if true
     */
    @SuppressWarnings("unchecked")
    public IndexRequest prepare(ShardUpdateRequest request, IndexShard indexShard) {
        final SymbolToFieldExtractor.Context ctx = new SymbolToFieldExtractor.Context(functions, request.assignments().size());
        Map<String, FieldExtractor> extractors = new HashMap<>(request.assignments().size());
        for (Map.Entry<String, Symbol> entry : request.assignments().entrySet()) {
            extractors.put(entry.getKey(), SYMBOL_TO_FIELD_EXTRACTOR.convert(entry.getValue(), ctx));
        }

        final GetResult getResult = indexShard.getService().get(request.type(), request.id(),
                new String[]{RoutingFieldMapper.NAME, ParentFieldMapper.NAME, TTLFieldMapper.NAME},
                true, request.version(), VersionType.INTERNAL, new FetchSourceContext(ctx.referenceNames()), false);

        if (!getResult.isExists()) {
            throw new DocumentMissingException(new ShardId(indexShard.indexService().index().name(), request.shardId()), request.type(), request.id());
        }

        if (getResult.internalSourceRef() == null) {
            // no source, we can't do nothing, through a failure...
            throw new DocumentSourceMissingException(new ShardId(indexShard.indexService().index().name(), request.shardId()), request.type(), request.id());
        }

        Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final Map<String, Object> updatedSourceAsMap;
        final XContentType updateSourceContentType = sourceAndContent.v1();
        String routing = getResult.getFields().containsKey(RoutingFieldMapper.NAME) ? getResult.field(RoutingFieldMapper.NAME).getValue().toString() : null;
        String parent = getResult.getFields().containsKey(ParentFieldMapper.NAME) ? getResult.field(ParentFieldMapper.NAME).getValue().toString() : null;

        updatedSourceAsMap = sourceAndContent.v2();

        Map<String, Object> pathsToUpdate = new HashMap<>(extractors.size());
        for (Map.Entry<String, FieldExtractor> entry : extractors.entrySet()) {
            /**
             * NOTE: mapping isn't applied. So if an Insert was done using the ES Rest Endpoint
             * the data might be returned in the wrong format (date as string instead of long)
             */
            pathsToUpdate.put(entry.getKey(), entry.getValue().extract(getResult));
        }

        updateSourceByPaths(updatedSourceAsMap, pathsToUpdate);

        final IndexRequest indexRequest = Requests.indexRequest(request.index()).type(request.type()).id(request.id()).routing(routing).parent(parent)
                .source(updatedSourceAsMap, updateSourceContentType)
                .version(getResult.getVersion());
        indexRequest.operationThreaded(false);
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


    static class GetResultFieldExtractorFactory implements FieldExtractorFactory<GetResult, SymbolToFieldExtractor.Context> {
        @Override
        public FieldExtractor<GetResult> build(final Reference reference, SymbolToFieldExtractor.Context context) {
            return new FieldExtractor<GetResult>() {
                @Override
                public Object extract(GetResult getResult) {
                    assert getResult.sourceAsMap() != null;
                    return XContentMapValues.extractValue(
                            reference.info().ident().columnIdent().fqn(), getResult.sourceAsMap());
                }
            };
        }
    }

}
