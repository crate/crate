/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.analyze.where.DocKeys;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Buckets;
import io.crate.executor.JobTask;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.jobs.ESJobContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.QueryResultRowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.*;
import io.crate.planner.node.dql.ESGetNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.ValueSymbolVisitor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.util.*;

public class ESGetTask extends JobTask {

    private final static SymbolToFieldExtractor<GetResponse> SYMBOL_TO_FIELD_EXTRACTOR =
            new SymbolToFieldExtractor<>(new GetResponseFieldExtractorFactory());

    private final List<? extends ListenableFuture<TaskResult>> results;
    private final ESJobContext context;

    public ESGetTask(UUID jobId,
                     Functions functions,
                     ProjectorFactory projectorFactory,
                     TransportMultiGetAction multiGetAction,
                     TransportGetAction getAction,
                     ESGetNode node,
                     JobContextService jobContextService) {
        super(jobId);

        assert multiGetAction != null;
        assert getAction != null;
        assert node != null;
        assert node.docKeys().size() > 0;
        assert node.limit() == null || node.limit() != 0 : "shouldn't execute ESGetTask if limit is 0";

        int executionNodeId = node.executionNodeId();

        final GetResponseContext ctx = new GetResponseContext(functions, node);
        List<FieldExtractor<GetResponse>> extractors = new ArrayList<>(node.outputs().size());
        for (Symbol symbol : node.outputs()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, ctx));
        }
        for (Symbol symbol : node.sortSymbols()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, ctx));
        }

        final FetchSourceContext fsc = new FetchSourceContext(ctx.referenceNames());


        ActionListener listener;
        ActionRequest request;
        TransportAction transportAction;
        if (node.docKeys().size() > 1) {
            MultiGetRequest multiGetRequest = prepareMultiGetRequest(node, fsc);
            transportAction = multiGetAction;
            request = multiGetRequest;

            SettableFuture<TaskResult> result = SettableFuture.create();
            List<SettableFuture<TaskResult>> settableFutures = Collections.singletonList(result);
            results = settableFutures;
            QueryResultRowDownstream queryResultRowDownstream = new QueryResultRowDownstream(settableFutures);

            FlatProjectorChain projectorChain = getFlatProjectorChain(projectorFactory, node, queryResultRowDownstream);
            listener = new MultiGetResponseListener(extractors, projectorChain);

        } else {
            GetRequest getRequest = prepareGetRequest(node, fsc);
            transportAction = getAction;
            request = getRequest;
            SettableFuture<Bucket> settableFuture = SettableFuture.create();
            listener = new GetResponseListener(settableFuture, extractors);
            results = ImmutableList.of(Futures.transform(settableFuture, QueryResult.TO_TASK_RESULT));
        }

        JobExecutionContext.Builder contextBuilder = jobContextService.newBuilder(jobId());
        context = new ESJobContext("lookup by primary key", ImmutableList.of(request), ImmutableList.of(listener), results, transportAction);
        contextBuilder.addSubContext(executionNodeId, context);
        jobContextService.createContext(contextBuilder);

    }

    public static String indexName(TableInfo tableInfo, Optional<List<BytesRef>> values) {
        if (tableInfo.isPartitioned()) {
            return new PartitionName(tableInfo.ident(), values.get()).stringValue();
        } else {
            return tableInfo.ident().esName();
        }
    }

    private GetRequest prepareGetRequest(ESGetNode node, FetchSourceContext fsc) {
        DocKeys.DocKey docKey = node.docKeys().getOnlyKey();
        GetRequest getRequest = new GetRequest(indexName(node.tableInfo(), docKey.partitionValues()),
                Constants.DEFAULT_MAPPING_TYPE, docKey.id());
        getRequest.fetchSourceContext(fsc);
        getRequest.realtime(true);
        getRequest.routing(docKey.routing());
        return getRequest;
    }

    private MultiGetRequest prepareMultiGetRequest(ESGetNode node, FetchSourceContext fsc) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (DocKeys.DocKey key : node.docKeys()) {
            MultiGetRequest.Item item = new MultiGetRequest.Item(
                    indexName(node.tableInfo(), key.partitionValues()), Constants.DEFAULT_MAPPING_TYPE, key.id());
            item.fetchSourceContext(fsc);
            item.routing(key.routing());
            multiGetRequest.add(item);
        }
        multiGetRequest.realtime(true);
        return multiGetRequest;
    }

    private FlatProjectorChain getFlatProjectorChain(ProjectorFactory projectorFactory,
                                                     ESGetNode node,
                                                     QueryResultRowDownstream queryResultRowDownstream) {
        if (node.limit() != null || node.offset() > 0 || !node.sortSymbols().isEmpty()) {
            List<Symbol> orderBySymbols = new ArrayList<>(node.sortSymbols().size());
            for (Symbol symbol : node.sortSymbols()) {
                int i = node.outputs().indexOf(symbol);
                if (i < 0) {
                    orderBySymbols.add(new InputColumn(node.outputs().size() + orderBySymbols.size()));
                } else {
                    orderBySymbols.add(new InputColumn(i));
                }
            }
            TopNProjection topNProjection = new TopNProjection(
                    MoreObjects.firstNonNull(node.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    node.offset(),
                    orderBySymbols,
                    node.reverseFlags(),
                    node.nullsFirst()
            );
            topNProjection.outputs(genInputColumns(node.outputs().size()));
            return FlatProjectorChain.withAttachedDownstream(
                    projectorFactory,
                    null,
                    ImmutableList.<Projection>of(topNProjection),
                    queryResultRowDownstream,
                    jobId()
            );
        } else {
            return FlatProjectorChain.withProjectors(
                    ImmutableList.<Projector>of(queryResultRowDownstream)
            );
        }
    }

    private static List<Symbol> genInputColumns(int size) {
        List<Symbol> inputColumns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            inputColumns.add(new InputColumn(i));
        }
        return inputColumns;
    }

    static class MultiGetResponseListener implements ActionListener<MultiGetResponse>, RowUpstream {

        private final List<FieldExtractor<GetResponse>> fieldExtractors;
        private final RowDownstreamHandle downstream;
        private final FlatProjectorChain projectorChain;


        public MultiGetResponseListener(List<FieldExtractor<GetResponse>> extractors,
                                        FlatProjectorChain projectorChain) {
            this.projectorChain = projectorChain;
            this.downstream = projectorChain.firstProjector().registerUpstream(this);
            this.fieldExtractors = extractors;
        }


        @Override
        public void onResponse(MultiGetResponse responses) {
            projectorChain.startProjections();
            FieldExtractorRow<GetResponse> row = new FieldExtractorRow<>(fieldExtractors);
            try {
                for (MultiGetItemResponse response : responses) {
                    if (response.isFailed() || !response.getResponse().isExists()) {
                        continue;
                    }
                    row.setCurrent(response.getResponse());
                    if (!downstream.setNextRow(row)) {
                        return;
                    }
                }
                downstream.finish();
            } catch (Exception e) {
                downstream.fail(e);
            }
        }

        @Override
        public void onFailure(Throwable e) {
            downstream.fail(e);
        }
    }

    static class GetResponseListener implements ActionListener<GetResponse> {

        private final Bucket bucket;
        private final FieldExtractorRow<GetResponse> row;
        private final SettableFuture<Bucket> result;

        public GetResponseListener(SettableFuture<Bucket> result, List<FieldExtractor<GetResponse>> extractors) {
            this.result = result;
            row = new FieldExtractorRow<>(extractors);
            bucket = Buckets.of(row);
        }

        @Override
        public void onResponse(GetResponse response) {
            if (!response.isExists()) {
                result.set(Bucket.EMPTY);
                return;
            }
            row.setCurrent(response);
            result.set(bucket);
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    @Override
    public void start() {
        context.start();
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<? extends ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "upstreamResult not supported on %s",
                        getClass().getSimpleName()));
    }

    static class GetResponseContext extends SymbolToFieldExtractor.Context {
        private final HashMap<String, DocKeys.DocKey> ids2Keys;
        private final ESGetNode node;
        private final HashMap<ColumnIdent, Integer> partitionPositions;

        public GetResponseContext(Functions functions, ESGetNode node) {
            super(functions, node.outputs().size());
            this.node = node;
            ids2Keys = new HashMap<>(node.docKeys().size());
            for (DocKeys.DocKey key : node.docKeys()) {
                ids2Keys.put(key.id(), key);
            }

            if (node.tableInfo().isPartitioned()) {
                partitionPositions = new HashMap<>(node.tableInfo().partitionedByColumns().size());
                for (Integer idx : node.docKeys().partitionIdx().get()) {
                    partitionPositions.put(node.tableInfo().primaryKey().get(idx), idx);
                }
            } else {
                partitionPositions = null;
            }
        }

        @Override
        public Object inputValueFor(InputColumn inputColumn) {
            throw new AssertionError("GetResponseContext does not support resolving InputColumn");
        }
    }

    static class GetResponseFieldExtractorFactory implements FieldExtractorFactory<GetResponse, GetResponseContext> {

        @Override
        public FieldExtractor<GetResponse> build(final Reference reference, final GetResponseContext context) {
            final String field = reference.info().ident().columnIdent().fqn();
            if (field.equals("_version")) {
                return new FieldExtractor<GetResponse>() {
                    @Override
                    public Object extract(GetResponse response) {
                        return response.getVersion();
                    }
                };
            } else if (field.equals("_id")) {
                return new FieldExtractor<GetResponse>() {
                    @Override
                    public Object extract(GetResponse response) {
                        return response.getId();
                    }
                };
            } else if (context.node.tableInfo().isPartitioned()
                    && context.node.tableInfo().partitionedBy().contains(reference.ident().columnIdent())) {
                final int pos = context.node.tableInfo().primaryKey().indexOf(reference.ident().columnIdent());
                if (pos >= 0) {
                    return new FieldExtractor<GetResponse>() {
                        @Override
                        public Object extract(GetResponse response) {
                            return ValueSymbolVisitor.VALUE.process(context.ids2Keys.get(response.getId()).values().get(pos));
                        }
                    };
                }
            }
            return new FieldExtractor<GetResponse>() {
                @Override
                public Object extract(GetResponse response) {
                    Map<String, Object> sourceAsMap = response.getSourceAsMap();
                    assert sourceAsMap != null;
                    return reference.valueType().value(XContentMapValues.extractValue(field, sourceAsMap));
                }
            };
        }
    }

}
