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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.analyze.where.DocKeys;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.executor.JobTask;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

public class ESGetTask extends JobTask {

    private final static SymbolToFieldExtractor<GetResponse> SYMBOL_TO_FIELD_EXTRACTOR =
            new SymbolToFieldExtractor<>(new GetResponseFieldExtractorFactory());
    private final List<ListenableFuture<TaskResult>> results;
    private final TransportAction transportAction;
    private final ActionRequest request;
    private final ActionListener listener;

    public ESGetTask(UUID jobId,
                     Functions functions,
                     ProjectionToProjectorVisitor projectionToProjectorVisitor,
                     TransportMultiGetAction multiGetAction,
                     TransportGetAction getAction,
                     ESGetNode node) {
        super(jobId);

        assert multiGetAction != null;
        assert getAction != null;
        assert node != null;
        assert node.docKeys().size() > 0;
        assert node.limit() == null || node.limit() != 0 : "shouldn't execute ESGetTask if limit is 0";


        final GetResponseContext ctx = new GetResponseContext(functions, node);
        List<FieldExtractor<GetResponse>> extractors = new ArrayList<>(node.outputs().size());
        for (Symbol symbol : node.outputs()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, ctx));
        }
        for (Symbol symbol : node.sortSymbols()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, ctx));
        }

        final FetchSourceContext fsc = new FetchSourceContext(ctx.referenceNames());
        final SettableFuture<TaskResult> result = SettableFuture.create();
        results = Arrays.<ListenableFuture<TaskResult>>asList(result);
        if (node.docKeys().size() > 1) {
            MultiGetRequest multiGetRequest = prepareMultiGetRequest(node, fsc);
            transportAction = multiGetAction;
            request = multiGetRequest;
            FlatProjectorChain projectorChain = getFlatProjectorChain(projectionToProjectorVisitor, node);
            listener = new MultiGetResponseListener(result, extractors, projectorChain);
        } else {
            GetRequest getRequest = prepareGetRequest(node, fsc);
            transportAction = getAction;
            request = getRequest;
            listener = new GetResponseListener(result, extractors);
        }
    }

    public static String indexName(TableInfo tableInfo, Optional<List<BytesRef>> values){
        if (tableInfo.isPartitioned()){
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

    private FlatProjectorChain getFlatProjectorChain(ProjectionToProjectorVisitor projectionToProjectorVisitor,
                                                     ESGetNode node) {
        FlatProjectorChain projectorChain = null;
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
            projectorChain = new FlatProjectorChain(
                    Arrays.<Projection>asList(topNProjection),
                    projectionToProjectorVisitor,
                    null
            );
        }
        return projectorChain;
    }

    private static List<Symbol> genInputColumns(int size) {
        List<Symbol> inputColumns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            inputColumns.add(new InputColumn(i));
        }
        return inputColumns;
    }

    static class MultiGetResponseListener implements ActionListener<MultiGetResponse>, RowUpstream {

        private final SettableFuture<TaskResult> result;
        private final List<FieldExtractor<GetResponse>> fieldExtractors;
        @Nullable
        private final FlatProjectorChain projectorChain;
        private final RowDownstreamHandle downstream;


        public MultiGetResponseListener(final SettableFuture<TaskResult> result,
                                        List<FieldExtractor<GetResponse>> extractors,
                                        @Nullable FlatProjectorChain projectorChain) {
            this.result = result;
            this.fieldExtractors = extractors;
            this.projectorChain = projectorChain;
            if (projectorChain == null) {
                downstream = null;
            } else {
                downstream = projectorChain.firstProjector().registerUpstream(this);
                Futures.addCallback(projectorChain.resultProvider().result(), new FutureCallback<Bucket>() {
                    @Override
                    public void onSuccess(@Nullable Bucket rows) {
                        result.set(new QueryResult(rows));
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        result.setException(t);
                    }
                });
            }
        }

        @Override
        public void onResponse(MultiGetResponse responses) {
            if (projectorChain == null) {
                List<Object[]> rows = new ArrayList<>(responses.getResponses().length);
                for (MultiGetItemResponse response : responses) {
                    if (response.isFailed() || !response.getResponse().isExists()) {
                        continue;
                    }
                    final Object[] row = new Object[fieldExtractors.size()];
                    int c = 0;
                    for (FieldExtractor<GetResponse> extractor : fieldExtractors) {
                        row[c] = extractor.extract(response.getResponse());
                        c++;
                    }
                    rows.add(row);
                }
                // NOTICE: this can be optimized by using a special Bucket
                result.set(new QueryResult(new ArrayBucket(rows.toArray(new Object[rows.size()][]))));
            } else {
                FieldExtractorRow<GetResponse> row = new FieldExtractorRow<>(fieldExtractors);
                projectorChain.startProjections();
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
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    static class GetResponseListener implements ActionListener<GetResponse> {

        private final SettableFuture<TaskResult> result;
        private final List<FieldExtractor<GetResponse>> extractors;

        public GetResponseListener(SettableFuture<TaskResult> result, List<FieldExtractor<GetResponse>> extractors) {
            this.result = result;
            this.extractors = extractors;
        }

        @Override
        public void onResponse(GetResponse response) {
            if (!response.isExists()) {
                result.set(TaskResult.EMPTY_RESULT);
                return;
            }
            final Object[][] rows = new Object[1][extractors.size()];
            int c = 0;
            for (FieldExtractor<GetResponse> extractor : extractors) {
                /**
                 * NOTE: mapping isn't applied. So if an Insert was done using the ES Rest Endpoint
                 * the data might be returned in the wrong format (date as string instead of long)
                 */
                rows[0][c] = extractor.extract(response);
                c++;
            }
            result.set(new QueryResult(rows));
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void start() {
        transportAction.execute(request, listener);
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
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
