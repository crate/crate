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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.executor.JobTask;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ReferenceInfo;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.planner.node.dql.ESGetNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
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

    private final static SymbolToFieldExtractor SYMBOL_TO_FIELD_EXTRACTOR =
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
        assert node.ids().size() > 0;
        assert node.limit() == null || node.limit() != 0 : "shouldn't execute ESGetTask if limit is 0";


        Map<String, Object> partitionValues = preparePartitionValues(node);
        final GetResponseContext ctx = new GetResponseContext(functions, node.outputs().size(), partitionValues);
        List<FieldExtractor> extractors = new ArrayList<>(node.outputs().size());
        for (Symbol symbol : node.outputs()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, ctx));
        }
        for (Symbol symbol : node.sortSymbols()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, ctx));
        }

        final FetchSourceContext fsc = new FetchSourceContext(ctx.referenceNames());
        final SettableFuture<TaskResult> result = SettableFuture.create();
        results = Arrays.<ListenableFuture<TaskResult>>asList(result);
        if (node.ids().size() > 1) {
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

    private Map<String, Object> preparePartitionValues(ESGetNode node) {
        Map<String, Object> partitionValues;
        if (node.partitionBy().isEmpty()) {
            partitionValues = ImmutableMap.of();
        } else {
            PartitionName partitionName = PartitionName.fromStringSafe(node.index());
            int numPartitionColumns = node.partitionBy().size();
            partitionValues = new HashMap<>(numPartitionColumns);
            for (int i = 0; i < node.partitionBy().size(); i++) {
                ReferenceInfo info = node.partitionBy().get(i);
                partitionValues.put(
                        info.ident().columnIdent().fqn(),
                        info.type().value(partitionName.values().get(i))
                );
            }
        }
        return partitionValues;
    }

    private GetRequest prepareGetRequest(ESGetNode node, FetchSourceContext fsc) {
        GetRequest getRequest = new GetRequest(node.index(), Constants.DEFAULT_MAPPING_TYPE, node.ids().get(0));
        getRequest.fetchSourceContext(fsc);
        getRequest.realtime(true);
        getRequest.routing(node.routingValues().get(0));
        return getRequest;
    }

    private MultiGetRequest prepareMultiGetRequest(ESGetNode node, FetchSourceContext fsc) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (int i = 0; i < node.ids().size(); i++) {
            String id = node.ids().get(i);
            MultiGetRequest.Item item = new MultiGetRequest.Item(node.index(), Constants.DEFAULT_MAPPING_TYPE, id);
            item.fetchSourceContext(fsc);
            item.routing(node.routingValues().get(i));
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
                if (i < 0 ) {
                    orderBySymbols.add(new InputColumn(node.outputs().size() + orderBySymbols.size()));
                } else {
                    orderBySymbols.add(new InputColumn(i));
                }
            }
            TopNProjection topNProjection = new TopNProjection(
                    com.google.common.base.Objects.firstNonNull(node.limit(), Constants.DEFAULT_SELECT_LIMIT),
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

    static class MultiGetResponseListener implements ActionListener<MultiGetResponse>, ProjectorUpstream {

        private final SettableFuture<TaskResult> result;
        private final List<FieldExtractor> fieldExtractor;
        @Nullable
        private final FlatProjectorChain projectorChain;
        private final Projector downstream;

        public MultiGetResponseListener(final SettableFuture<TaskResult> result,
                                        List<FieldExtractor> extractors,
                                        @Nullable FlatProjectorChain projectorChain) {
            this.result = result;
            this.fieldExtractor = extractors;
            this.projectorChain = projectorChain;
            if (projectorChain == null) {
                downstream = null;
            } else {
                downstream = projectorChain.firstProjector();
                downstream.registerUpstream(this);
                Futures.addCallback(projectorChain.result(), new FutureCallback<Object[][]>() {
                    @Override
                    public void onSuccess(@Nullable Object[][] rows) {
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
                    final Object[] row = new Object[fieldExtractor.size()];
                    int c = 0;
                    for (FieldExtractor extractor : fieldExtractor) {
                        row[c] = extractor.extract(response.getResponse());
                        c++;
                    }
                    rows.add(row);
                }

                result.set(new QueryResult(rows.toArray(new Object[rows.size()][])));
            } else {
                projectorChain.startProjections();
                try {
                    for (MultiGetItemResponse response : responses) {
                        if (response.isFailed() || !response.getResponse().isExists()) {
                            continue;
                        }
                        final Object[] row = new Object[fieldExtractor.size()];
                        int c = 0;
                        for (FieldExtractor extractor : fieldExtractor) {
                            row[c] = extractor.extract(response.getResponse());
                            c++;
                        }
                        if (!downstream.setNextRow(row)) {
                            break;
                        }
                    }
                    downstream.upstreamFinished();
                } catch (Exception e) {
                    downstream.upstreamFailed(e);
                }
            }
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }

        @Override
        public void downstream(Projector downstream) {
            throw new UnsupportedOperationException("Setting downstream isn't supported on MultiGetResponseListener");
        }
    }

    static class GetResponseListener implements ActionListener<GetResponse> {

        private final SettableFuture<TaskResult> result;
        private final List<FieldExtractor> extractors;

        public GetResponseListener(SettableFuture<TaskResult> result, List<FieldExtractor> extractors) {
            this.result = result;
            this.extractors = extractors;
        }

        @Override
        public void onResponse(GetResponse response) {
            if (!response.isExists()) {
                result.set( TaskResult.EMPTY_RESULT);
                return;
            }

            final Object[][] rows = new Object[1][extractors.size()];
            int c = 0;
            for (FieldExtractor extractor : extractors) {
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
        private final Map<String, Object> partitionValues;

        public GetResponseContext(Functions functions, int size, Map<String, Object> partitionValues) {
            super(functions, size);
            this.partitionValues = partitionValues;
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
            } else if (context.partitionValues.containsKey(field)) {
                return new FieldExtractor<GetResponse>() {
                    @Override
                    public Object extract(GetResponse response) {
                        return context.partitionValues.get(field);
                    }
                };
            } else {
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

}
