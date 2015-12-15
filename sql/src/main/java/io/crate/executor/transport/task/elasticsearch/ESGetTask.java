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
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.analyze.where.DocKeys;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Buckets;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.jobs.JobContextService;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.QueryResultRowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectorFactory;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.node.dql.ESGetNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.util.*;

public class ESGetTask extends EsJobContextTask implements RowUpstream {

    private final static SymbolToFieldExtractor<GetResponse> SYMBOL_TO_FIELD_EXTRACTOR =
            new SymbolToFieldExtractor<>(new GetResponseFieldExtractorFactory());

    public ESGetTask(UUID jobId,
                     Functions functions,
                     ProjectorFactory projectorFactory,
                     TransportMultiGetAction multiGetAction,
                     TransportGetAction getAction,
                     ESGetNode node,
                     JobContextService jobContextService) {
        super(jobId, node.executionPhaseId(), 1, jobContextService);

        assert multiGetAction != null;
        assert getAction != null;
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

        boolean fetchSource = false;
        List<String> includes = new ArrayList<>(ctx.references().size());

        for (Reference ref : ctx.references()) {
            if (ref.ident().columnIdent().isSystemColumn()) {
                if (ref.ident().columnIdent().name().equals("_raw")
                        || ref.ident().columnIdent().name().equals("_doc")) {
                    fetchSource = true;
                    break;
                }
            } else {
                includes.add(ref.ident().columnIdent().name());
            }
        }

        final FetchSourceContext fsc;

        if (fetchSource) {
            fsc = new FetchSourceContext(true);
        } else if (includes.size() > 0) {
            fsc = new FetchSourceContext(includes.toArray(new String[includes.size()]));
        } else {
            fsc = new FetchSourceContext(false);
        }

        ActionListener listener;
        ActionRequest request;
        TransportAction transportAction;

        FlatProjectorChain projectorChain = null;

        SettableFuture<TaskResult> result = SettableFuture.create();
        results.add(result);

        if (node.docKeys().size() > 1) {
            request = prepareMultiGetRequest(node, fsc);
            transportAction = multiGetAction;
            QueryResultRowDownstream queryResultRowDownstream = new QueryResultRowDownstream(result);
            projectorChain = getFlatProjectorChain(projectorFactory, node, queryResultRowDownstream);
            RowReceiver rowReceiver = projectorChain.firstProjector();
            listener = new MultiGetResponseListener(extractors, rowReceiver);
        } else {
            request = prepareGetRequest(node, fsc);
            transportAction = getAction;
            listener = new GetResponseListener(result, extractors);
        }

        createContext("lookup by primary key", ImmutableList.of(request), ImmutableList.of(listener),
                transportAction, projectorChain);
    }

    public static String indexName(DocTableInfo tableInfo, Optional<List<BytesRef>> values) {
        if (tableInfo.isPartitioned()) {
            return new PartitionName(tableInfo.ident(), values.get()).asIndexName();
        } else {
            return tableInfo.ident().indexName();
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
                    // TODO: use TopN.NO_LIMIT as default once this can be used as subrelation
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
            return FlatProjectorChain.withReceivers(ImmutableList.of(queryResultRowDownstream));
        }
    }

    private static List<Symbol> genInputColumns(int size) {
        List<Symbol> inputColumns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            inputColumns.add(new InputColumn(i));
        }
        return inputColumns;
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }

    /**
     * tells the RowUpstream that it should push all rows again
     */
    @Override
    public void repeat() {
        throw new UnsupportedOperationException();
    }

    static class MultiGetResponseListener implements ActionListener<MultiGetResponse> {

        private final List<FieldExtractor<GetResponse>> fieldExtractors;
        private final RowReceiver downstream;


        public MultiGetResponseListener(List<FieldExtractor<GetResponse>> extractors,
                                        RowReceiver rowDownstreamHandle) {
            downstream = rowDownstreamHandle;
            this.fieldExtractors = extractors;
        }


        @Override
        public void onResponse(MultiGetResponse responses) {
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
        private final SettableFuture<TaskResult> result;

        public GetResponseListener(SettableFuture<TaskResult> result, List<FieldExtractor<GetResponse>> extractors) {
            this.result = result;
            row = new FieldExtractorRow<>(extractors);
            bucket = Buckets.of(row);
        }

        @Override
        public void onResponse(GetResponse response) {
            if (!response.isExists()) {
                result.set(TaskResult.EMPTY_RESULT);
                return;
            }
            row.setCurrent(response);
            result.set(new QueryResult(bucket));
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
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

            if (field.startsWith("_")) {
                switch (field) {
                    case "_version":
                        return new FieldExtractor<GetResponse>() {
                            @Override
                            public Object extract(GetResponse response) {
                                return response.getVersion();
                            }
                        };
                    case "_id":
                        return new FieldExtractor<GetResponse>() {
                            @Override
                            public Object extract(GetResponse response) {
                                return response.getId();
                            }
                        };
                    case "_raw":
                        return new FieldExtractor<GetResponse>() {
                            @Override
                            public Object extract(GetResponse response) {
                                return response.getSourceAsBytesRef().toBytesRef();
                            }
                        };
                    case "_doc":
                        return new FieldExtractor<GetResponse>() {
                            @Override
                            public Object extract(GetResponse response) {
                                return response.getSource();
                            }
                        };
                }
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
