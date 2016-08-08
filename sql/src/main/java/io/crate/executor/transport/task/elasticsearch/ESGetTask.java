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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.analyze.symbol.InputColumn;
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
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.QueryResultRowDownstream;
import io.crate.operation.projectors.*;
import io.crate.planner.node.dql.ESGet;
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

public class ESGetTask extends EsJobContextTask {

    private final static SymbolToFieldExtractor<GetResponse> SYMBOL_TO_FIELD_EXTRACTOR =
            new SymbolToFieldExtractor<>(new GetResponseFieldExtractorFactory());

    private final static Set<ColumnIdent> FETCH_SOURCE_COLUMNS = ImmutableSet.of(DocSysColumns.DOC, DocSysColumns.RAW);

    public ESGetTask(Functions functions,
                     ProjectorFactory projectorFactory,
                     TransportMultiGetAction multiGetAction,
                     TransportGetAction getAction,
                     ESGet esGet,
                     JobContextService jobContextService) {
        super(esGet.jobId(), esGet.executionPhaseId(), 1, jobContextService);

        assert multiGetAction != null;
        assert getAction != null;
        assert esGet.docKeys().size() > 0;
        assert esGet.limit() != 0 : "shouldn't execute ESGetTask if limit is 0";

        ActionListener listener;
        ActionRequest request;
        TransportAction transportAction;
        FlatProjectorChain projectorChain = null;

        SettableFuture<TaskResult> result = SettableFuture.create();
        results.add(result);

        GetResponseContext ctx = new GetResponseContext(functions, esGet);
        List<Function<GetResponse, Object>> extractors = getFieldExtractors(esGet, ctx);
        FetchSourceContext fsc = getFetchSourceContext(ctx.references());
        if (esGet.docKeys().size() > 1) {
            request = prepareMultiGetRequest(esGet, fsc);
            transportAction = multiGetAction;
            QueryResultRowDownstream queryResultRowDownstream = new QueryResultRowDownstream(result);
            projectorChain = getFlatProjectorChain(projectorFactory, esGet, queryResultRowDownstream);
            RowReceiver rowReceiver = projectorChain.firstProjector();
            listener = new MultiGetResponseListener(extractors, rowReceiver);
        } else {
            request = prepareGetRequest(esGet, fsc);
            transportAction = getAction;
            listener = new GetResponseListener(result, extractors);
        }

        createContextBuilder("lookup by primary key", ImmutableList.of(request), ImmutableList.of(listener),
                transportAction, projectorChain);
    }

    private static FetchSourceContext getFetchSourceContext(List<Reference> references) {
        List<String> includes = new ArrayList<>(references.size());
        for (Reference ref : references) {
            if (ref.ident().columnIdent().isSystemColumn() &&
                FETCH_SOURCE_COLUMNS.contains(ref.ident().columnIdent())) {
                return new FetchSourceContext(true);
            }
            includes.add(ref.ident().columnIdent().name());
        }
        if (includes.size() > 0) {
            return new FetchSourceContext(includes.toArray(new String[includes.size()]));
        }
        return new FetchSourceContext(false);
    }

    private static List<Function<GetResponse, Object>> getFieldExtractors(ESGet node, GetResponseContext ctx) {
        List<Function<GetResponse, Object>> extractors = new ArrayList<>(node.outputs().size() + node.sortSymbols().size());
        for (Symbol symbol : node.outputs()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, ctx));
        }
        for (Symbol symbol : node.sortSymbols()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, ctx));
        }
        return extractors;
    }

    public static String indexName(DocTableInfo tableInfo, Optional<List<BytesRef>> values) {
        if (tableInfo.isPartitioned()) {
            return new PartitionName(tableInfo.ident(), values.get()).asIndexName();
        } else {
            return tableInfo.ident().indexName();
        }
    }

    private GetRequest prepareGetRequest(ESGet node, FetchSourceContext fsc) {
        DocKeys.DocKey docKey = node.docKeys().getOnlyKey();
        GetRequest getRequest = new GetRequest(indexName(node.tableInfo(), docKey.partitionValues()),
                Constants.DEFAULT_MAPPING_TYPE, docKey.id());
        getRequest.fetchSourceContext(fsc);
        getRequest.realtime(true);
        getRequest.routing(docKey.routing());
        return getRequest;
    }

    private MultiGetRequest prepareMultiGetRequest(ESGet node, FetchSourceContext fsc) {
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
                                                     ESGet node,
                                                     QueryResultRowDownstream queryResultRowDownstream) {
        if (node.limit() > TopN.NO_LIMIT || node.offset() > 0 || !node.sortSymbols().isEmpty()) {
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
                    node.limit(),
                    node.offset(),
                    orderBySymbols,
                    node.reverseFlags(),
                    node.nullsFirst()
            );
            topNProjection.outputs(InputColumn.numInputs(node.outputs().size()));
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


    static class MultiGetResponseListener implements ActionListener<MultiGetResponse> {

        private final List<Function<GetResponse, Object>> fieldExtractors;
        private final RowReceiver downstream;


        MultiGetResponseListener(List<Function<GetResponse, Object>> extractors,
                                 RowReceiver rowDownstreamHandle) {
            downstream = rowDownstreamHandle;
            this.fieldExtractors = extractors;
        }


        @Override
        public void onResponse(MultiGetResponse responses) {
            FieldExtractorRow<GetResponse> row = new FieldExtractorRow<>(fieldExtractors);
            try {
                loop:
                for (MultiGetItemResponse response : responses) {
                    if (response.isFailed() || !response.getResponse().isExists()) {
                        continue;
                    }
                    row.setCurrent(response.getResponse());
                    RowReceiver.Result result = downstream.setNextRow(row);
                    switch (result) {
                        case CONTINUE:
                            continue;
                        case PAUSE:
                            throw new UnsupportedOperationException("ESGetTask doesn't support pause");
                        case STOP:
                            break loop;
                    }
                    throw new AssertionError("Unrecognized setNextRow result: " + result);
                }
                downstream.finish(RepeatHandle.UNSUPPORTED);
            } catch (Exception e) {
                downstream.fail(e);
            }
        }

        @Override
        public void onFailure(Throwable e) {
            downstream.fail(e);
        }
    }

    private static class GetResponseListener implements ActionListener<GetResponse> {

        private final Bucket bucket;
        private final FieldExtractorRow<GetResponse> row;
        private final SettableFuture<TaskResult> result;

        GetResponseListener(SettableFuture<TaskResult> result, List<Function<GetResponse, Object>> extractors) {
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
        private final ESGet node;

        GetResponseContext(Functions functions, ESGet node) {
            super(functions, node.outputs().size());
            this.node = node;
            ids2Keys = new HashMap<>(node.docKeys().size());
            for (DocKeys.DocKey key : node.docKeys()) {
                ids2Keys.put(key.id(), key);
            }
        }

        @Override
        public Object inputValueFor(InputColumn inputColumn) {
            throw new AssertionError("GetResponseContext does not support resolving InputColumn");
        }
    }

    private static class GetResponseFieldExtractorFactory implements FieldExtractorFactory<GetResponse, GetResponseContext> {

        @Override
        public Function<GetResponse, Object> build(final Reference reference, final GetResponseContext context) {
            final String field = reference.ident().columnIdent().fqn();

            if (field.startsWith("_")) {
                switch (field) {
                    case "_version":
                        return new Function<GetResponse, Object>() {
                            @Override
                            public Object apply(GetResponse response) {
                                return response.getVersion();
                            }
                        };
                    case "_id":
                        return new Function<GetResponse, Object>() {
                            @Override
                            public Object apply(GetResponse response) {
                                return response.getId();
                            }
                        };
                    case "_raw":
                        return new Function<GetResponse, Object>() {
                            @Override
                            public Object apply(GetResponse response) {
                                return response.getSourceAsBytesRef().toBytesRef();
                            }
                        };
                    case "_doc":
                        return new Function<GetResponse, Object>() {
                            @Override
                            public Object apply(GetResponse response) {
                                return response.getSource();
                            }
                        };
                }
            } else if (context.node.tableInfo().isPartitioned()
                    && context.node.tableInfo().partitionedBy().contains(reference.ident().columnIdent())) {
                final int pos = context.node.tableInfo().primaryKey().indexOf(reference.ident().columnIdent());
                if (pos >= 0) {
                    return new Function<GetResponse, Object>() {
                        @Override
                        public Object apply(GetResponse response) {
                            return ValueSymbolVisitor.VALUE.process(context.ids2Keys.get(response.getId()).values().get(pos));
                        }
                    };
                }
            }
            return new Function<GetResponse, Object>() {
                @Override
                public Object apply(GetResponse response) {
                    Map<String, Object> sourceAsMap = response.getSourceAsMap();
                    assert sourceAsMap != null;
                    return reference.valueType().value(XContentMapValues.extractValue(field, sourceAsMap));
                }
            };
        }
    }

}
