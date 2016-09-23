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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.Constants;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.analyze.where.DocKeys;
import io.crate.executor.JobTask;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.jobs.AbstractExecutionSubContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.projectors.*;
import io.crate.planner.node.dql.ESGet;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import javax.annotation.Nullable;
import java.util.*;

public class ESGetTask extends JobTask {

    private final static SymbolToFieldExtractor<GetResponse> SYMBOL_TO_FIELD_EXTRACTOR =
        new SymbolToFieldExtractor<>(new GetResponseFieldExtractorFactory());

    private final static Set<ColumnIdent> FETCH_SOURCE_COLUMNS = ImmutableSet.of(DocSysColumns.DOC, DocSysColumns.RAW);
    private final ProjectorFactory projectorFactory;
    private final TransportActionProvider transportActionProvider;
    private final ESGet esGet;

    private final JobContextService jobContextService;
    private final List<Function<GetResponse, Object>> extractors;
    private final FetchSourceContext fsc;

    static abstract class JobContext<Action extends TransportAction<Request, Response>,
        Request extends ActionRequest, Response extends ActionResponse> extends AbstractExecutionSubContext
        implements ActionListener<Response> {

        private static final ESLogger LOGGER = Loggers.getLogger(JobContext.class);


        private final Request request;
        protected RowReceiver downstream;

        private final Action transportAction;
        protected final ESGetTask task;

        JobContext(ESGetTask task, Action transportAction, RowReceiver downstream) {
            super(task.esGet.executionPhaseId(), LOGGER);
            this.task = task;
            this.transportAction = transportAction;
            this.request = prepareRequest(task.esGet, task.fsc);
            this.downstream = downstream;
        }

        protected abstract Request prepareRequest(ESGet node, FetchSourceContext fsc);

        @Override
        protected void innerStart() {
            transportAction.execute(request, this);
        }
    }

    private static class MultiGetJobContext extends JobContext<TransportMultiGetAction, MultiGetRequest, MultiGetResponse> {

        MultiGetJobContext(ESGetTask task,
                           TransportMultiGetAction transportAction,
                           RowReceiver downstream) {
            super(task, transportAction, downstream);
            assert task.esGet.docKeys().size() > 1;
            assert task.projectorFactory != null;
        }

        @Override
        protected void innerPrepare() throws Exception {
            FlatProjectorChain projectorChain = getFlatProjectorChain(downstream);
            downstream = projectorChain.firstProjector();
            projectorChain.prepare();
        }

        private FlatProjectorChain getFlatProjectorChain(RowReceiver downstream) {
            if (task.esGet.limit() > TopN.NO_LIMIT || task.esGet.offset() > 0 || !task.esGet.sortSymbols().isEmpty()) {
                List<Symbol> orderBySymbols = new ArrayList<>(task.esGet.sortSymbols().size());
                for (Symbol symbol : task.esGet.sortSymbols()) {
                    int i = task.esGet.outputs().indexOf(symbol);
                    if (i < 0) {
                        orderBySymbols.add(new InputColumn(task.esGet.outputs().size() + orderBySymbols.size()));
                    } else {
                        orderBySymbols.add(new InputColumn(i));
                    }
                }
                TopNProjection topNProjection = new TopNProjection(
                    task.esGet.limit(),
                    task.esGet.offset(),
                    orderBySymbols,
                    task.esGet.reverseFlags(),
                    task.esGet.nullsFirst()
                );
                topNProjection.outputs(InputColumn.numInputs(task.esGet.outputs().size()));
                return FlatProjectorChain.withAttachedDownstream(
                    task.projectorFactory,
                    null,
                    ImmutableList.<Projection>of(topNProjection),
                    downstream,
                    task.jobId()
                );
            } else {
                return FlatProjectorChain.withReceivers(ImmutableList.of(downstream));
            }
        }

        @Override
        public String name() {
            return "MultiGet";
        }

        @Override
        public void onResponse(MultiGetResponse responses) {
            FieldExtractorRow<GetResponse> row = new FieldExtractorRow<>(task.extractors);
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
                close();
            } catch (Exception e) {
                downstream.fail(e);
                close(e);
            }
        }

        @Override
        public void onFailure(Throwable e) {
            downstream.fail(e);
            close(e);
        }

        @Override
        protected MultiGetRequest prepareRequest(ESGet node, FetchSourceContext fsc) {
            MultiGetRequest multiGetRequest = new MultiGetRequest();
            for (DocKeys.DocKey key : node.docKeys()) {
                if (key.id() != null) {
                    MultiGetRequest.Item item = new MultiGetRequest.Item(
                        indexName(node.tableInfo(), key.partitionValues().orNull()), Constants.DEFAULT_MAPPING_TYPE, key.id());
                    item.fetchSourceContext(fsc);
                    item.routing(key.routing());
                    multiGetRequest.add(item);
                }
            }
            multiGetRequest.realtime(true);
            return multiGetRequest;
        }
    }

    private static class SingleGetJobContext extends JobContext<TransportGetAction, GetRequest, GetResponse> {

        private final FieldExtractorRow<GetResponse> row;

        SingleGetJobContext(ESGetTask task,
                            TransportGetAction transportAction,
                            RowReceiver downstream) {
            super(task, transportAction, downstream);
            assert task.esGet.docKeys().size() == 1;
            this.row = new FieldExtractorRow<>(task.extractors);
        }

        @Override
        public String name() {
            return "SingleGet";
        }

        @Override
        protected GetRequest prepareRequest(ESGet node, FetchSourceContext fsc) {
            DocKeys.DocKey docKey = node.docKeys().getOnlyKey();
            GetRequest getRequest = new GetRequest(indexName(node.tableInfo(), docKey.partitionValues().orNull()),
                Constants.DEFAULT_MAPPING_TYPE, docKey.id());
            getRequest.fetchSourceContext(fsc);
            getRequest.realtime(true);
            getRequest.routing(docKey.routing());
            return getRequest;
        }

        @Override
        public void onResponse(GetResponse response) {
            if (response.isExists()) {
                row.setCurrent(response);
                downstream.setNextRow(row);
            }
            downstream.finish(RepeatHandle.UNSUPPORTED);
            close();
        }

        @Override
        public void onFailure(Throwable e) {
            if (task.esGet.tableInfo().isPartitioned() && e instanceof IndexNotFoundException) {
                // this means we have no matching document
                downstream.finish(RepeatHandle.UNSUPPORTED);
                close();
            } else {
                downstream.fail(e);
                close(e);
            }
        }
    }

    public ESGetTask(Functions functions,
                     ProjectorFactory projectorFactory,
                     TransportActionProvider transportActionProvider,
                     ESGet esGet,
                     JobContextService jobContextService) {
        super(esGet.jobId());
        this.projectorFactory = projectorFactory;
        this.transportActionProvider = transportActionProvider;
        this.esGet = esGet;
        this.jobContextService = jobContextService;

        assert esGet.docKeys().size() > 0;
        assert esGet.limit() != 0 : "shouldn't execute ESGetTask if limit is 0";

        GetResponseContext ctx = new GetResponseContext(functions, esGet);
        extractors = getFieldExtractors(esGet, ctx);
        fsc = getFetchSourceContext(ctx.references());
    }

    @Override
    public void execute(RowReceiver rowReceiver) {
        JobContext jobContext;
        if (esGet.docKeys().size() == 1) {
            jobContext = new SingleGetJobContext(this, transportActionProvider.transportGetAction(), rowReceiver);
        } else {
            jobContext = new MultiGetJobContext(this, transportActionProvider.transportMultiGetAction(), rowReceiver);
        }
        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId());
        builder.addSubContext(jobContext);

        try {
            JobExecutionContext ctx = jobContextService.createContext(builder);
            ctx.start();
        } catch (Throwable throwable) {
            rowReceiver.fail(throwable);
        }
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
        List<Function<GetResponse, Object>> extractors = new ArrayList<>(
            node.outputs().size() + node.sortSymbols().size());
        for (Symbol symbol : node.outputs()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, ctx));
        }
        for (Symbol symbol : node.sortSymbols()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, ctx));
        }
        return extractors;
    }

    public static String indexName(DocTableInfo tableInfo, @Nullable List<BytesRef> values) {
        if (tableInfo.isPartitioned()) {
            assert values != null;
            return new PartitionName(tableInfo.ident(), values).asIndexName();
        } else {
            return tableInfo.ident().indexName();
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
