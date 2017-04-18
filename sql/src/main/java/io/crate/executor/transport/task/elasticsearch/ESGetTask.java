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

import com.google.common.collect.ImmutableSet;
import io.crate.Constants;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.collections.Lists2;
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.RowsBatchIterator;
import io.crate.executor.JobTask;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.jobs.AbstractExecutionSubContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.InputFactory;
import io.crate.operation.InputRow;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.ProjectingBatchConsumer;
import io.crate.operation.projectors.ProjectorFactory;
import io.crate.operation.projectors.TopN;
import io.crate.planner.node.dql.ESGet;
import io.crate.planner.projection.OrderedTopNProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.support.TransportAction;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.StreamSupport;

public class ESGetTask extends JobTask {

    private final static Set<ColumnIdent> FETCH_SOURCE_COLUMNS = ImmutableSet.of(DocSysColumns.DOC, DocSysColumns.RAW);
    private final ProjectorFactory projectorFactory;
    private final TransportActionProvider transportActionProvider;
    private final ESGet esGet;

    private final JobContextService jobContextService;
    private final FetchSourceContext fsc;
    private final InputRow inputRow;
    private final Collection<CollectExpression<GetResponse, ?>> expressions;

    static abstract class JobContext<Action extends TransportAction<Request, Response>,
        Request extends ActionRequest, Response extends ActionResponse> extends AbstractExecutionSubContext
        implements ActionListener<Response> {

        private static final Logger LOGGER = Loggers.getLogger(JobContext.class);


        private final Request request;
        private final Action transportAction;
        protected final ESGetTask task;

        BatchConsumer consumer;

        JobContext(ESGetTask task, Action transportAction, BatchConsumer consumer) {
            super(task.esGet.executionPhaseId(), LOGGER);
            this.task = task;
            this.transportAction = transportAction;
            this.request = prepareRequest(task.esGet, task.fsc);
            this.consumer = consumer;
        }

        @Nullable
        protected abstract Request prepareRequest(ESGet node, FetchSourceContext fsc);

        @Override
        protected void innerStart() {
            if (request == null) {
                // request can be null if id is null -> since primary keys cannot be null this is a no-match
                consumer.accept(RowsBatchIterator.empty(), null);
                close();
            } else {
                transportAction.execute(request, this);
            }
        }
    }

    private static class MultiGetJobContext extends JobContext<TransportMultiGetAction, MultiGetRequest, MultiGetResponse> {

        MultiGetJobContext(ESGetTask task,
                           TransportMultiGetAction transportAction,
                           BatchConsumer consumer) {
            super(task, transportAction, consumer);
            assert task.esGet.docKeys().size() > 1 : "number of docKeys must be > 1";
            assert task.projectorFactory != null : "task.projectorFactory must not be null";
        }

        @Override
        protected void innerPrepare() throws Exception {
            consumer = prependProjectors(consumer);
        }

        private BatchConsumer prependProjectors(BatchConsumer consumer) {
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
                Projection projection;
                if (task.esGet.sortSymbols().isEmpty()) {
                    projection = new TopNProjection(
                        task.esGet.limit(), task.esGet.offset(), InputColumn.numInputs(task.esGet.outputs().size()));
                } else {
                    projection = new OrderedTopNProjection(
                        task.esGet.limit(),
                        task.esGet.offset(),
                        InputColumn.numInputs(task.esGet.outputs().size()),
                        orderBySymbols,
                        task.esGet.reverseFlags(),
                        task.esGet.nullsFirst()
                    );
                }
                return ProjectingBatchConsumer.create(
                    consumer,
                    Collections.singletonList(projection),
                    task.jobId(),
                    null,
                    task.projectorFactory
                );
            } else {
                return consumer;
            }
        }

        @Override
        public String name() {
            return "MultiGet";
        }

        @Override
        public void onResponse(MultiGetResponse responses) {
            try {
                Iterable<Row> rows = responseToRows(responses);
                consumer.accept(RowsBatchIterator.newInstance(rows, task.inputRow.numColumns()), null);
                close();
            } catch (Exception e) {
                consumer.accept(null, e);
                close(e);
            }
        }

        private Iterable<Row> responseToRows(MultiGetResponse responses) {
            return () -> StreamSupport.stream(responses.spliterator(), false)
                .filter(r -> r.isFailed() == false)
                .filter(r -> r.getResponse().isExists())
                .map(response -> {
                    for (CollectExpression<GetResponse, ?> expression : task.expressions) {
                        expression.setNextRow(response.getResponse());
                    }
                    return (Row) task.inputRow;
                }).iterator();
        }


        @Override
        public void onFailure(Exception e) {
            consumer.accept(null, e);
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

        SingleGetJobContext(ESGetTask task,
                            TransportGetAction transportAction,
                            BatchConsumer consumer) {
            super(task, transportAction, consumer);
            assert task.esGet.docKeys().size() == 1 : "numer of docKeys must be 1";
        }

        @Override
        public String name() {
            return "SingleGet";
        }

        @Override
        protected GetRequest prepareRequest(ESGet node, FetchSourceContext fsc) {
            DocKeys.DocKey docKey = node.docKeys().getOnlyKey();
            String id = docKey.id();
            if (id == null) {
                return null;
            }
            GetRequest getRequest = new GetRequest(indexName(node.tableInfo(), docKey.partitionValues().orNull()),
                Constants.DEFAULT_MAPPING_TYPE, id);
            getRequest.fetchSourceContext(fsc);
            getRequest.realtime(true);
            getRequest.routing(docKey.routing());
            return getRequest;
        }

        @Override
        public void onResponse(GetResponse response) {
            if (response.isExists()) {
                for (CollectExpression<GetResponse, ?> expression : task.expressions) {
                    expression.setNextRow(response);
                }
                consumer.accept(RowsBatchIterator.newInstance(task.inputRow), null);
            } else {
                consumer.accept(RowsBatchIterator.empty(), null);
            }
            close();
        }

        @Override
        public void onFailure(Exception e) {
            if (task.esGet.tableInfo().isPartitioned() && e instanceof IndexNotFoundException) {
                // this means we have no matching document
                consumer.accept(RowsBatchIterator.empty(), null);
                close();
            } else {
                consumer.accept(null, e);
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

        assert esGet.docKeys().size() > 0 : "number of docKeys must be > 0";
        assert esGet.limit() != 0 : "shouldn't execute ESGetTask if limit is 0";

        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions, ReplaceMode.MUTATE);
        for (DocKeys.DocKey docKey : esGet.docKeys()) {
            normalizer.normalizeInplace(docKey.values(), null);
        }
        InputFactory inputFactory = new InputFactory(functions);
        Map<String, DocKeys.DocKey> docKeysById = groupDocKeysById(esGet.docKeys());
        List<ColumnIdent> columns = new ArrayList<>();
        GetResponseRefResolver refResolver = new GetResponseRefResolver(columns::add, esGet.tableInfo(), docKeysById);
        InputFactory.Context<CollectExpression<GetResponse, ?>> ctx = inputFactory.ctxForRefs(refResolver);
        List<Symbol> outputsAndSortSymbols = Lists2.concatUnique(esGet.outputs(), esGet.sortSymbols());
        ctx.add(outputsAndSortSymbols);

        inputRow = new InputRow(ctx.topLevelInputs());
        expressions = ctx.expressions();
        fsc = getFetchSourceContext(columns);
    }

    @Override
    public void execute(BatchConsumer consumer, Row parameters) {
        JobContext jobContext;
        if (esGet.docKeys().size() == 1) {
            jobContext = new SingleGetJobContext(this, transportActionProvider.transportGetAction(), consumer);
        } else {
            jobContext = new MultiGetJobContext(this, transportActionProvider.transportMultiGetAction(), consumer);
        }
        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId());
        builder.addSubContext(jobContext);

        try {
            JobExecutionContext ctx = jobContextService.createContext(builder);
            ctx.start();
        } catch (Throwable throwable) {
            consumer.accept(null, throwable);
        }
    }

    private static FetchSourceContext getFetchSourceContext(List<ColumnIdent> columns) {
        List<String> includes = new ArrayList<>(columns.size());
        for (ColumnIdent col : columns) {
            if (col.isSystemColumn() && FETCH_SOURCE_COLUMNS.contains(col)) {
                return new FetchSourceContext(true);
            }
            includes.add(col.name());
        }
        if (includes.size() > 0) {
            return new FetchSourceContext(false, includes.toArray(new String[includes.size()]), null);
        }
        return new FetchSourceContext(false);
    }

    public static String indexName(DocTableInfo tableInfo, @Nullable List<BytesRef> values) {
        if (tableInfo.isPartitioned()) {
            assert values != null : "values must not be null";
            return new PartitionName(tableInfo.ident(), values).asIndexName();
        } else {
            return tableInfo.ident().indexName();
        }
    }

    private static Map<String, DocKeys.DocKey> groupDocKeysById(DocKeys docKeys) {
        Map<String, DocKeys.DocKey> keysById = new HashMap<>(docKeys.size());
        for (DocKeys.DocKey key : docKeys) {
            keysById.put(key.id(), key);
        }
        return keysById;
    }
}
