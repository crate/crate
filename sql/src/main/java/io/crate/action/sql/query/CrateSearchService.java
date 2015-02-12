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

package io.crate.action.sql.query;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.crate.Constants;
import io.crate.core.StringUtils;
import io.crate.executor.transport.task.elasticsearch.SortOrder;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.InternalSearchService;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.search.sort.SortParseElement;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CrateSearchService extends InternalSearchService {

    private final SortSymbolVisitor sortSymbolVisitor;
    private final Functions functions;

    @Inject
    public CrateSearchService(Settings settings,
                              ClusterService clusterService,
                              IndicesService indicesService,
                              IndicesLifecycle indicesLifecycle,
                              IndicesWarmer indicesWarmer,
                              ThreadPool threadPool,
                              ScriptService scriptService,
                              CacheRecycler cacheRecycler,
                              PageCacheRecycler pageCacheRecycler,
                              BigArrays bigArrays,
                              DfsPhase dfsPhase,
                              QueryPhase queryPhase,
                              FetchPhase fetchPhase,
                              Functions functions,
                              IndicesQueryCache indicesQueryCache) {
        super(settings, clusterService, indicesService, indicesLifecycle,
                indicesWarmer,
                threadPool,
                scriptService,
                cacheRecycler,
                pageCacheRecycler,
                bigArrays, dfsPhase, queryPhase, fetchPhase, indicesQueryCache);
        this.functions = functions;
        CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor =
                new CollectInputSymbolVisitor<>(functions, LuceneDocLevelReferenceResolver.INSTANCE);
        sortSymbolVisitor = new SortSymbolVisitor(inputSymbolVisitor);
    }


    public ScrollQuerySearchResult executeScrollQueryPhase(QueryShardScrollRequest request) {
        final SearchContext context = findContext(request.id());
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            contextProcessing(context);
            processScroll(request, context);
            queryPhase.execute(context);
            contextProcessedSuccessfully(context);
            context.indexShard().searchService().onQueryPhase(context, System.nanoTime() - time);
            return new ScrollQuerySearchResult(context.queryResult(), context.shardTarget());
        } catch (Throwable e) {
            context.indexShard().searchService().onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            freeContext(context.id());
            throw Throwables.propagate(e);
        } finally {
            cleanContext(context);
        }
    }

    private void processScroll(QueryShardScrollRequest request, SearchContext context) {
        // process scroll
        context.size(request.limit());
        context.from(request.from());

        context.scroll(request.scroll());
        // update the context keep alive based on the new scroll value
        if (request.scroll() != null && request.scroll().keepAlive() != null) {
            context.keepAlive(request.scroll().keepAlive().millis());
        }
    }

    public QuerySearchResult executeQueryPhase(QueryShardRequest request) {
        SearchContext context = createAndPutContext(request);
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            contextProcessing(context);
            queryPhase.execute(context);

            assert context.searchType() != SearchType.COUNT : "searchType COUNT is not supported using QueryShardRequests";
            contextProcessedSuccessfully(context);

            context.indexShard().searchService().onQueryPhase(context, System.nanoTime() - time);
            return context.queryResult();
        } catch (Throwable e) {
            context.indexShard().searchService().onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            freeContext(context.id());
            throw Throwables.propagate(e);
        } finally {
            cleanContext(context);
        }
    }



    private SearchContext createAndPutContext(QueryShardRequest request) {
        SearchContext context = createContext(request, null);
        boolean success = false;
        try {
            activeContexts.put(context.id(), context);
            context.indexShard().searchService().onNewContext(context);
            success = true;
            return context;
        } finally {
            if (!success) {
                freeContext(context.id());
            }
        }
    }

    /**
     * Creates a new SearchContext. <br />
     * <p>
     * This is similar to
     * {@link org.elasticsearch.search.InternalSearchService#createContext(org.elasticsearch.search.internal.ShardSearchRequest, org.elasticsearch.index.engine.Engine.Searcher)}
     * but uses Symbols to create the lucene query / sorting.
     * </p>
     *
     */
    private SearchContext createContext(QueryShardRequest request, @Nullable Engine.Searcher searcher) {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());

        SearchShardTarget searchShardTarget = new SearchShardTarget(
                clusterService.localNode().id(),
                request.index(),
                request.shardId()
        );
        Engine.Searcher engineSearcher = searcher == null ? indexShard.acquireSearcher("search") : searcher;
        long keepAlive = defaultKeepAlive;
        if (request.scroll().isPresent() && request.scroll().get().keepAlive() != null) {
            keepAlive = request.scroll().get().keepAlive().millis();
        }
        SearchContext context = new CrateSearchContext(
                idGenerator.incrementAndGet(),
                0, // TODO: is this necessary? seems not, wasn't set before
                new String[] { Constants.DEFAULT_MAPPING_TYPE },
                System.currentTimeMillis(),
                searchShardTarget,
                engineSearcher,
                indexService,
                indexShard,
                scriptService,
                cacheRecycler,
                pageCacheRecycler,
                bigArrays,
                threadPool.estimatedTimeInMillisCounter(),
                request.scroll(),
                keepAlive
        );
        SearchContext.setCurrent(context);

        try {
            LuceneQueryBuilder builder = new LuceneQueryBuilder(functions, context, indexService.cache());
            LuceneQueryBuilder.Context ctx = builder.convert(request.whereClause());
            context.parsedQuery(new ParsedQuery(ctx.query(), ImmutableMap.<String, Filter>of()));
            Float minScore = ctx.minScore();
            if (minScore != null) {
                context.minimumScore(minScore);
            }

            // the OUTPUTS_VISITOR sets the sourceFetchContext / version / minScore onto the SearchContext
            OutputContext outputContext = new OutputContext(context, request.partitionBy());
            OUTPUTS_VISITOR.process(request.outputs(), outputContext);

            context.sort(generateLuceneSort(
                    context, request.orderBy(), request.reverseFlags(), request.nullsFirst()));

            context.from(request.offset());
            context.size(request.limit());
            // pre process
            dfsPhase.preProcess(context);
            queryPhase.preProcess(context);
            fetchPhase.preProcess(context);


        } catch (Throwable e) {
            context.close();
            throw Throwables.propagate(e);
        }
        return context;
    }

    private static final OutputSymbolVisitor OUTPUTS_VISITOR = new OutputSymbolVisitor();

    private static class OutputContext {
        private final SearchContext searchContext;
        private final List<ReferenceInfo> partitionBy;
        private final List<String> fields = new ArrayList<>();
        public boolean needWholeSource = false;

        private OutputContext(SearchContext searchContext, List<ReferenceInfo> partitionBy) {
            this.searchContext = searchContext;
            this.partitionBy = partitionBy;
        }
    }

    private static class OutputSymbolVisitor extends SymbolVisitor<OutputContext, Void> {

        public void process(List<? extends Symbol> outputs, OutputContext context) {
            for (Symbol output : outputs) {
                process(output, context);
            }
            if (!context.needWholeSource) {
                if (context.fields.isEmpty()) {
                    context.searchContext.fetchSourceContext(new FetchSourceContext(false));
                } else {
                    Set<String> fields = StringUtils.commonAncestors(context.fields);
                    context.searchContext.fetchSourceContext(
                            new FetchSourceContext(fields.toArray(new String[fields.size()])));
                }
            }
        }

        @Override
        public Void visitReference(Reference symbol, OutputContext context) {
            ColumnIdent columnIdent = symbol.info().ident().columnIdent();
            if (columnIdent.isSystemColumn()) {
                if (DocSysColumns.VERSION.equals(columnIdent)) {
                    context.searchContext.version(true);
                } else {
                    context.needWholeSource = true;
                }
            } else if (!context.partitionBy.contains(symbol.info())) {
                context.fields.add(columnIdent.fqn());
            }
            return null;
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, OutputContext context) {
            return visitReference(symbol, context);
        }

        @Override
        protected Void visitSymbol(Symbol symbol, OutputContext context) {
            throw new UnsupportedOperationException(SymbolFormatter.format(
                    "Can't use %s as an output", symbol));
        }
    }

    private static final Map<DataType, SortField.Type> luceneTypeMap = ImmutableMap.<DataType, SortField.Type>builder()
            .put(DataTypes.STRING, SortField.Type.STRING)
            .put(DataTypes.LONG, SortField.Type.LONG)
            .put(DataTypes.INTEGER, SortField.Type.INT)
            .put(DataTypes.DOUBLE, SortField.Type.DOUBLE)
            .put(DataTypes.FLOAT, SortField.Type.FLOAT)
            .build();


    @Nullable
    private Sort generateLuceneSort(SearchContext context,
                                    List<Symbol> symbols,
                                    boolean[] reverseFlags,
                                    Boolean[] nullsFirst) {
        if (symbols.isEmpty()) {
            return null;
        }
        SortField[] sortFields = new SortField[symbols.size()];
        for (int i = 0, symbolsSize = symbols.size(); i < symbolsSize; i++) {
            sortFields[i] = sortSymbolVisitor.generateSortField(
                    symbols.get(i), new SortSymbolContext(context, reverseFlags[i], nullsFirst[i]));
        }
        return new Sort(sortFields);
    }

    private static class SortSymbolContext {

        private final boolean reverseFlag;
        private final CollectorContext context;
        private final Boolean nullFirst;

        public SortSymbolContext(SearchContext searchContext, boolean reverseFlag, Boolean nullFirst) {
            this.nullFirst = nullFirst;
            this.context = new CollectorContext();
            this.context.searchContext(searchContext);
            this.reverseFlag = reverseFlag;
        }
    }

    private static class SortSymbolVisitor extends SymbolVisitor<SortSymbolContext, SortField> {

        private final CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor;

        public SortSymbolVisitor(CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor) {
            super();
            this.inputSymbolVisitor = inputSymbolVisitor;
        }

        public SortField generateSortField(Symbol symbol, SortSymbolContext sortSymbolContext) {
            return process(symbol, sortSymbolContext);
        }


        /**
         * generate a SortField from a Reference symbol.
         *
         * the implementation is similar to what {@link org.elasticsearch.search.sort.SortParseElement}
         * does.
         */
        @Override
        public SortField visitReference(Reference symbol, SortSymbolContext context) {
            // can't use the SortField(fieldName, type) constructor
            // because values are saved using docValues and therefore they're indexed in lucene as binary and not
            // with the reference valueType.
            // this is why we use a custom comparator source with the same logic as ES

            ColumnIdent columnIdent = symbol.info().ident().columnIdent();
            if (columnIdent.isColumn() && SortParseElement.SCORE_FIELD_NAME.equals(columnIdent.name())) {
                return !context.reverseFlag ? SortParseElement.SORT_SCORE_REVERSE : SortParseElement.SORT_SCORE;
            }

            MultiValueMode sortMode = context.reverseFlag ? MultiValueMode.MAX : MultiValueMode.MIN;
            SearchContext searchContext = context.context.searchContext();

            FieldMapper fieldMapper = context.context.searchContext().smartNameFieldMapper(columnIdent.fqn());
            SortOrder sortOrder = new SortOrder(context.reverseFlag, context.nullFirst);
            IndexFieldData.XFieldComparatorSource fieldComparatorSource =
                    searchContext.fieldData().getForField(fieldMapper).comparatorSource(sortOrder.missing(), sortMode, null);

            return new SortField(
                    fieldMapper.names().indexName(),
                    fieldComparatorSource,
                    context.reverseFlag
            );
        }

        @Override
        public SortField visitFunction(final Function function, final SortSymbolContext context) {
            CollectInputSymbolVisitor.Context inputContext = inputSymbolVisitor.process(function);
            ArrayList<Input<?>> inputs = inputContext.topLevelInputs();
            assert inputs.size() == 1;
            final Input functionInput = inputs.get(0);
            @SuppressWarnings("unchecked")
            final List<LuceneCollectorExpression> expressions = inputContext.docLevelExpressions();
            final SortField.Type type = luceneTypeMap.get(function.valueType());
            final SortField.Type reducedType = MoreObjects.firstNonNull(type, SortField.Type.DOC);
            final SortOrder sortOrder = new SortOrder(context.reverseFlag, context.nullFirst);

            return new SortField(function.toString(), new IndexFieldData.XFieldComparatorSource() {
                @Override
                public FieldComparator<?> newComparator(String fieldName, int numHits, int sortPos, boolean reversed) throws IOException {
                    return new InputFieldComparator(
                            numHits,
                            context.context,
                            expressions,
                            functionInput,
                            function.valueType(),
                            type == null ? null : missingObject(sortOrder.missing(), reversed)
                    );
                }

                @Override
                public SortField.Type reducedType() {
                    return reducedType;
                }
            }, context.reverseFlag);
        }

        @Override
        protected SortField visitSymbol(Symbol symbol, SortSymbolContext context) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("sorting on %s is not supported", symbol));
        }
    }

    static class InputFieldComparator extends FieldComparator {

        private final Object[] values;
        private final Input input;
        private final List<LuceneCollectorExpression> collectorExpressions;
        private final Object missingValue;
        private final DataType valueType;
        private Object bottom;
        private Object top;

        public InputFieldComparator(int numHits,
                                    CollectorContext context,
                                    List<LuceneCollectorExpression> collectorExpressions,
                                    Input input,
                                    DataType valueType,
                                    Object missingValue) {
            this.collectorExpressions = collectorExpressions;
            this.missingValue = missingValue;
            for (int i = 0, collectorExpressionsSize = collectorExpressions.size(); i < collectorExpressionsSize; i++) {
                LuceneCollectorExpression collectorExpression = collectorExpressions.get(i);
                collectorExpression.startCollect(context);
            }
            this.valueType = valueType;
            this.values = new Object[numHits];
            this.input = input;
        }

        @Override
        @SuppressWarnings("unchecked")
        public int compare(int slot1, int slot2) {
            return valueType.compareValueTo(values[slot1], values[slot2]);
        }

        @Override
        public void setBottom(int slot) {
            bottom = values[slot];
        }

        @Override
        public void setTopValue(Object value) {
            top = value;
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compareBottom(int doc) throws IOException {
            for (int i = 0, collectorExpressionsSize = collectorExpressions.size(); i < collectorExpressionsSize; i++) {
                LuceneCollectorExpression collectorExpression = collectorExpressions.get(i);
                collectorExpression.setNextDocId(doc);
            }
            return valueType.compareValueTo(bottom, input.value());
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compareTop(int doc) throws IOException {
            for (int i = 0, collectorExpressionsSize = collectorExpressions.size(); i < collectorExpressionsSize; i++) {
                LuceneCollectorExpression collectorExpression = collectorExpressions.get(i);
                collectorExpression.setNextDocId(doc);
            }
            return valueType.compareValueTo(top, input.value());
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            for (int i = 0, collectorExpressionsSize = collectorExpressions.size(); i < collectorExpressionsSize; i++) {
                LuceneCollectorExpression collectorExpression = collectorExpressions.get(i);
                collectorExpression.setNextDocId(doc);
            }
            Object value = input.value();
            if (value == null) {
                values[slot] = missingValue;
            } else {
                values[slot] = value;
            }
        }

        @Override
        public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
            for (int i = 0, collectorExpressionsSize = collectorExpressions.size(); i < collectorExpressionsSize; i++) {
                LuceneCollectorExpression collectorExpression = collectorExpressions.get(i);
                collectorExpression.setNextReader(context);
            }
            return this;
        }

        @Override
        public Object value(int slot) {
            return values[slot];
        }
    }
}
