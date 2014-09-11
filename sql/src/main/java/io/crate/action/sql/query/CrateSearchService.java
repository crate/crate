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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.Constants;
import io.crate.executor.transport.task.elasticsearch.ESQueryBuilder;
import io.crate.executor.transport.task.elasticsearch.SortOrder;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.planner.node.dql.ESSearchNode;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.InternalSearchService;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.SortParseElement;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CrateSearchService extends InternalSearchService {

    private static final ESQueryBuilder ESQueryBuilder = new ESQueryBuilder();
    private final SortSymbolVisitor sortSymbolVisitor;

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
                              Functions functions) {
        super(settings, clusterService, indicesService, indicesLifecycle,
                indicesWarmer,
                threadPool,
                scriptService,
                cacheRecycler, pageCacheRecycler, bigArrays, dfsPhase, queryPhase, fetchPhase);
        CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor =
                new CollectInputSymbolVisitor<>(functions, LuceneDocLevelReferenceResolver.INSTANCE);
        sortSymbolVisitor = new SortSymbolVisitor(inputSymbolVisitor);
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
            freeContext(context);
            throw ExceptionsHelper.convertToRuntime(e);
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
                freeContext(context);
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
     * <p>
     * Note: Scrolling isn't supported.
     * </p>
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

        ShardSearchRequest shardSearchRequest = new ShardSearchRequest();
        shardSearchRequest.types(new String[] {Constants.DEFAULT_MAPPING_TYPE });

        // TODO: use own CrateSearchContext that doesn't require ShardSearchRequest
        SearchContext context = new DefaultSearchContext(
                idGenerator.incrementAndGet(),
                shardSearchRequest,
                searchShardTarget,
                engineSearcher,
                indexService,
                indexShard,
                scriptService,
                cacheRecycler,
                pageCacheRecycler,
                bigArrays
        );
        SearchContext.setCurrent(context);

        try {
            ESSearchNode searchNode = new ESSearchNode(
                    new Routing(null),
                    request.outputs(),
                     // can omit sort because it's added below using generateLuceneSort
                    ImmutableList.<Symbol>of(),
                    new boolean[0],
                    new Boolean[0],
                    request.limit(),
                    request.offset(),
                    request.whereClause(),
                    request.partitionBy()
            );

            // TODO: remove xcontent
            BytesReference source = ESQueryBuilder.convert(searchNode);
            parseSource(context, source);

            context.sort(generateLuceneSort(
                    context, request.orderBy(), request.reverseFlags(), request.nullsFirst()));

            // if the from and size are still not set, default them
            if (context.from() == -1) {
                context.from(0);
            }
            if (context.size() == -1) {
                context.size(10);
            }

            // pre process
            dfsPhase.preProcess(context);
            queryPhase.preProcess(context);
            fetchPhase.preProcess(context);

            // compute the context keep alive
            long keepAlive = defaultKeepAlive;
            context.keepAlive(keepAlive);
        } catch (Throwable e) {
            context.close();
            throw ExceptionsHelper.convertToRuntime(e);
        }
        return context;
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
                    searchContext.fieldData().getForField(fieldMapper).comparatorSource(sortOrder.missing(), sortMode);

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
            final SortOrder sortOrder = new SortOrder(context.reverseFlag, context.nullFirst);
            assert type != null : "Could not get lucene sort type for " + function.valueType();

            return new SortField(function.toString(), new IndexFieldData.XFieldComparatorSource() {
                @Override
                public FieldComparator<?> newComparator(String fieldName, int numHits, int sortPos, boolean reversed) throws IOException {
                    return new InputFieldComparator(
                            numHits,
                            context.context,
                            expressions,
                            functionInput,
                            function.valueType(),
                            missingObject(sortOrder.missing(), reversed)
                    );
                }

                @Override
                public SortField.Type reducedType() {
                    return type;
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
            values[slot] = Objects.firstNonNull(input.value(), missingValue);
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
