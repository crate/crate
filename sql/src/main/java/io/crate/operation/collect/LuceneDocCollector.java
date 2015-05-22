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

package io.crate.operation.collect;

import com.google.common.base.Throwables;
import io.crate.Constants;
import io.crate.action.sql.query.CrateSearchContext;
import io.crate.action.sql.query.LuceneSortGenerator;
import io.crate.analyze.OrderBy;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.lucene.QueryBuilderHelper;
import io.crate.metadata.Functions;
import io.crate.operation.*;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.operation.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CancellationException;

/**
 * collect documents from ES shard, a lucene index
 */
public class LuceneDocCollector extends Collector implements CrateCollector, RowUpstream {

    public static class CollectorFieldsVisitor extends FieldsVisitor {

        final HashSet<String> requiredFields;
        private boolean required = false;

        public CollectorFieldsVisitor(int size) {
            requiredFields = new HashSet<>(size);
        }

        public boolean addField(String name) {
            required = true;
            return requiredFields.add(name);
        }

        public boolean required() {
            return required;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
                return Status.YES;
            }
            return requiredFields.contains(fieldInfo.name) ? Status.YES : Status.NO;
        }

        public void required(boolean required) {
            this.required = required;
        }
    }

    private CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor;

    private final RowDownstreamHandle downstream;
    private final CollectorFieldsVisitor fieldsVisitor;
    private final InputRow inputRow;
    private final List<LuceneCollectorExpression<?>> collectorExpressions;
    private final JobCollectContext jobCollectContext;
    private final CrateSearchContext searchContext;
    private final int jobSearchContextId;
    private final boolean keepContextForFetcher;
    private final List<OrderByCollectorExpression> orderByCollectorExpressions = new ArrayList<>();
    private final Integer limit;
    private final OrderBy orderBy;

    private boolean visitorEnabled = false;
    private AtomicReader currentReader;
    private RamAccountingContext ramAccountingContext;
    private boolean producedRows = false;
    private Scorer scorer;
    private int rowCount = 0;
    private int pageSize;

    public LuceneDocCollector(List<Input<?>> inputs,
                              List<LuceneCollectorExpression<?>> collectorExpressions,
                              CollectNode collectNode,
                              Functions functions,
                              RowDownstream downStreamProjector,
                              JobCollectContext jobCollectContext,
                              CrateSearchContext searchContext,
                              int jobSearchContextId,
                              boolean keepContextForFetcher) throws Exception {
        this.limit = collectNode.limit();
        this.orderBy = collectNode.orderBy();
        this.downstream = downStreamProjector.registerUpstream(this);
        this.inputRow = new InputRow(inputs);
        this.collectorExpressions = collectorExpressions;
        for (LuceneCollectorExpression expr : collectorExpressions) {
            if ( expr instanceof OrderByCollectorExpression) {
                orderByCollectorExpressions.add((OrderByCollectorExpression)expr);
            }
        }
        this.fieldsVisitor = new CollectorFieldsVisitor(collectorExpressions.size());
        this.jobSearchContextId = jobSearchContextId;
        this.jobCollectContext = jobCollectContext;
        this.searchContext = searchContext;
        this.keepContextForFetcher = keepContextForFetcher;
        inputSymbolVisitor = new CollectInputSymbolVisitor<>(functions, new LuceneDocLevelReferenceResolver(null));
        this.pageSize = Constants.PAGE_SIZE;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
        for (LuceneCollectorExpression expr : collectorExpressions) {
            expr.setScorer(scorer);
        }
    }

    @Override
    public void collect(int doc) throws IOException {
        Collectors.cancelIfInterrupted();

        rowCount++;
        if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
            // stop collecting because breaker limit was reached
            throw new UnexpectedCollectionTerminatedException(
                    CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                            ramAccountingContext.limit()));
        }
        // validate minimum score
        if (searchContext.minimumScore() != null
                && scorer.score() < searchContext.minimumScore()) {
            return;
        }

        producedRows = true;
        if (visitorEnabled) {
            fieldsVisitor.reset();
            currentReader.document(doc, fieldsVisitor);
        }
        for (LuceneCollectorExpression e : collectorExpressions) {
            e.setNextDocId(doc);
        }
        boolean wantMore;
        try {
            wantMore = downstream.setNextRow(inputRow);
        } catch (Throwable t) {
            throw new CollectionAbortedException(
                    Collectors.gotInterrupted(t) ? new CancellationException() : t);
        }
        if (!wantMore || (limit != null && rowCount == limit)) {
            // no more rows required, we can stop here
            throw new CollectionAbortedException();
        }

    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.currentReader = context.reader();
        for (LuceneCollectorExpression expr : collectorExpressions) {
            expr.setNextReader(context);
        }
    }

    public void setNextOrderByValues(ScoreDoc scoreDoc) {
        for (OrderByCollectorExpression expr : orderByCollectorExpressions) {
            expr.setNextFieldDoc((FieldDoc)scoreDoc);
        }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void doCollect(RamAccountingContext ramAccountingContext) {
        this.ramAccountingContext = ramAccountingContext;
        // start collect
        CollectorContext collectorContext = new CollectorContext()
                .searchContext(searchContext)
                .visitor(fieldsVisitor)
                .jobSearchContextId(jobSearchContextId);
        for (LuceneCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }
        visitorEnabled = fieldsVisitor.required();
        jobCollectContext.acquireContext(searchContext);
        Query query = searchContext.query();
        if (query == null) {
            query = new MatchAllDocsQuery();
        }

        // do the lucene search
        boolean failed = false;
        try {
            if( orderBy != null) {
                Integer batchSize = limit == null ? pageSize : Math.min(pageSize, limit);
                Sort sort = LuceneSortGenerator.generateLuceneSort(searchContext, orderBy, inputSymbolVisitor);
                TopFieldDocs topFieldDocs = searchContext.searcher().search(query, batchSize, sort);
                int collected = topFieldDocs.scoreDocs.length;
                ScoreDoc lastCollected = collectTopFields(topFieldDocs);
                while ((limit == null || collected < limit) && topFieldDocs.scoreDocs.length >= batchSize && lastCollected != null) {
                    Collectors.cancelIfInterrupted();
                    batchSize = limit == null ? pageSize : Math.min(pageSize, limit - collected);
                    Query alreadyCollectedQuery = alreadyCollectedQuery((FieldDoc)lastCollected);
                    if (alreadyCollectedQuery != null) {
                        BooleanQuery searchAfterQuery = new BooleanQuery();
                        searchAfterQuery.add(query, BooleanClause.Occur.MUST);
                        searchAfterQuery.add(alreadyCollectedQuery, BooleanClause.Occur.MUST_NOT);
                        topFieldDocs = (TopFieldDocs)searchContext.searcher().searchAfter(lastCollected, searchAfterQuery, batchSize, sort);
                    } else {
                        topFieldDocs = (TopFieldDocs)searchContext.searcher().searchAfter(lastCollected, query, batchSize, sort);
                    }
                    collected += topFieldDocs.scoreDocs.length;
                    lastCollected = collectTopFields(topFieldDocs);
                }
            } else {
                searchContext.searcher().search(query, this);
            }
            downstream.finish();
        } catch (CollectionAbortedException e) {
            if (e.getCause() == null) {
                // ok, we stopped lucene from searching unnecessary leaf readers
                downstream.finish();
            } else {
                // got a reason for abortion, hand it over
                failed = true;
                downstream.fail(Throwables.getRootCause(e));
            }
        } catch (Exception e) {
            failed = true;
            downstream.fail(Collectors.gotInterrupted(e) ? new CancellationException() : e);
        } finally {
            jobCollectContext.releaseContext(searchContext);
            if (!keepContextForFetcher || !producedRows || failed) {
                jobCollectContext.closeContext(jobSearchContextId);
            }
        }
    }

    public CrateSearchContext searchContext() {
        return searchContext;
    }

    public void pageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    private ScoreDoc collectTopFields(TopFieldDocs topFieldDocs) throws IOException{
        IndexReaderContext indexReaderContext = searchContext.searcher().getTopReaderContext();
        ScoreDoc lastDoc = null;
        if(!indexReaderContext.leaves().isEmpty()) {
            for (ScoreDoc scoreDoc : topFieldDocs.scoreDocs) {
                int readerIndex = ReaderUtil.subIndex(scoreDoc.doc, searchContext.searcher().getIndexReader().leaves());
                AtomicReaderContext subReaderContext = searchContext.searcher().getIndexReader().leaves().get(readerIndex);
                int subDoc = scoreDoc.doc - subReaderContext.docBase;
                setNextReader(subReaderContext);
                setNextOrderByValues(scoreDoc);
                collect(subDoc);
                lastDoc = scoreDoc;
            }
        }
        return lastDoc;
    }

    private @Nullable Query alreadyCollectedQuery(FieldDoc lastCollected) {
       BooleanQuery query = new BooleanQuery();
       for (int i = 0; i < orderBy.orderBySymbols().size(); i++) {
            Symbol order = orderBy.orderBySymbols().get(i);
            Object value = lastCollected.fields[i];
            // only filter for null values if nulls last
            if (order instanceof Reference && (value != null || !orderBy.nullsFirst()[i])) {
                QueryBuilderHelper helper = QueryBuilderHelper.forType(order.valueType());
                String columnName = ((Reference)order).info().ident().columnIdent().fqn();
                if (orderBy.reverseFlags()[i]) {
                    query.add(helper.rangeQuery(columnName, value, null, false, false ), BooleanClause.Occur.MUST);
                } else {
                    query.add(helper.rangeQuery(columnName, null, value, false, false ), BooleanClause.Occur.MUST);
                }
            }
        }
        if (query.clauses().size() > 0) {
            return query;
        } else {
            return null;
        }
    }
}
