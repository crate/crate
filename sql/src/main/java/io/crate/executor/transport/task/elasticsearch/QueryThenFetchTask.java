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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.*;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.qtf.QueryThenFetchOperation;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.InternalSearchResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;

public class QueryThenFetchTask extends JobTask implements PageableTask {

    private static final SymbolToFieldExtractor<SearchHit> SYMBOL_TO_FIELD_EXTRACTOR =
            new SymbolToFieldExtractor<>(new SearchHitFieldExtractorFactory());

    private final ESLogger logger = Loggers.getLogger(this.getClass());

    private final QueryThenFetchNode searchNode;
    private final QueryThenFetchOperation operation;

    private SettableFuture<TaskResult> result;
    private List<ListenableFuture<TaskResult>> results;

    private final List<FieldExtractor<SearchHit>> extractors;

    private List<Reference> references;

    public QueryThenFetchTask(UUID jobId,
                              QueryThenFetchOperation operation,
                              Functions functions,
                              QueryThenFetchNode searchNode) {
        super(jobId);
        this.operation = operation;
        this.searchNode = searchNode;

        SearchHitExtractorContext context = new SearchHitExtractorContext(functions, searchNode.outputs().size(), searchNode.partitionBy());
        extractors = new ArrayList<>(searchNode.outputs().size());
        for (Symbol symbol : searchNode.outputs()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, context));
        }
        references = context.references();

        result = SettableFuture.create();
        results = Arrays.<ListenableFuture<TaskResult>>asList(result);
    }

    @Override
    public void start() {
        doStart(Optional.<PageInfo>absent());
    }

    @Override
    public void start(PageInfo pageInfo) {
        doStart(Optional.of(pageInfo));
    }

    private void doStart(final Optional<PageInfo> pageInfo) {
        Futures.addCallback(
            operation.execute(searchNode, references, pageInfo),
            new FutureCallback<QueryThenFetchOperation.QueryThenFetchContext>() {
                @Override
                public void onSuccess(@Nullable final QueryThenFetchOperation.QueryThenFetchContext context) {
                    Futures.addCallback(context.createSearchResponse(), new FutureCallback<InternalSearchResponse>() {
                        @Override
                        public void onSuccess(@Nullable InternalSearchResponse searchResponse) {
                            try {
                                if (pageInfo.isPresent()) {
                                    ObjectArray<Object[]> pageSource = context.toPage(searchResponse.hits().hits(), extractors);
                                    context.cleanAfterFirstPage();
                                    result.set(new QueryThenFetchPageableTaskResult(operation, context, extractors, pageInfo.get(), pageSource, 0L));
                                } else {
                                    Object[][] rows = context.toRows(searchResponse.hits().hits(), extractors);
                                    context.close();
                                    result.set(new QueryResult(rows));
                                }
                            } catch (Throwable t) {
                                onFailure(t);
                            }
                        }

                        @Override
                        public void onFailure(@Nonnull Throwable t) {
                            try {
                                context.close();
                                logger.error("error creating a QueryThenFetch response", t);
                                result.setException(t);
                            } catch (IOException e) {
                                // ignore in this case
                                logger.error("error closing QueryThenFetch context", t);
                                result.setException(e);
                            }

                        }
                    });
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("error executing a QueryThenFetch query", t);
                    result.setException(t);
                }
            }
        );
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("Can't have upstreamResults");
    }

    static class SearchHitExtractorContext extends SymbolToFieldExtractor.Context {
        private final List<ReferenceInfo> partitionBy;

        public SearchHitExtractorContext(Functions functions, int size, List<ReferenceInfo> partitionBy) {
            super(functions, size);
            this.partitionBy = partitionBy;
        }

        @Override
        public Object inputValueFor(InputColumn inputColumn) {
            throw new AssertionError("SearchHitExtractorContext does not support resolving InputColumn");
        }
    }

    static class SearchHitFieldExtractorFactory implements FieldExtractorFactory<SearchHit, SearchHitExtractorContext> {

        @Override
        public FieldExtractor<SearchHit> build(Reference field, SearchHitExtractorContext context) {
            final ColumnIdent columnIdent = field.info().ident().columnIdent();
            if (columnIdent.isSystemColumn()) {
                if (DocSysColumns.VERSION.equals(columnIdent)) {
                    return new ESFieldExtractor() {
                        @Override
                        public Object extract(SearchHit hit) {
                            return hit.getVersion();
                        }
                    };
                } else if (DocSysColumns.ID.equals(columnIdent)) {
                    return new ESFieldExtractor() {
                        @Override
                        public Object extract(SearchHit hit) {
                            return new BytesRef(hit.getId());
                        }
                    };
                } else if (DocSysColumns.DOC.equals(columnIdent)) {
                    return new ESFieldExtractor() {
                        @Override
                        public Object extract(SearchHit hit) {
                            return hit.getSource();
                        }
                    };
                } else if (DocSysColumns.RAW.equals(columnIdent)) {
                    return new ESFieldExtractor() {
                        @Override
                        public Object extract(SearchHit hit) {
                            return hit.getSourceRef().toBytesRef();
                        }
                    };
                } else if (DocSysColumns.SCORE.equals(columnIdent)) {
                    return new ESFieldExtractor() {
                        @Override
                        public Object extract(SearchHit hit) {
                            return hit.getScore();
                        }
                    };
                } else {
                    throw new UnsupportedOperationException(
                            String.format(Locale.ENGLISH, "Unsupported system column %s", columnIdent.name()));
                }
            } else if (context.partitionBy.contains(field.info())) {
                return new ESFieldExtractor.PartitionedByColumnExtractor(field, context.partitionBy);
            } else {
                return new ESFieldExtractor.Source(columnIdent, field.valueType());
            }
        }
    }

}
