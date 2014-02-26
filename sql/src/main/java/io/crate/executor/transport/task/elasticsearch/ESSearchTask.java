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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.elasticsearch.ESQueryBuilder;
import io.crate.executor.Task;
import io.crate.planner.node.ESSearchNode;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;
import org.cratedb.sql.ExceptionHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import java.io.IOException;
import java.util.*;

public class ESSearchTask implements Task<Object[][]> {

    private final ESSearchNode searchNode;
    private final TransportSearchAction transportSearchAction;
    private final SettableFuture<Object[][]> result;
    private final List<ListenableFuture<Object[][]>> results;
    private final ESQueryBuilder queryBuilder;
    private final Visitor visitor = new Visitor();

    public ESSearchTask(ESSearchNode searchNode,
                        TransportSearchAction transportSearchAction) {
        this.searchNode = searchNode;
        this.transportSearchAction = transportSearchAction;
        this.queryBuilder = new ESQueryBuilder();

        result = SettableFuture.create();
        results = Arrays.<ListenableFuture<Object[][]>>asList(result);
    }

    @Override
    public void start() {
        final Context ctx = new Context();
        final SearchRequest request = new SearchRequest();

        for (Symbol symbol : searchNode.outputs()) {
            visitor.process(symbol, ctx);
        }

        final FieldExtractor[] extractor = buildExtractor(ctx.outputs);
        final int numColumns = ctx.outputs.size();

        try {
            request.source(queryBuilder.convert(searchNode, ctx.outputs), false);
            request.indices(ctx.indices.toArray(new String[ctx.indices.size()]));

            transportSearchAction.execute(request, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    if (searchResponse.getFailedShards() > 0) {
                        try {
                            ExceptionHelper.exceptionOnSearchShardFailures(searchResponse.getShardFailures());
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    } else {
                        final SearchHit[] hits = searchResponse.getHits().getHits();
                        final Object[][] rows = new Object[hits.length][ctx.outputs.size()];

                        for (int r = 0; r < hits.length; r++) {
                            rows[r] = new Object[numColumns];
                            for (int c = 0; c < numColumns; c++) {
                                rows[r][c] = extractor[c].extract(hits[r]);
                            }
                        }

                        result.set(rows);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    result.setException(e);
                }
            });
        } catch (IOException e) {
            result.setException(e);
        }
    }

    private FieldExtractor[] buildExtractor(final List<Reference> outputs) {
        FieldExtractor[] extractors = new FieldExtractor[outputs.size()];
        int i = 0;
        for (Reference output : outputs) {
            final String fieldName = output.info().ident().columnIdent().fqn();
            if (fieldName.equals("_version")) {
                extractors[i] = new FieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return hit.getVersion();
                    }
                };
            } else if (fieldName.equals("_id")) {
                extractors[i] = new FieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return new BytesRef(hit.getId());
                    }
                };
            } else if (fieldName.equals("_source")) {
                extractors[i] = new FieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return hit.getSource();
                    }
                };
            } else if (fieldName.equals("_score")) {
                extractors[i] = new FieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return hit.getScore();
                    }
                };
            } else {
                extractors[i] = new FieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        SearchHitField field = null;
                        Object value = hit.getSource().get(fieldName);
                        return value;
                    }
                };
            }
            i++;
        }

        return extractors;
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        throw new UnsupportedOperationException("Can't have upstreamResults");
    }

    class Context {
        final public List<Reference> outputs = new ArrayList<>();
        final public Set<String> indices = new HashSet<>();
    }

    static class Visitor extends SymbolVisitor<Context, Void> {

        private void addReference(Reference symbol, Context context) {
            context.outputs.add(symbol);
            context.indices.add(symbol.info().ident().tableIdent().name());
        }

        @Override
        public Void visitReference(Reference symbol, Context context) {
            addReference(symbol, context);
            return null;
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, Context context) {
            addReference(symbol, context);
            return null;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(SymbolFormatter.format("Symbol %s not supported", symbol));
        }
    }

    private interface FieldExtractor {
        Object extract(SearchHit hit);
    }
}
