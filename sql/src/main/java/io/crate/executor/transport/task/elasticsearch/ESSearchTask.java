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
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.aggregation.impl.CountAggregation;
import io.crate.planner.node.ESSearchNode;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolVisitor;
import org.cratedb.action.sql.SQLFields;
import org.cratedb.sql.ExceptionHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.count.TransportCountAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.*;

public class ESSearchTask implements Task<Object[][]> {

    private final ESSearchNode searchNode;
    private final TransportSearchAction transportSearchAction;
    private final SettableFuture<Object[][]> result;
    private final List<ListenableFuture<Object[][]>> results;
    private final ESQueryBuilder queryBuilder;
    private final Visitor visitor = new Visitor();
    private final TransportCountAction transportCountAction;

    public ESSearchTask(ESSearchNode searchNode,
                        TransportSearchAction transportSearchAction,
                        TransportCountAction transportCountAction,
                        Functions functions,
                        ReferenceResolver referenceResolver) {
        this.searchNode = searchNode;
        this.transportSearchAction = transportSearchAction;
        this.transportCountAction = transportCountAction;
        this.queryBuilder = new ESQueryBuilder(functions, referenceResolver);

        result = SettableFuture.create();
        results = Arrays.<ListenableFuture<Object[][]>>asList(result);
    }

    @Override
    public void start() {
        final Context ctx = new Context();
        for (Symbol symbol : searchNode.outputs()) {
            visitor.process(symbol, ctx);
        }

        if (ctx.isCountRequest) {
            doCountRequest(ctx);
        } else {
            doSearchRequest(ctx);
        }
    }

    private void doSearchRequest(final Context ctx) {
        final SearchRequest request = new SearchRequest();
        final SQLFields fields = new SQLFields(ctx.fields);

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

                        for (int i = 0; i < hits.length; i++) {
                            fields.hit(hits[i]);
                            rows[i] = fields.getRowValues();
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

    private void doCountRequest(final Context ctx) {
        Preconditions.checkArgument(ctx.outputs.isEmpty()); // ESSearchTask supports only global count
        CountRequest request = new CountRequest(ctx.indices());

        transportCountAction.execute(request, new ActionListener<CountResponse>() {
            @Override
            public void onResponse(CountResponse countResponse) {
                final Object[][] rows = new Object[1][1];
                rows[0][0] = countResponse.getCount();
                result.set(rows);
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });
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
        public boolean isCountRequest = false;

        final public List<Reference> outputs = new ArrayList<>();
        final public Set<String> indices = new HashSet<>();

        // TODO: these are currently only here so that SQLFields can be re-used
        // to generate the Object[][] result from the SearchResponse
        // remove and adjust SQLFields once the akiban stuff is removed.
        final public List<Tuple<String, String>> fields = new ArrayList<>();

        public String[] indices() {
            return indices.toArray(new String[indices.size()]);
        }
    }

    static class Visitor extends SymbolVisitor<Context, Void> {

        @Override
        public Void visitReference(Reference symbol, Context context) {
            final String columnName = symbol.info().ident().fqDottedColumnName();
            context.fields.add(new Tuple<>(columnName, columnName));
            context.outputs.add(symbol);
            context.indices.add(symbol.info().ident().tableIdent().name());
            return null;
        }

        @Override
        public Void visitAggregation(Aggregation symbol, Context context) {
            if (symbol.functionIdent().name().equals(CountAggregation.NAME)) {
                Preconditions.checkArgument(symbol.inputs().isEmpty()); // only global count works.
                context.isCountRequest = true;
                return null;
            }
            throw new UnsupportedOperationException(String.format("Symbol %s not supported", symbol));
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(String.format("Symbol %s not supported", symbol));
        }
    }
}
