/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.planner.node.ESDeleteByQueryNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolVisitor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;

import java.io.IOException;
import java.util.*;

public class ESDeleteByQueryTask implements Task<Object[][]> {

    private final ESDeleteByQueryNode deleteByQueryNode;
    private final TransportDeleteByQueryAction transportDeleteByQueryAction;
    private final SettableFuture<Object[][]> result;
    private final List<ListenableFuture<Object[][]>> results;
    private final ESQueryBuilder queryBuilder;
    private final Visitor visitor = new Visitor();

    public ESDeleteByQueryTask(ESDeleteByQueryNode deleteByQueryNode,
                               TransportDeleteByQueryAction transportDeleteByQueryAction,
                               Functions functions,
                               ReferenceResolver referenceResolver) {
        this.deleteByQueryNode = deleteByQueryNode;
        this.transportDeleteByQueryAction = transportDeleteByQueryAction;
        this.queryBuilder = new ESQueryBuilder(functions, referenceResolver);

        result = SettableFuture.create();
        results = Arrays.<ListenableFuture<Object[][]>>asList(result);
    }

    @Override
    public void start() {
        final Context ctx = new Context();
        final DeleteByQueryRequest request = new DeleteByQueryRequest();

        try {
            request.query(queryBuilder.convert(deleteByQueryNode), false);
            request.indices(deleteByQueryNode.indices().toArray(new String[ctx.indices.size()]));

            transportDeleteByQueryAction.execute(request, new ActionListener<DeleteByQueryResponse>() {
                @Override
                public void onResponse(DeleteByQueryResponse deleteByQueryResponses) {
                    result.set(new Object[][] { new Object[] { -1L }});
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

        @Override
        public Void visitReference(Reference symbol, Context context) {
            context.outputs.add(symbol);
            context.indices.add(symbol.info().ident().tableIdent().name());
            return null;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(String.format("Symbol %s not supported", symbol));
        }
    }
}
