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
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.executor.Task;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.Input;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.ESIndexNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.cratedb.Constants;
import org.cratedb.sql.CrateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;

import java.util.*;

public class ESIndexTask implements Task<Object[][]> {

    private final TransportIndexAction transport;
    private final IndexRequest request;
    private final List<ListenableFuture<Object[][]>> results;
    private final ActionListener<IndexResponse> listener;
    private final ESIndexNode node;
    private final EvaluatingNormalizer normalizer;

    static class IndexResponseListener implements ActionListener<IndexResponse> {
        public static Object[][] affectedRowsResult = new Object[][]{new Object[]{1}};
        private final SettableFuture<Object[][]> result;

        IndexResponseListener(SettableFuture<Object[][]> result) {
            this.result = result;
        }

        @Override
        public void onResponse(IndexResponse indexResponse) {
            result.set(affectedRowsResult);
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    /**
     * @param transport         the transportAction to run the actual ES operation on
     * @param node              the plan node
     * @param functions
     * @param referenceResolver
     */
    public ESIndexTask(TransportIndexAction transport,
                       ESIndexNode node,
                       Functions functions,
                       ReferenceResolver referenceResolver) {
        this.transport = transport;
        this.node = node;
        this.normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);

        final SettableFuture<Object[][]> result = SettableFuture.create();
        results = Arrays.<ListenableFuture<Object[][]>>asList(result);
        request = buildRequest();
        listener = new IndexResponseListener(result);

    }

    @Override
    public void start() {
        transport.execute(request, listener);
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        throw new UnsupportedOperationException();
    }

    private IndexRequest buildRequest() {
        IndexRequest request = new IndexRequest(node.index(), Constants.DEFAULT_MAPPING_TYPE);
        request.create(true);

        Map<String, Object> sourceMap = new HashMap<>();
        List<Symbol> values = node.valuesLists().get(0);
        Iterator<Symbol> valuesIt = values.iterator();


        int primaryKeyIdx = -1;
        if (node.hasPrimaryKey()) {
            primaryKeyIdx = node.primaryKeyIndices()[0];
        }
        for (int i = 0, length = node.columns().size(); i < length; i++) {
            Reference column = node.columns().get(i);
            Symbol v = valuesIt.next();
            try {
                Object value = ((Input) normalizer.process(v, null)).value();
                // TODO: handle this conversion in XContent
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                sourceMap.put(
                        column.info().ident().columnIdent().name(),
                        value
                );
                if (i == primaryKeyIdx) {
                    request.id(value.toString());
                }
            } catch (ClassCastException e) {
                // symbol is no input
                throw new CrateException(String.format("invalid value '%s' in insert statement", v.toString()));
            }
        }
        request.source(sourceMap);
        return request;
    }
}
