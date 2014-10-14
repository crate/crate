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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.types.DataType;
import io.crate.PartitionName;
import io.crate.executor.Task;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.node.dql.ESGetNode;
import io.crate.planner.symbol.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.util.*;

public class ESGetTask extends Task<QueryResult> {

    private final static Visitor visitor = new Visitor();
    private final ESGetNode node;
    private final List<ListenableFuture<QueryResult>> results;
    private final TransportAction transportAction;
    private final ActionRequest request;
    private final ActionListener listener;

    private final Map<String, Object> partitionValues;

    public ESGetTask(UUID jobId,
                     TransportMultiGetAction multiGetAction,
                     TransportGetAction getAction,
                     ESGetNode node) {
        super(jobId);
        assert multiGetAction != null;
        assert getAction != null;
        assert node != null;
        assert node.ids().size() > 0;

        this.node = node;

        if (this.node.partitionBy().isEmpty()) {
            this.partitionValues = ImmutableMap.of();
        } else {
            PartitionName partitionName = PartitionName.fromStringSafe(node.index());
            int numPartitionColumns = this.node.partitionBy().size();
            this.partitionValues = new HashMap<>(numPartitionColumns);
            for (int i = 0; i<this.node.partitionBy().size(); i++) {
                ReferenceInfo info = this.node.partitionBy().get(i);
                this.partitionValues.put(
                        info.ident().columnIdent().fqn(),
                        info.type().value(partitionName.values().get(i))
                );
            }
        }


        final Context ctx = new Context(node.outputs().size());
        for (Symbol symbol : node.outputs()) {
            visitor.process(symbol, ctx);
        }
        final FetchSourceContext fsc = new FetchSourceContext(ctx.fields);
        final FieldExtractor[] extractors = buildExtractors(ctx.fields, ctx.types);
        final SettableFuture<QueryResult> result = SettableFuture.create();
        results = Arrays.<ListenableFuture<QueryResult>>asList(result);
        if (node.ids().size() > 1) {
            MultiGetRequest multiGetRequest = new MultiGetRequest();
            for (int i = 0; i < node.ids().size(); i++) {
                String id = node.ids().get(i);
                MultiGetRequest.Item item = new MultiGetRequest.Item(node.index(), Constants.DEFAULT_MAPPING_TYPE, id);
                item.fetchSourceContext(fsc);
                item.routing(node.routingValues().get(i));
                multiGetRequest.add(item);
            }
            multiGetRequest.realtime(true);

            transportAction = multiGetAction;
            request = multiGetRequest;
            listener = new MultiGetResponseListener(result, extractors);
        } else {
            GetRequest getRequest = new GetRequest(node.index(), Constants.DEFAULT_MAPPING_TYPE, node.ids().get(0));
            getRequest.fetchSourceContext(fsc);
            getRequest.realtime(true);
            getRequest.routing(node.routingValues().get(0));

            transportAction = getAction;
            request = getRequest;
            listener = new GetResponseListener(result, extractors);
        }
    }

    private FieldExtractor[] buildExtractors(String[] fields, DataType[] types) {
        FieldExtractor[] extractors = new FieldExtractor[fields.length];
        int i = 0;
        for (final String field : fields) {
            if (field.equals("_version")) {
                extractors[i] = new FieldExtractor() {
                    @Override
                    public Object extract(GetResponse response) {
                        return response.getVersion();
                    }
                };
            } else if (field.equals("_id")) {
                extractors[i] = new FieldExtractor() {
                    @Override
                    public Object extract(GetResponse response) {
                        return response.getId();
                    }
                };
            } else if (partitionValues.containsKey(field)) {
                extractors[i] = new FieldExtractor() {
                    @Override
                    public Object extract(GetResponse response) {
                        return partitionValues.get(field);
                    }
                };
            } else {
                extractors[i] = new FieldExtractor() {
                    @Override
                    public Object extract(GetResponse response) {
                        assert response.getSourceAsMap() != null;
                        return response.getSourceAsMap().get(field);
                    }
                };
            }
            i++;
        }
        return extractors;
    }

    static class MultiGetResponseListener implements ActionListener<MultiGetResponse> {

        private final SettableFuture<QueryResult> result;
        private final FieldExtractor[] fieldExtractor;

        public MultiGetResponseListener(SettableFuture<QueryResult> result, FieldExtractor[] extractors) {
            this.result = result;
            this.fieldExtractor = extractors;
        }

        @Override
        public void onResponse(MultiGetResponse responses) {
            List<Object[]> rows = new ArrayList<>(responses.getResponses().length);
            for (MultiGetItemResponse response : responses) {
                if (response.isFailed() || !response.getResponse().isExists()) {
                    continue;
                }
                final Object[] row = new Object[fieldExtractor.length];
                int c = 0;
                for (FieldExtractor extractor : fieldExtractor) {
                    row[c] = extractor.extract(response.getResponse());
                    c++;
                }
                rows.add(row);
            }

            result.set(new QueryResult(rows.toArray(new Object[rows.size()][])));
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    static class GetResponseListener implements ActionListener<GetResponse> {

        private final SettableFuture<QueryResult> result;
        private final FieldExtractor[] extractors;

        public GetResponseListener(SettableFuture<QueryResult> result, FieldExtractor[] extractors) {
            this.result = result;
            this.extractors = extractors;
        }

        @Override
        public void onResponse(GetResponse response) {
            if (!response.isExists()) {
                result.set((QueryResult) TaskResult.EMPTY_RESULT);
                return;
            }

            final Object[][] rows = new Object[1][extractors.length];
            int c = 0;
            for (FieldExtractor extractor : extractors) {
                /**
                 * NOTE: mapping isn't applied. So if an Insert was done using the ES Rest Endpoint
                 * the data might be returned in the wrong format (date as string instead of long)
                 */
                rows[0][c] = extractor.extract(response);
                c++;
            }

            result.set(new QueryResult(rows));
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void start() {
        transportAction.execute(request, listener);
    }

    @Override
    public List<ListenableFuture<QueryResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "upstreamResult not supported on %s",
                        getClass().getSimpleName()));
    }

    static class Context {
        final DataType[] types;
        final String[] fields;
        int idx;

        Context(int size) {
            idx = 0;
            fields = new String[size];
            types = new DataType[size];
        }

        void add(Reference reference) {
            fields[idx] = reference.info().ident().columnIdent().fqn();
            types[idx] = reference.valueType();
            idx++;
        }
    }

    static class Visitor extends SymbolVisitor<Context, Void> {

        @Override
        public Void visitReference(Reference symbol, Context context) {
            context.add(symbol);
            return null;
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, Context context) {
            context.add(symbol);
            return null;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("Get operation not supported with symbol %s in the result column list", symbol));
        }
    }

    private interface FieldExtractor {
        Object extract(GetResponse response);
    }
}
