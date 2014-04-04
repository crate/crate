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

package io.crate.operation.projectors;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.Id;
import io.crate.exceptions.CrateException;
import io.crate.operation.Input;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class IndexWriterProjector implements Projector {

    private final BulkProcessor bulkProcessor;
    private final Listener listener;
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final CollectExpression<?>[] collectExpressions;
    private final List<Input<?>> idInputs;
    private final Input<?> sourceInput;
    private final Input<?> routingInput;
    private final String tableName;
    private final Object lock = new Object();
    private final List<String> primaryKeys;
    private Projector downstream;

    public IndexWriterProjector(Client client,
                                String tableName,
                                List<String> primaryKeys,
                                List<Input<?>> idInputs,
                                Input<?> routingInput,
                                Input<?> sourceInput,
                                CollectExpression<?>[] collectExpressions,
                                @Nullable Integer bulkActions,
                                @Nullable Integer concurrency) {
        listener = new Listener();
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.collectExpressions = collectExpressions;
        this.idInputs = idInputs;
        this.routingInput = routingInput;
        this.sourceInput = sourceInput;
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener);
        if (bulkActions != null) {
            builder.setBulkActions(bulkActions);
        }
        if (concurrency != null) {
            builder.setConcurrentRequests(concurrency);
        }
        bulkProcessor = builder.build();
    }

    @Override
    public void startProjection() {
        listener.allRowsAdded.set(false);
    }

    @Override
    public boolean setNextRow(Object... row) {
        IndexRequest indexRequest;
        synchronized (lock) {
            for (CollectExpression<?> collectExpression : collectExpressions) {
                collectExpression.setNextRow(row);
            }
            indexRequest = buildRequest();
        }
        if (indexRequest != null) {
            bulkProcessor.add(indexRequest);
        }
        return true;
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        remainingUpstreams.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            bulkProcessor.close();
            listener.allRowsAdded.set(true);
            if (listener.inProgress.get() == 0) {
                downstream.setNextRow(listener.rowsImported.get());
                downstream.upstreamFinished();
            }
        }
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            bulkProcessor.close();
            if (downstream != null) {
                downstream.setNextRow(listener.rowsImported.get());
                downstream.upstreamFailed(throwable);
            }
            return;
        }
        listener.failure.set(throwable);
    }

    private IndexRequest buildRequest() {
        // TODO: reuse logic that is currently  in AbstractESIndexTask
        IndexRequest indexRequest = new IndexRequest(tableName, Constants.DEFAULT_MAPPING_TYPE);
        if (sourceInput.value() != null) {
            indexRequest.source(((BytesRef) sourceInput.value()).bytes);
        } else {
            return null;
        }

        List<String> primaryKeyValues = Lists.transform(idInputs, new Function<Input<?>, String>() {
            @Nullable
            @Override
            public String apply(Input<?> input) {
                return input.value().toString();
            }
        });

        Object routing = routingInput.value();
        String clusteredBy = null;
        if (routing != null) {
            clusteredBy = routing.toString();
            indexRequest.routing(clusteredBy);
        }
        Id id = new Id(primaryKeys, primaryKeyValues, clusteredBy, true);
        indexRequest.id(id.stringValue());
        return indexRequest;
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        this.listener.downstream(downstream);
    }

    @Override
    public Projector downstream() {
        return downstream;
    }

    private class Listener implements BulkProcessor.Listener {
        AtomicInteger inProgress = new AtomicInteger(0);
        final AtomicBoolean allRowsAdded;
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicLong rowsImported = new AtomicLong(0);
        Projector downstream;

        Listener() {
            allRowsAdded = new AtomicBoolean(false);
        }

        void downstream(Projector downstream) {
            this.downstream = downstream;
        }

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            inProgress.incrementAndGet();
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                for (BulkItemResponse item : response.getItems()) {
                    if (!item.isFailed()) {
                        rowsImported.incrementAndGet();
                    } else {
                        failure.set(new CrateException(item.getFailureMessage()));
                    }
                }
            } else {
                rowsImported.addAndGet(response.getItems().length);
            }

            if (inProgress.decrementAndGet() == 0 && allRowsAdded.get() && downstream != null) {
                Throwable throwable = failure.get();
                if (throwable != null) {
                    downstream.upstreamFailed(throwable);
                } else {
                    downstream.setNextRow(rowsImported.get());
                    downstream.upstreamFinished();
                }
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            this.failure.set(failure);
            if (inProgress.decrementAndGet() == 0 && allRowsAdded.get() && downstream != null) {
                downstream.setNextRow(rowsImported.get());
                downstream.upstreamFailed(failure);
            }
        }
    }
}
