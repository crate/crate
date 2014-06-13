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

package io.crate.operation.projectors;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.PartitionName;
import io.crate.analyze.Id;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.ColumnIdent;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractIndexWriterProjector implements Projector {

    private final BulkProcessor bulkProcessor;
    private final Listener listener;
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final CollectExpression<?>[] collectExpressions;
    private final List<Input<?>> idInputs;
    private final Optional<ColumnIdent> routingIdent;
    private final Optional<Input<?>> routingInput;
    private final String tableName;
    private final Object lock = new Object();
    private final List<ColumnIdent> primaryKeys;
    private final List<Input<?>> partitionedByInputs;

    private Projector downstream;

    protected AbstractIndexWriterProjector(Client client,
                                           String tableName,
                                           List<ColumnIdent> primaryKeys,
                                           List<Input<?>> idInputs,
                                           List<Input<?>> partitionedByInputs,
                                           @Nullable ColumnIdent clusteredBy,
                                           @Nullable Input<?> routingInput,
                                           CollectExpression<?>[] collectExpressions,
                                           @Nullable Integer bulkActions,
                                           @Nullable Integer concurrency) {
        listener = new Listener();
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.collectExpressions = collectExpressions;
        this.idInputs = idInputs;
        this.routingIdent = Optional.fromNullable(clusteredBy);
        this.routingInput = Optional.<Input<?>>fromNullable(routingInput);
        this.partitionedByInputs = partitionedByInputs;
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener);
        if (bulkActions != null) {
            builder.setBulkActions(bulkActions);
        }
        if (concurrency != null) {
            builder.setConcurrentRequests(concurrency);
        }
        bulkProcessor = builder.build();

    }

    /**
     * generate the value used as source for the index request
     * @return something that can be used as argument as argument to <code>IndexRequest.source(...)</code>
     */
    protected abstract Object generateSource();

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

    private IndexRequest buildRequest() {
        // TODO: reuse logic that is currently  in AbstractESIndexTask
        IndexRequest indexRequest = new IndexRequest();

        Object value = generateSource();
        if (value == null) {
            return null;
        }
        indexRequest.type(Constants.DEFAULT_MAPPING_TYPE);

        if (partitionedByInputs.size() > 0) {
            List<String> partitionedByValues = Lists.transform(partitionedByInputs, new Function<Input<?>, String>() {
                @Nullable
                @Override
                public String apply(Input<?> input) {
                    Object value = input.value();
                    if (value == null) {
                        return null;
                    }
                    return value.toString();
                }
            });

            String partition = new PartitionName(tableName, partitionedByValues).stringValue();
            indexRequest.index(partition);

        } else {
            indexRequest.index(tableName);
        }

        if (value instanceof Map) {
            indexRequest.source((Map)value);
        } else {
            assert value instanceof BytesRef;
            indexRequest.source(((BytesRef) value).bytes);
        }

        List<String> primaryKeyValues = Lists.transform(idInputs, new Function<Input<?>, String>() {
            @Override
            public String apply(Input<?> input) {
                if (input.value() == null)
                    return null;
                return input.value().toString();
            }
        });

        String clusteredByValue = null;
        if (routingInput.isPresent()) {
            Object routing = routingInput.get().value();

            if (routing != null) {
                clusteredByValue = routing.toString();
                indexRequest.routing(clusteredByValue);
            }
        }

        Id id = new Id(primaryKeys, primaryKeyValues, routingIdent.isPresent() ? routingIdent.get() : null, true);
        indexRequest.id(id.stringValue());
        return indexRequest;
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

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        this.listener.downstream(downstream);
    }

    @Override
    public Projector downstream() {
        return downstream;
    }

    protected static class Listener implements BulkProcessor.Listener {
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
                        failure.set(new UnhandledServerException(item.getFailureMessage()));
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
