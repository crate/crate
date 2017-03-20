/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.projectors;

import com.carrotsearch.hppc.IntArrayList;
import io.crate.action.FutureActionListener;
import io.crate.action.LimitedExponentialBackoff;
import io.crate.data.BatchAccumulator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.ShardRequest;
import io.crate.executor.transport.ShardResponse;
import io.crate.operation.collect.CollectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;

import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class ShardRequestAccumulator<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item>
    implements BatchAccumulator<Row, Iterator<? extends Row>> {

    private static final BackoffPolicy BACKOFF_POLICY = LimitedExponentialBackoff.limitedExponential(1000);

    private final int batchSize;
    private final ScheduledExecutorService scheduler;
    private final CollectExpression<Row, ?> uidExpression;
    private final Supplier<TReq> requestFactory;
    private final Function<String, TItem> itemFactory;
    private final BitSet responses;
    private final BiConsumer<TReq, ActionListener<ShardResponse>> transportAction;

    private TReq currentRequest;
    private int numItems = -1;

    public ShardRequestAccumulator(int batchSize,
                                   ScheduledExecutorService scheduler,
                                   CollectExpression<Row, ?> uidExpression,
                                   Supplier<TReq> requestFactory,
                                   Function<String, TItem> itemFactory,
                                   BiConsumer<TReq, ActionListener<ShardResponse>> transportAction) {
        this.batchSize = batchSize;
        this.scheduler = scheduler;
        this.uidExpression = uidExpression;
        this.requestFactory = requestFactory;
        this.itemFactory = itemFactory;
        this.transportAction = transportAction;
        currentRequest = requestFactory.get();
        responses = new BitSet();
    }

    @Override
    public void onItem(Row row) {
        numItems++;
        uidExpression.setNextRow(row);
        currentRequest.add(numItems, itemFactory.apply(((BytesRef) uidExpression.value()).utf8ToString()));
    }

    @Override
    public int batchSize() {
        return batchSize;
    }

    @Override
    public CompletableFuture<Iterator<? extends Row>> processBatch(boolean isLastBatch) {
        if (currentRequest.itemIndices().isEmpty() && isLastBatch) {
            return CompletableFuture.completedFuture(getSingleRowWithRowCount());
        }
        FutureActionListener<ShardResponse, Iterator<? extends Row>> listener = new FutureActionListener<>(r -> {
            currentRequest = requestFactory.get();
            return responseToRowIt(isLastBatch, r);
        });
        transportAction.accept(
            currentRequest,
            new RetryListener<>(scheduler,
                (actionListener) -> transportAction.accept(currentRequest, actionListener),
                listener,
                BACKOFF_POLICY
            )
        );
        return listener;
    }

    private Iterator<Row> responseToRowIt(boolean isLastBatch, ShardResponse response) {
        Throwable throwable = response.failure();
        if (throwable == null) {
            setSuccessResponsesAndSingleFailures(response.itemIndices(), response.failures());
        } else {
            Exceptions.rethrowUnchecked(throwable);
        }
        if (isLastBatch) {
            return getSingleRowWithRowCount();
        }
        return Collections.emptyIterator();

    }

    private Iterator<Row> getSingleRowWithRowCount() {
        return Collections.<Row>singletonList(new Row1((long) responses.cardinality())).iterator();
    }

    private void setSuccessResponsesAndSingleFailures(IntArrayList itemIndices, List<ShardResponse.Failure> failures) {
        for (int i = 0; i < itemIndices.size(); i++) {
            int location = itemIndices.get(i);
            ShardResponse.Failure failure = failures.get(i);
            if (failure == null) {
                responses.set(location, true);
            } else {
                responses.set(location, false);
            }
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void reset() {
        currentRequest = requestFactory.get();
    }
}
