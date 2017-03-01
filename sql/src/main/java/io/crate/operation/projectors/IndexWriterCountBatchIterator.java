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

import io.crate.concurrent.CompletableFutures;
import io.crate.data.*;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.RowShardResolver;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkShardProcessor;

import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * A BatchIterator implementation which consumes the provided source by processing
 * the rows using {@link org.elasticsearch.action.bulk.BulkShardProcessor}
 * and producing the count of successful writes.
 */
public class IndexWriterCountBatchIterator implements BatchIterator {

    private final Input<BytesRef> sourceInput;
    private final RowShardResolver rowShardResolver;
    private final Supplier<String> indexNameResolver;
    private final Iterable<? extends CollectExpression<Row, ?>> collectExpressions;
    private final BulkShardProcessor<ShardUpsertRequest> bulkShardProcessor;
    private final BatchIterator source;
    private final RowColumns rowData;
    private final Row sourceRow;
    private CompletableFuture<BitSet> loading;
    private  BitSet result;
    private boolean fromStart = true;

    private IndexWriterCountBatchIterator(BatchIterator source,
                                          Supplier<String> indexNameResolver,
                                          Input<BytesRef> sourceInput,
                                          Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                          RowShardResolver rowShardResolver,
                                          BulkShardProcessor bulkShardProcessor) {
        this.indexNameResolver = indexNameResolver;
        this.collectExpressions = collectExpressions;
        this.sourceInput = sourceInput;
        this.rowShardResolver = rowShardResolver;
        this.bulkShardProcessor = bulkShardProcessor;
        this.source = source;
        this.sourceRow = RowBridging.toRow(source.rowData());
        this.rowData = new RowColumns(1);
    }

    public static BatchIterator newInstance(BatchIterator source, Supplier<String> indexNameResolver,
                                            Input<BytesRef> sourceInput,
                                            Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                            RowShardResolver rowShardResolver,
                                            BulkShardProcessor bulkShardProcessor) {
        IndexWriterCountBatchIterator delegate = new IndexWriterCountBatchIterator(source, indexNameResolver,
            sourceInput, collectExpressions, rowShardResolver, bulkShardProcessor);
        return new CloseAssertingBatchIterator(delegate);
    }

    @Override
    public Columns rowData() {
        return rowData;
    }

    @Override
    public void moveToStart() {
        raiseIfLoading();
        rowData.updateRef(RowBridging.OFF_ROW);
        fromStart = true;
    }

    @Override
    public boolean moveNext() {
        if (loading == null) {
            return false;
        }
        raiseIfLoading();

        if (fromStart) {
            long rowCount = result == null ? 0 : result.cardinality();
            rowData.updateRef(new Row1(rowCount));
            fromStart = false;
            return true;
        }

        rowData.updateRef(RowBridging.OFF_ROW);
        return false;
    }

    @Override
    public void close() {
        source.close();
        bulkShardProcessor.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (isLoading()) {
            return CompletableFutures.failedFuture(new IllegalStateException("Iterator is already loading"));
        }

        if (allLoaded()) {
            return CompletableFutures.failedFuture(new IllegalStateException("All batches already loaded"));
        }
        loading = new CompletableFuture();
        bulkShardProcessor.result().whenComplete((BitSet r, Throwable t) -> {
            if (t == null) {
                result = r;
                loading.complete(result);
            } else {
                loading.completeExceptionally(t);
            }
        });

        try {
            consumeSource();
        } catch (Exception e) {
            loading.completeExceptionally(e);
        }
        return loading;
    }

    private void consumeSource() {
        while (source.moveNext()) {
            if (!consumeRow()) {
                // the bulkShardProcessor doesn't accept more items when it fails. The loading future will complete
                // exceptionally with the exception yielded by the bulkShardProcessor result future.
                return;
            }
        }

        if (source.allLoaded()) {
            bulkShardProcessor.close();
            return;
        } else {
            source.loadNextBatch().whenComplete((r, t) -> {
                if (t == null) {
                    consumeSource();
                } else {
                    loading.completeExceptionally(t);
                }
            });
        }
    }

    private boolean consumeRow() {
        for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(sourceRow);
        }
        rowShardResolver.setNextRow(sourceRow);
        ShardUpsertRequest.Item item = new ShardUpsertRequest.Item(
            rowShardResolver.id(), null, new Object[]{sourceInput.value()}, null);
        return bulkShardProcessor.add(indexNameResolver.get(), item, rowShardResolver.routing());
    }

    @Override
    public boolean allLoaded() {
        return loading != null;
    }

    private boolean isLoading() {
        return loading != null && loading.isDone() == false;
    }

    private void raiseIfLoading() {
        if (isLoading()) {
            throw new IllegalStateException("Iterator is loading");
        }
    }
}

