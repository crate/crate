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
import io.crate.executor.transport.ShardRequest;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.RowShardResolver;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.shard.ShardId;

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

    @Nullable
    private RowShardResolver rowShardResolver;
    @Nullable
    private Supplier<String> indexNameResolver;
    @Nullable
    private ShardId shardId;

    private final Iterable<? extends CollectExpression<Row, ?>> collectExpressions;
    private final BulkShardProcessor<ShardUpsertRequest> bulkShardProcessor;
    private final BatchIterator source;
    private final RowColumns rowData;
    private final Row sourceRow;
    private CompletableFuture<BitSet> loading;
    private BitSet result;
    private boolean fromStart = true;
    private Supplier<? extends ShardRequest.Item> updateItemSupplier;

    private IndexWriterCountBatchIterator(BatchIterator source,
                                          Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                          BulkShardProcessor bulkShardProcessor,
                                          Supplier<? extends ShardRequest.Item> updateItemSupplier,
                                          Supplier<String> indexNameResolver,
                                          RowShardResolver rowShardResolver) {
        this.source = source;
        this.collectExpressions = collectExpressions;
        this.bulkShardProcessor = bulkShardProcessor;
        this.updateItemSupplier = updateItemSupplier;
        this.sourceRow = RowBridging.toRow(source.rowData());
        this.rowData = new RowColumns(1);
        this.indexNameResolver = indexNameResolver;
        this.rowShardResolver = rowShardResolver;
    }

    private IndexWriterCountBatchIterator(BatchIterator source,
                                          Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                          BulkShardProcessor bulkShardProcessor,
                                          Supplier<? extends ShardRequest.Item> updateItemSupplier,
                                          ShardId shardId) {
        this.source = source;
        this.collectExpressions = collectExpressions;
        this.bulkShardProcessor = bulkShardProcessor;
        this.updateItemSupplier = updateItemSupplier;
        this.sourceRow = RowBridging.toRow(source.rowData());
        this.rowData = new RowColumns(1);
        this.shardId = shardId;
    }

    public static BatchIterator newIndexInstance(BatchIterator source, Supplier<String> indexNameResolver,
                                                 Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                                 RowShardResolver rowShardResolver,
                                                 BulkShardProcessor bulkShardProcessor,
                                                 Supplier<? extends ShardRequest.Item> updateItemSupplier) {
        IndexWriterCountBatchIterator delegate = new IndexWriterCountBatchIterator(source,
            collectExpressions, bulkShardProcessor, updateItemSupplier, indexNameResolver,
            rowShardResolver);
        return new CloseAssertingBatchIterator(delegate);
    }

    public static BatchIterator newShardInstance(BatchIterator source, ShardId shardId,
                                                 Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                                 BulkShardProcessor bulkShardProcessor,
                                                 Supplier<? extends ShardRequest.Item> updateItemSupplier) {
        IndexWriterCountBatchIterator delegate = new IndexWriterCountBatchIterator(source,
            collectExpressions, bulkShardProcessor, updateItemSupplier, shardId);
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
        loading = new CompletableFuture<>();
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

        if (rowShardResolver != null) {
            rowShardResolver.setNextRow(sourceRow);
        }

        ShardRequest.Item item = updateItemSupplier.get();
        if (shardId != null) {
            return bulkShardProcessor.addForExistingShard(shardId, item, null);
        } else {
            return this.bulkShardProcessor.add(indexNameResolver.get(), item, rowShardResolver.routing());
        }
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

