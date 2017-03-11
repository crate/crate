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

import io.crate.data.BatchIteratorProjector;
import io.crate.data.Row;
import io.crate.executor.transport.ShardRequest;
import io.crate.operation.collect.CollectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collections;
import java.util.function.Function;
import java.util.function.Supplier;

class DMLProjector<Request extends ShardRequest> extends AbstractProjector {

    private final ShardId shardId;
    private final CollectExpression<Row, ?> collectIdExpression;

    private final BulkShardProcessor<Request> bulkShardProcessor;
    private final Function<String, ShardRequest.Item> itemFactory;

    DMLProjector(ShardId shardId,
                 CollectExpression<Row, ?> collectIdExpression,
                 BulkShardProcessor<Request> bulkShardProcessor,
                 Function<String, ShardRequest.Item> itemFactory) {
        this.shardId = shardId;
        this.collectIdExpression = collectIdExpression;
        this.bulkShardProcessor = bulkShardProcessor;
        this.itemFactory = itemFactory;
    }

    @Override
    public Result setNextRow(Row row) {
        throw new UnsupportedOperationException("DMLProjector must be used as BatchIteratorProjector");
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        throw new UnsupportedOperationException("DMLProjector must be used as BatchIteratorProjector");
    }

    @Override
    public void downstream(RowReceiver rowReceiver) {
        super.downstream(rowReceiver);
    }

    @Override
    public void fail(Throwable throwable) {
        throw new UnsupportedOperationException("DMLProjector must be used as BatchIteratorProjector");
    }

    @Override
    public void kill(Throwable throwable) {
        throw new UnsupportedOperationException("DMLProjector must be used as BatchIteratorProjector");
    }

    @Override
    public BatchIteratorProjector asProjector() {
        Supplier<ShardRequest.Item> updateItemSupplier = () -> {
            BytesRef id = (BytesRef) collectIdExpression.value();
            return itemFactory.apply(id.utf8ToString());
        };

        return it -> IndexWriterCountBatchIterator.newShardInstance(it, shardId,
            Collections.singletonList(collectIdExpression), bulkShardProcessor, updateItemSupplier);
    }
}
