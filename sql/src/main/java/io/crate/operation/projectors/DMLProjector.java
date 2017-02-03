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

import io.crate.data.Row;
import io.crate.executor.transport.ShardRequest;
import io.crate.operation.collect.CollectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

class DMLProjector<Request extends ShardRequest> extends AbstractProjector {

    private final ShardId shardId;
    private final CollectExpression<Row, ?> collectUidExpression;
    private final AtomicBoolean failed = new AtomicBoolean(false);

    private final BulkShardProcessor<Request> bulkShardProcessor;
    private final Function<String, ShardRequest.Item> itemFactory;

    DMLProjector(ShardId shardId,
                 CollectExpression<Row, ?> collectUidExpression,
                 BulkShardProcessor<Request> bulkShardProcessor,
                 Function<String, ShardRequest.Item> itemFactory) {
        this.shardId = shardId;
        this.collectUidExpression = collectUidExpression;
        this.bulkShardProcessor = bulkShardProcessor;
        this.itemFactory = itemFactory;
    }

    @Override
    public Result setNextRow(Row row) {
        // resolve the Uid
        collectUidExpression.setNextRow(row);
        Uid uid = Uid.createUid(((BytesRef) collectUidExpression.value()).utf8ToString());
        // routing is already resolved
        bulkShardProcessor.addForExistingShard(shardId, itemFactory.apply(uid.id()), null);
        return Result.CONTINUE;
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        bulkShardProcessor.close();
    }

    @Override
    public void downstream(RowReceiver rowReceiver) {
        super.downstream(rowReceiver);
        BulkProcessorFutureCallback bulkProcessorFutureCallback = new BulkProcessorFutureCallback(failed, rowReceiver);
        bulkShardProcessor.result().whenComplete(bulkProcessorFutureCallback);
    }

    @Override
    public void fail(Throwable throwable) {
        failed.set(true);
        downstream.fail(throwable);

        if (throwable instanceof InterruptedException) {
            bulkShardProcessor.kill(throwable);
        } else {
            bulkShardProcessor.close();
        }
    }

    @Override
    public void kill(Throwable throwable) {
        failed.set(true);
        super.kill(throwable);
        bulkShardProcessor.kill(throwable);
    }
}
