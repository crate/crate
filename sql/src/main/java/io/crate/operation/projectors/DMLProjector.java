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

import com.google.common.util.concurrent.Futures;
import io.crate.core.collections.Row;
import io.crate.executor.transport.ShardRequest;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.operation.collect.CollectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class DMLProjector<Request extends ShardRequest> extends AbstractProjector {

    static final int DEFAULT_BULK_SIZE = 1024;

    private final ShardId shardId;
    private final CollectExpression<Row, ?> collectUidExpression;
    private final AtomicBoolean failed = new AtomicBoolean(false);

    protected final ClusterService clusterService;
    protected final Settings settings;
    protected final TransportActionProvider transportActionProvider;
    protected final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    protected final UUID jobId;

    private BulkShardProcessor<Request> bulkShardProcessor;

    DMLProjector(ClusterService clusterService,
                        Settings settings,
                        ShardId shardId,
                        TransportActionProvider transportActionProvider,
                        BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                        CollectExpression<Row, ?> collectUidExpression,
                        UUID jobId) {
        this.clusterService = clusterService;
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.shardId = shardId;
        this.collectUidExpression = collectUidExpression;
        this.jobId = jobId;
    }

    protected abstract BulkShardProcessor<Request> createBulkShardProcessor(int bulkSize);

    protected abstract ShardRequest.Item createItem(String id);

    @Override
    public void prepare() {
        super.prepare();
        bulkShardProcessor = createBulkShardProcessor(DEFAULT_BULK_SIZE);
        Futures.addCallback(bulkShardProcessor.result(), new BulkProcessorFutureCallback(failed, downstream));
    }

    @Override
    public boolean setNextRow(Row row) {
        // resolve the Uid
        collectUidExpression.setNextRow(row);
        Uid uid = Uid.createUid(((BytesRef)collectUidExpression.value()).utf8ToString());
        // routing is already resolved
        bulkShardProcessor.addForExistingShard(shardId, createItem(uid.id()), null);
        return true;
    }

    @Override
    public void finish() {
        bulkShardProcessor.close();
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
