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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.executor.transport.ShardUpsertResponse;
import io.crate.executor.transport.SymbolBasedShardUpsertRequest;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.action.bulk.SymbolBasedBulkShardProcessor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class UpdateProjector implements Projector, RowDownstreamHandle {

    public static final int DEFAULT_BULK_SIZE = 1024;

    private RowDownstreamHandle downstream;
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicReference<Throwable> upstreamFailure = new AtomicReference<>(null);

    private final ShardId shardId;
    private final CollectExpression<Row, ?> collectUidExpression;
    private final Symbol[] assignments;
    @Nullable
    private final Long requiredVersion;
    private final Object lock = new Object();

    private final SymbolBasedBulkShardProcessor<SymbolBasedShardUpsertRequest, ShardUpsertResponse> bulkShardProcessor;

    public UpdateProjector(ClusterService clusterService,
                           Settings settings,
                           ShardId shardId,
                           TransportActionProvider transportActionProvider,
                           BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                           CollectExpression<Row, ?> collectUidExpression,
                           String[] assignmentsColumns,
                           Symbol[] assignments,
                           @Nullable Long requiredVersion,
                           UUID jobId) {
        this.shardId = shardId;
        this.collectUidExpression = collectUidExpression;
        this.assignments = assignments;
        this.requiredVersion = requiredVersion;
        SymbolBasedShardUpsertRequest.Builder builder = new SymbolBasedShardUpsertRequest.Builder(
                CrateSettings.BULK_REQUEST_TIMEOUT.extractTimeValue(settings),
                false,
                false,
                assignmentsColumns,
                null,
                jobId
        );
        this.bulkShardProcessor = new SymbolBasedBulkShardProcessor<>(
                clusterService,
                transportActionProvider.transportBulkCreateIndicesAction(),
                settings,
                bulkRetryCoordinatorPool,
                false,
                DEFAULT_BULK_SIZE,
                builder,
                transportActionProvider.symbolBasedTransportShardUpsertActionDelegate(),
                jobId
        );
    }

    @Override
    public void startProjection(ExecutionState executionState) {
    }

    @Override
    public boolean setNextRow(Row row) {
        final Uid uid;
        synchronized (lock) {
            // resolve the Uid
            collectUidExpression.setNextRow(row);
            uid = Uid.createUid(((BytesRef)collectUidExpression.value()).utf8ToString());
        }
        // routing is already resolved
        bulkShardProcessor.addForExistingShard(shardId, uid.id(), assignments, null, null, requiredVersion);
        return true;
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        remainingUpstreams.incrementAndGet();
        return this;
    }

    @Override
    public void finish() {
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }

        bulkShardProcessor.close();
        if (downstream != null) {
            collectUpdateResultsAndPassOverRowCount();
        }
    }

    @Override
    public void fail(Throwable throwable) {
        upstreamFailure.set(throwable);
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }
        if (throwable instanceof CancellationException) {
            bulkShardProcessor.kill();
        } else {
            bulkShardProcessor.close();
        }

        if (downstream != null) {
            collectUpdateResultsAndPassOverRowCount();
        }
    }

    @Override
    public void downstream(RowDownstream downstream) {
        this.downstream = downstream.registerUpstream(this);
    }

    private void collectUpdateResultsAndPassOverRowCount() {
        Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
            @Override
            public void onSuccess(@Nullable BitSet result) {
                assert result != null : "BulkShardProcessor result is null";
                downstream.setNextRow(new Row1(result.cardinality()));
                Throwable throwable = upstreamFailure.get();
                if (throwable == null) {
                    downstream.finish();
                } else {
                    downstream.fail(throwable);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                downstream.setNextRow(new Row1(0L));
                downstream.fail(t);
            }
        });
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume() {
        throw new UnsupportedOperationException();
    }
}
