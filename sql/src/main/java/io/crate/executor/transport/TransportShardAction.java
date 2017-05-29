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

package io.crate.executor.transport;

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.crate.exceptions.JobKilledException;
import io.crate.executor.transport.kill.KillableCallable;
import io.crate.jobs.KillAllListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public abstract class TransportShardAction<Request extends ShardRequest<Request, Item>, Item extends ShardRequest.Item>
    extends TransportWriteAction<Request, ShardResponse> implements KillAllListener {

    private final Multimap<UUID, KillableCallable> activeOperations = Multimaps.synchronizedMultimap(HashMultimap.<UUID, KillableCallable>create());

    TransportShardAction(Settings settings,
                         String actionName,
                         TransportService transportService,
                         IndexNameExpressionResolver indexNameExpressionResolver,
                         ClusterService clusterService,
                         IndicesService indicesService,
                         ThreadPool threadPool,
                         ShardStateAction shardStateAction,
                         ActionFilters actionFilters,
                         Supplier<Request> requestSupplier) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction,
            actionFilters, indexNameExpressionResolver, requestSupplier, ThreadPool.Names.BULK);
    }

    @Override
    protected ShardResponse newResponseInstance() {
        return new ShardResponse();
    }

    @Override
    protected boolean resolveIndex() {
        return true;
    }

    @Override
    protected WriteResult<ShardResponse> onPrimaryShard(Request shardRequest, IndexShard indexShard) throws Exception {
        KillableWrapper<WriteResult<ShardResponse>> callable = new KillableWrapper<WriteResult<ShardResponse>>() {
            @Override
            public WriteResult<ShardResponse> call() throws Exception {
                return processRequestItems(indexShard, shardRequest, killed);
            }
        };
        return wrapOperationInKillable(shardRequest, callable);
    }


    @Override
    protected Translog.Location onReplicaShard(Request shardRequest, IndexShard indexShard) {
        KillableWrapper<Translog.Location> callable = new KillableWrapper<Translog.Location>() {
            @Override
            public Translog.Location call() throws Exception {
                return processRequestItemsOnReplica(indexShard, shardRequest);
            }

        };
        return wrapOperationInKillable(shardRequest, callable);
    }

    private <WrapperResponse> WrapperResponse wrapOperationInKillable(Request request, KillableCallable<WrapperResponse> callable) {
        activeOperations.put(request.jobId(), callable);
        WrapperResponse response;
        try {
            //noinspection unchecked
            response = callable.call();
        } catch (Throwable e) {
            throw Throwables.propagate(e);
        } finally {
            activeOperations.remove(request.jobId(), callable);
        }
        return response;

    }

    @Override
    public void killAllJobs() {
        synchronized (activeOperations) {
            for (KillableCallable callable : activeOperations.values()) {
                callable.kill(new InterruptedException(JobKilledException.MESSAGE));
            }
            activeOperations.clear();
        }
    }

    @Override
    public void killJob(UUID jobId) {
        synchronized (activeOperations) {
            Collection<KillableCallable> operations = activeOperations.get(jobId);
            for (KillableCallable callable : operations) {
                callable.kill(new InterruptedException(JobKilledException.MESSAGE));
            }
            activeOperations.removeAll(jobId);
        }
    }

    protected abstract WriteResult<ShardResponse> processRequestItems(IndexShard indexShard, Request request, AtomicBoolean killed) throws InterruptedException;

    protected abstract Translog.Location processRequestItemsOnReplica(IndexShard indexShard, Request request);

    static abstract class KillableWrapper<WrapperResponse> implements KillableCallable<WrapperResponse> {

        protected AtomicBoolean killed = new AtomicBoolean(false);

        @Override
        public void kill(@Nullable Throwable t) {
            killed.getAndSet(true);
        }
    }
}
