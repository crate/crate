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
import io.crate.executor.transport.kill.KillableCallable;
import io.crate.jobs.KillAllListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class TransportShardAction<R extends ShardRequest>
        extends TransportReplicationAction<R, R, ShardResponse> implements KillAllListener {

    private final Multimap<UUID, KillableCallable> activeOperations = Multimaps.synchronizedMultimap(HashMultimap.<UUID, KillableCallable>create());

    public TransportShardAction(Settings settings,
                                String actionName,
                                TransportService transportService,
                                MappingUpdatedAction mappingUpdatedAction,
                                IndexNameExpressionResolver indexNameExpressionResolver,
                                ClusterService clusterService,
                                IndicesService indicesService,
                                ThreadPool threadPool,
                                ShardStateAction shardStateAction,
                                ActionFilters actionFilters,
                                Class<R> requestClass) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction,
                mappingUpdatedAction, actionFilters, indexNameExpressionResolver, requestClass, requestClass, ThreadPool.Names.BULK);
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
    protected void shardOperationOnReplica(final ShardId shardId, final R shardRequest) {
        KillableCallable<Tuple> callable = new KillableWrapper() {
            @Override
            public Tuple call() throws Exception {
                processRequestItemsOnReplica(shardId, shardRequest, killed);
                return null;
            }
        };
        wrapOperationInKillable(shardRequest, callable);
    }

    @Override
    protected Tuple<ShardResponse, R> shardOperationOnPrimary(ClusterState clusterState, final PrimaryOperationRequest shardRequest) {
        KillableCallable<Tuple> callable = new KillableWrapper() {
            @Override
            public Tuple call() throws Exception {
                ShardResponse shardResponse = processRequestItems(shardRequest.shardId, shardRequest.request, killed);
                return new Tuple<>(shardResponse, shardRequest.request);
            }
        };

        return wrapOperationInKillable(shardRequest.request, callable);
    }

    protected Tuple<ShardResponse, R> wrapOperationInKillable(R request, KillableCallable<Tuple> callable) {
        activeOperations.put(request.jobId(), callable);
        Tuple<ShardResponse, R> response;
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
    public void killAllJobs(long timestamp) {
        synchronized (activeOperations) {
            for (KillableCallable callable : activeOperations.values()) {
                callable.kill();
            }
            activeOperations.clear();
        }
    }

    @Override
    public void killJob(UUID jobId) {
        synchronized (activeOperations) {
            Collection<KillableCallable> operations = activeOperations.get(jobId);
            for (KillableCallable callable : operations) {
                callable.kill();
            }
            activeOperations.removeAll(jobId);
        }
    }

    protected abstract ShardResponse processRequestItems(ShardId shardId, R request, AtomicBoolean killed);

    protected abstract void processRequestItemsOnReplica(ShardId shardId, R request, AtomicBoolean killed);

    static abstract class KillableWrapper implements KillableCallable<Tuple> {

        protected AtomicBoolean killed = new AtomicBoolean(false);

        @Override
        public void kill() {
            killed.getAndSet(true);
        }
    }
}
