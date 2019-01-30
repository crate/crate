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

package io.crate.execution.dml;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.crate.Constants;
import io.crate.exceptions.JobKilledException;
import io.crate.execution.ddl.SchemaUpdateClient;
import io.crate.execution.jobs.kill.KillAllListener;
import io.crate.execution.jobs.kill.KillableCallable;
import io.crate.metadata.ColumnIdent;
import org.elasticsearch.action.bulk.MappingUpdatePerformer;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Base class for performing Crate-specific TransportWriteActions like Delete or Upsert.
 * @param <Request> The ShardRequest implementation including a replicated write request
 */
public abstract class TransportShardAction<Request extends ShardRequest<Request, Item>, Item extends ShardRequest.Item>
        extends TransportWriteAction<Request, Request, ShardResponse>
        implements KillAllListener {

    private final Multimap<UUID, KillableCallable> activeOperations = Multimaps.synchronizedMultimap(HashMultimap.<UUID, KillableCallable>create());
    private final MappingUpdatePerformer mappingUpdate;

    protected TransportShardAction(Settings settings,
                                   String actionName,
                                   TransportService transportService,
                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                   ClusterService clusterService,
                                   IndicesService indicesService,
                                   ThreadPool threadPool,
                                   ShardStateAction shardStateAction,
                                   Supplier<Request> requestSupplier,
                                   SchemaUpdateClient schemaUpdateClient) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction,
            indexNameExpressionResolver, requestSupplier, requestSupplier,ThreadPool.Names.WRITE);
        this.mappingUpdate = (update, shardId, type) -> {
            validateMapping(update.root().iterator(), false);
            schemaUpdateClient.blockingUpdateOnMaster(shardId.getIndex(), update);
        };
    }

    @Override
    protected ShardResponse newResponseInstance() {
        return new ShardResponse();
    }

    @Override
    protected boolean resolveIndex() {
        return true;
    }

    @VisibleForTesting
    @Override
    protected WritePrimaryResult<Request, ShardResponse> shardOperationOnPrimary(Request shardRequest, IndexShard indexShard) {
        KillableWrapper<WritePrimaryResult<Request, ShardResponse>> callable =
            new KillableWrapper<WritePrimaryResult<Request, ShardResponse>>() {
                @Override
                public WritePrimaryResult<Request, ShardResponse> call() throws Exception {
                    return processRequestItems(indexShard, shardRequest, killed);
                }
        };
        return wrapOperationInKillable(shardRequest, callable);
    }


    @Override
    protected WriteReplicaResult<Request> shardOperationOnReplica(Request replicaRequest, IndexShard indexShard) {
        KillableWrapper<WriteReplicaResult<Request>> callable =
            new KillableWrapper<WriteReplicaResult<Request>>() {
                @Override
                public WriteReplicaResult<Request> call() throws Exception {
                    return processRequestItemsOnReplica(indexShard, replicaRequest);
                }
        };
        return wrapOperationInKillable(replicaRequest, callable);
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

    protected abstract WritePrimaryResult<Request, ShardResponse> processRequestItems(IndexShard indexShard, Request request, AtomicBoolean killed) throws InterruptedException, IOException;

    protected abstract WriteReplicaResult<Request> processRequestItemsOnReplica(IndexShard indexShard, Request replicaRequest) throws IOException;

    abstract static class KillableWrapper<WrapperResponse> implements KillableCallable<WrapperResponse> {

        protected AtomicBoolean killed = new AtomicBoolean(false);

        @Override
        public void kill(@Nullable Throwable t) {
            killed.getAndSet(true);
        }
    }

    protected <T extends Engine.Result> T executeOnPrimaryHandlingMappingUpdate(ShardId shardId,
                                                                                CheckedSupplier<T, IOException> execute,
                                                                                Function<Exception, T> onMappingUpdateError) throws IOException {
        T result = execute.get();
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
            try {
                mappingUpdate.updateMappings(result.getRequiredMappingUpdate(), shardId, Constants.DEFAULT_MAPPING_TYPE);
            } catch (Exception e) {
                return onMappingUpdateError.apply(e);
            }
            result = execute.get();
            if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                // double mapping update. We assume that the successful mapping update wasn't yet processed on the node
                // and retry the entire request again.
                throw new ReplicationOperation.RetryOnPrimaryException(shardId,
                    "Dynamic mappings are not available on the node that holds the primary yet");
            }
        }
        assert result.getFailure() instanceof ReplicationOperation.RetryOnPrimaryException == false :
            "IndexShard shouldn't use RetryOnPrimaryException. got " + result.getFailure();
        return result;
    }

    @VisibleForTesting
    public static void validateMapping(Iterator<Mapper> mappers, boolean nested) {
        while (mappers.hasNext()) {
            Mapper mapper = mappers.next();
            if (nested) {
                ColumnIdent.validateObjectKey(mapper.simpleName());
            } else {
                ColumnIdent.validateColumnName(mapper.simpleName());
            }
            validateMapping(mapper.iterator(), true);
        }
    }
}
