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

package io.crate.executor.transport.task;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.executor.JobTask;
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.jobs.*;
import io.crate.metadata.PartitionName;
import io.crate.metadata.settings.CrateSettings;
import io.crate.planner.node.dml.UpsertById;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class UpsertByIdTask extends JobTask {

    private final BulkRequestExecutor<ShardUpsertRequest> transportShardUpsertActionDelegate;
    private final UpsertById upsertById;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final TransportBulkCreateIndicesAction transportBulkCreateIndicesAction;
    private final ClusterService clusterService;
    private final List<SettableFuture<TaskResult>> resultList;
    private final AutoCreateIndex autoCreateIndex;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final JobContextService jobContextService;

    @Nullable
    private BulkShardProcessorContext bulkShardProcessorContext;
    private JobExecutionContext jobExecutionContext;

    public UpsertByIdTask(UpsertById upsertById,
                          ClusterService clusterService,
                          IndexNameExpressionResolver indexNameExpressionResolver,
                          Settings settings,
                          BulkRequestExecutor<ShardUpsertRequest> transportShardUpsertActionDelegate,
                          TransportCreateIndexAction transportCreateIndexAction,
                          TransportBulkCreateIndicesAction transportBulkCreateIndicesAction,
                          BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                          JobContextService jobContextService) {
        super(upsertById.jobId());
        this.upsertById = upsertById;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transportShardUpsertActionDelegate = transportShardUpsertActionDelegate;
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.transportBulkCreateIndicesAction = transportBulkCreateIndicesAction;
        this.clusterService = clusterService;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.jobContextService = jobContextService;
        autoCreateIndex = new AutoCreateIndex(settings, indexNameExpressionResolver);

        if (upsertById.items().size() == 1) {
            // skip useless usage of bulk processor if only 1 item in statement
            // and instead create upsert request directly on start()
            resultList = new ArrayList<>(1);
            resultList.add(SettableFuture.<TaskResult>create());
        } else {
            resultList = initializeBulkShardProcessor(settings);
        }

    }

    @Override
    public void start() {
        try {
            if (upsertById.items().size() == 1) {
                // directly execute upsert request without usage of bulk processor
                @SuppressWarnings("unchecked")
                SettableFuture<TaskResult> futureResult = (SettableFuture) resultList.get(0);
                UpsertById.Item item = upsertById.items().get(0);
                if (upsertById.isPartitionedTable()
                        && autoCreateIndex.shouldAutoCreate(item.index(), clusterService.state())) {
                    createIndexAndExecuteUpsertRequest(item, futureResult);
                } else {
                    executeUpsertRequest(item, futureResult);
                }

            } else if (bulkShardProcessorContext != null) {
                assert jobExecutionContext != null;
                for (UpsertById.Item item : upsertById.items()) {
                    ShardUpsertRequest.Item requestItem = new ShardUpsertRequest.Item(
                            item.id(), item.updateAssignments(), item.insertValues(), item.version());
                    bulkShardProcessorContext.add(item.index(), requestItem, item.routing());
                }
                jobExecutionContext.start();
            }
        } catch (Throwable t){
            for (SettableFuture<TaskResult> future : resultList) {
                future.setException(t);
            }
        }
    }

    private void executeUpsertRequest(final UpsertById.Item item, final SettableFuture<TaskResult> futureResult) {
        ShardId shardId;
        try {
            shardId = clusterService.operationRouting().indexShards(
                    clusterService.state(),
                    item.index(),
                    Constants.DEFAULT_MAPPING_TYPE,
                    item.id(),
                    item.routing()
            ).shardId();
        } catch (IndexNotFoundException e) {
            if (PartitionName.isPartition(item.index())) {
                futureResult.set(TaskResult.ZERO);
                return;
            }
            throw e;
        }

        ShardUpsertRequest upsertRequest = new ShardUpsertRequest(
                shardId, upsertById.updateColumns(), upsertById.insertColumns(), item.routing(), jobId());
        upsertRequest.continueOnError(false);
        ShardUpsertRequest.Item requestItem = new ShardUpsertRequest.Item(
                item.id(), item.updateAssignments(), item.insertValues(), item.version());
        upsertRequest.add(0, requestItem);

        UpsertByIdContext upsertByIdContext = new UpsertByIdContext(
                upsertById.executionPhaseId(), upsertRequest, item, futureResult, transportShardUpsertActionDelegate);
        createJobExecutionContext(upsertByIdContext);
        try {
            jobExecutionContext.start();
        } catch (Throwable throwable) {
            futureResult.setException(throwable);
        }
    }

    private void createJobExecutionContext(ExecutionSubContext context) {
        assert jobExecutionContext == null;
        JobExecutionContext.Builder contextBuilder = jobContextService.newBuilder(jobId());
        contextBuilder.addSubContext(context);
        jobExecutionContext = jobContextService.createContext(contextBuilder);
    }

    private List<SettableFuture<TaskResult>> initializeBulkShardProcessor(Settings settings) {

        assert upsertById.updateColumns() != null | upsertById.insertColumns() != null;
        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
                CrateSettings.BULK_REQUEST_TIMEOUT.extractTimeValue(settings),
                false, // do not overwrite duplicates
                upsertById.isBulkRequest() || upsertById.updateColumns() != null, // continue on error on bulk and/or update
                upsertById.updateColumns(),
                upsertById.insertColumns(),
                jobId(),
                false
        );
        BulkShardProcessor<ShardUpsertRequest> bulkShardProcessor = new BulkShardProcessor<>(
                clusterService,
                transportBulkCreateIndicesAction,
                indexNameExpressionResolver,
                settings,
                bulkRetryCoordinatorPool,
                upsertById.isPartitionedTable(),
                upsertById.items().size(),
                builder,
                transportShardUpsertActionDelegate,
                jobId());
        bulkShardProcessorContext = new BulkShardProcessorContext(
                upsertById.executionPhaseId(), bulkShardProcessor);
        createJobExecutionContext(bulkShardProcessorContext);

        if (!upsertById.isBulkRequest()) {
            final SettableFuture<TaskResult> futureResult = SettableFuture.create();
            List<SettableFuture<TaskResult>> resultList = new ArrayList<>(1);
            resultList.add(futureResult);
            Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
                @Override
                public void onSuccess(@Nullable BitSet result) {
                    if (result == null) {
                        futureResult.set(TaskResult.ROW_COUNT_UNKNOWN);
                    } else {
                        futureResult.set(new RowCountResult(result.cardinality()));
                    }
                    bulkShardProcessorContext.close();
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    futureResult.setException(t);
                    bulkShardProcessorContext.close();
                }
            });
            return resultList;
        } else {
            final int numResults = upsertById.items().size();
            final List<SettableFuture<TaskResult>> resultList = new ArrayList<>(numResults);
            for (int i = 0; i < numResults; i++) {
                resultList.add(SettableFuture.<TaskResult>create());
            }

            Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
                @Override
                public void onSuccess(@Nullable BitSet result) {
                    if (result == null) {
                        setAllToFailed(null);
                        return;
                    }

                    for (int i = 0; i < numResults; i++) {
                        SettableFuture<TaskResult> future = resultList.get(i);
                        future.set(result.get(i) ? TaskResult.ONE_ROW : TaskResult.FAILURE);
                    }
                    bulkShardProcessorContext.close();
                }

                private void setAllToFailed(@Nullable Throwable throwable) {
                    if (throwable instanceof InterruptedException) {
                        for (ListenableFuture<TaskResult> future : resultList) {
                            ((SettableFuture<TaskResult>) future).setException(throwable);
                        }
                    } else if (throwable == null) {
                        for (ListenableFuture<TaskResult> future : resultList) {
                            ((SettableFuture<TaskResult>) future).set(TaskResult.FAILURE);
                        }
                    } else {
                        for (ListenableFuture<TaskResult> future : resultList) {
                            ((SettableFuture<TaskResult>) future).set(RowCountResult.error(throwable));
                        }
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    setAllToFailed(t);
                    bulkShardProcessorContext.close();
                }
            });
            return resultList;
        }
    }

    private void createIndexAndExecuteUpsertRequest(final UpsertById.Item item,
                                                    final SettableFuture<TaskResult> futureResult) {
        transportCreateIndexAction.execute(
                new CreateIndexRequest(item.index()).cause("upsert single item"),
                new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                executeUpsertRequest(item, futureResult);
            }

            @Override
            public void onFailure(Throwable e) {
                e = ExceptionsHelper.unwrapCause(e);
                if (e instanceof IndexAlreadyExistsException) {
                    executeUpsertRequest(item, futureResult);
                } else {
                    futureResult.setException(e);
                }

            }
        });
    }


    @Override
    public List<? extends ListenableFuture<TaskResult>> result() {
        return resultList;
    }
}
