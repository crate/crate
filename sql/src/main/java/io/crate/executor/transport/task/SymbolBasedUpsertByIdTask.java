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
import io.crate.executor.transport.ShardUpsertResponse;
import io.crate.executor.transport.SymbolBasedShardUpsertRequest;
import io.crate.jobs.*;
import io.crate.metadata.settings.CrateSettings;
import io.crate.planner.node.dml.SymbolBasedUpsertByIdNode;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.action.bulk.SymbolBasedBulkShardProcessor;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;

public class SymbolBasedUpsertByIdTask extends JobTask {

    private final BulkRequestExecutor<SymbolBasedShardUpsertRequest, ShardUpsertResponse> transportShardUpsertActionDelegate;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final TransportBulkCreateIndicesAction transportBulkCreateIndicesAction;
    private final ClusterService clusterService;
    private final SymbolBasedUpsertByIdNode node;
    private final List<SettableFuture<TaskResult>> resultList;
    private final AutoCreateIndex autoCreateIndex;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final JobContextService jobContextService;

    @Nullable
    private SymbolBasedBulkShardProcessorContext bulkShardProcessorContext;
    private JobExecutionContext jobExecutionContext;

    public SymbolBasedUpsertByIdTask(UUID jobId,
                                     ClusterService clusterService,
                                     Settings settings,
                                     BulkRequestExecutor<SymbolBasedShardUpsertRequest, ShardUpsertResponse> transportShardUpsertActionDelegate,
                                     TransportCreateIndexAction transportCreateIndexAction,
                                     TransportBulkCreateIndicesAction transportBulkCreateIndicesAction,
                                     BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                     SymbolBasedUpsertByIdNode node,
                                     JobContextService jobContextService) {
        super(jobId);
        this.transportShardUpsertActionDelegate = transportShardUpsertActionDelegate;
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.transportBulkCreateIndicesAction = transportBulkCreateIndicesAction;
        this.clusterService = clusterService;
        this.node = node;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.jobContextService = jobContextService;
        autoCreateIndex = new AutoCreateIndex(settings);

        if (node.items().size() == 1) {
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
            if (node.items().size() == 1) {
                // directly execute upsert request without usage of bulk processor
                @SuppressWarnings("unchecked")
                SettableFuture<TaskResult> futureResult = (SettableFuture) resultList.get(0);
                SymbolBasedUpsertByIdNode.Item item = node.items().get(0);
                if (node.isPartitionedTable()
                        && autoCreateIndex.shouldAutoCreate(item.index(), clusterService.state())) {
                    createIndexAndExecuteUpsertRequest(item, futureResult);
                } else {
                    executeUpsertRequest(item, futureResult);
                }

            } else if (bulkShardProcessorContext != null) {
                assert jobExecutionContext != null;
                for (SymbolBasedUpsertByIdNode.Item item : node.items()) {
                    bulkShardProcessorContext.add(
                            item.index(),
                            item.id(),
                            item.updateAssignments(),
                            item.insertValues(),
                            item.routing(),
                            item.version());
                }
                jobExecutionContext.start();
            }
        } catch (Throwable t){
            for (SettableFuture<TaskResult> future : resultList) {
                future.setException(t);
            }
        }
    }

    private void executeUpsertRequest(final SymbolBasedUpsertByIdNode.Item item, final SettableFuture futureResult) {
        ShardId shardId = clusterService.operationRouting().indexShards(
                clusterService.state(),
                item.index(),
                Constants.DEFAULT_MAPPING_TYPE,
                item.id(),
                item.routing()
        ).shardId();

        SymbolBasedShardUpsertRequest upsertRequest = new SymbolBasedShardUpsertRequest(shardId, node.updateColumns(), node.insertColumns(), jobId());
        upsertRequest.continueOnError(false);
        upsertRequest.add(0, item.id(), item.updateAssignments(), item.insertValues(), item.version(), item.routing());

        UpsertByIdContext upsertByIdContext = new UpsertByIdContext(
                node.executionPhaseId(), upsertRequest, item, futureResult, transportShardUpsertActionDelegate);
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

        assert node.updateColumns() != null | node.insertColumns() != null;
        SymbolBasedShardUpsertRequest.Builder builder = new SymbolBasedShardUpsertRequest.Builder(
                CrateSettings.BULK_REQUEST_TIMEOUT.extractTimeValue(settings),
                false, // do not overwrite duplicates
                node.isBulkRequest() || node.updateColumns() != null, // continue on error on bulk and/or update
                node.updateColumns(),
                node.insertColumns(),
                jobId()
        );
        SymbolBasedBulkShardProcessor<SymbolBasedShardUpsertRequest, ShardUpsertResponse> bulkShardProcessor = new SymbolBasedBulkShardProcessor<>(
                clusterService,
                transportBulkCreateIndicesAction,
                settings,
                bulkRetryCoordinatorPool,
                node.isPartitionedTable(),
                node.items().size(),
                builder,
                transportShardUpsertActionDelegate,
                jobId());
        bulkShardProcessorContext = new SymbolBasedBulkShardProcessorContext(
                node.executionPhaseId(), bulkShardProcessor);
        createJobExecutionContext(bulkShardProcessorContext);

        if (!node.isBulkRequest()) {
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
            final int numResults = node.items().size();
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
                    if (throwable instanceof CancellationException) {
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

    private void createIndexAndExecuteUpsertRequest(final SymbolBasedUpsertByIdNode.Item item,
                                                    final SettableFuture futureResult) {
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

    @Override
    public void upstreamResult(List<? extends ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("UpsertByIdTask can't have an upstream result");
    }
}
