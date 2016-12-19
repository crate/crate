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
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.executor.Executor;
import io.crate.executor.JobTask;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.jobs.*;
import io.crate.metadata.PartitionName;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;
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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
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

    private final Settings settings;
    private final BulkRequestExecutor<ShardUpsertRequest> transportShardUpsertActionDelegate;
    private final UpsertById upsertById;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final TransportBulkCreateIndicesAction transportBulkCreateIndicesAction;
    private final ClusterService clusterService;
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
        this.settings = settings;
        this.transportShardUpsertActionDelegate = transportShardUpsertActionDelegate;
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.transportBulkCreateIndicesAction = transportBulkCreateIndicesAction;
        this.clusterService = clusterService;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.jobContextService = jobContextService;
        autoCreateIndex = new AutoCreateIndex(settings, clusterService.getClusterSettings(), indexNameExpressionResolver);
    }

    @Override
    public void execute(final RowReceiver rowReceiver, Row parameters) {
        ListenableFuture<Long> result;
        if (upsertById.items().size() > 1) {
            try {
                result = executeBulkShardProcessor().get(0);
            } catch (Throwable throwable) {
                rowReceiver.fail(throwable);
                return;
            }
        } else {
            assert upsertById.items().size() == 1 : "number of upsertById.items() must be 1";
            try {
                UpsertById.Item item = upsertById.items().get(0);
                if (upsertById.isPartitionedTable() &&
                    autoCreateIndex.shouldAutoCreate(item.index(), clusterService.state())) {
                    result = createIndexAndExecuteUpsertRequest(item);
                } else {
                    result = executeUpsertRequest(item);
                }
            } catch (Throwable throwable) {
                rowReceiver.fail(throwable);
                return;
            }
        }
        Futures.addCallback(result, new FutureCallback<Long>() {
            @Override
            public void onSuccess(@Nullable Long result) {
                rowReceiver.setNextRow(new Row1(result));
                rowReceiver.finish(RepeatHandle.UNSUPPORTED);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                rowReceiver.fail(t);
            }
        });
    }

    private List<SettableFuture<Long>> executeBulkShardProcessor() throws Throwable {
        List<SettableFuture<Long>> resultList = initializeBulkShardProcessor(settings);
        assert bulkShardProcessorContext != null : "bulkShardProcessorContext must not be null";
        createJobExecutionContext(bulkShardProcessorContext);
        assert jobExecutionContext != null : "jobExecutionContext must not be null";
        for (UpsertById.Item item : upsertById.items()) {
            ShardUpsertRequest.Item requestItem = new ShardUpsertRequest.Item(
                item.id(), item.updateAssignments(), item.insertValues(), item.version());
            bulkShardProcessorContext.add(item.index(), requestItem, item.routing());
        }
        jobExecutionContext.start();
        return resultList;
    }


    @Override
    public ListenableFuture<List<Long>> executeBulk() {
        try {
            List<SettableFuture<Long>> resultList = executeBulkShardProcessor();
            return Futures.allAsList(resultList);
        } catch (Throwable throwable) {
            return Futures.immediateFailedFuture(throwable);
        }
    }

    private void executeUpsertRequest(final UpsertById.Item item, final SettableFuture<Long> future) {
        final FutureCallback<Long> callback = new FutureCallback<Long>() {
            @Override
            public void onSuccess(@Nullable Long result) {
                future.set(result);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.setException(t);
            }
        };
        ListenableFuture<Long> f = executeUpsertRequest(item);
        Futures.addCallback(f, callback);
    }

    private ListenableFuture<Long> executeUpsertRequest(final UpsertById.Item item) {
        ShardId shardId;
        try {
            shardId = clusterService.operationRouting().indexShards(
                clusterService.state(),
                item.index(),
                item.id(),
                item.routing()
            ).shardId();
        } catch (IndexNotFoundException e) {
            if (PartitionName.isPartition(item.index())) {
                return Futures.immediateFuture(0L);
            } else {
                return Futures.immediateFailedFuture(e);
            }
        }

        ShardUpsertRequest upsertRequest = new ShardUpsertRequest.Builder(
            false,
            false,
            upsertById.updateColumns(),
            upsertById.insertColumns(),
            jobId(),
            false
        ).newRequest(shardId, item.routing());

        ShardUpsertRequest.Item requestItem = new ShardUpsertRequest.Item(
            item.id(), item.updateAssignments(), item.insertValues(), item.version());
        upsertRequest.add(0, requestItem);

        UpsertByIdContext upsertByIdContext = new UpsertByIdContext(
            upsertById.executionPhaseId(), upsertRequest, item, transportShardUpsertActionDelegate);

        try {
            createJobExecutionContext(upsertByIdContext);
            jobExecutionContext.start();
        } catch (Throwable throwable) {
            return Futures.immediateFailedFuture(throwable);
        }
        return upsertByIdContext.resultFuture();
    }

    private void createJobExecutionContext(ExecutionSubContext context) throws Exception {
        assert jobExecutionContext == null : "jobExecutionContext must be null";
        JobExecutionContext.Builder contextBuilder = jobContextService.newBuilder(jobId());
        contextBuilder.addSubContext(context);
        jobExecutionContext = jobContextService.createContext(contextBuilder);
    }

    private List<SettableFuture<Long>> initializeBulkShardProcessor(Settings settings) {
        assert upsertById.updateColumns() != null || upsertById.insertColumns() != null :
            "upsertById.updateColumns() or upsertById.insertColumns() must not be null";
        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            CrateSettings.BULK_REQUEST_TIMEOUT.extractTimeValue(settings),
            false, // do not overwrite duplicates
            upsertById.numBulkResponses() > 0 ||
            upsertById.items().size() > 1 ||
            upsertById.updateColumns() != null, // continue on error on bulk, on multiple values and/or update
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

        if (upsertById.numBulkResponses() == 0) {
            final SettableFuture<Long> futureResult = SettableFuture.create();
            List<SettableFuture<Long>> resultList = new ArrayList<>(1);
            resultList.add(futureResult);
            Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
                @Override
                public void onSuccess(@Nullable BitSet result) {
                    if (result == null) {
                        // unknown rowcount
                        futureResult.set(Executor.ROWCOUNT_UNKNOWN);
                    } else {
                        futureResult.set((long) result.cardinality());
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
            final int numResults = upsertById.numBulkResponses();
            final Integer[] resultsRowCount = new Integer[numResults];
            final List<SettableFuture<Long>> resultList = new ArrayList<>(numResults);
            for (int i = 0; i < numResults; i++) {
                resultList.add(SettableFuture.create());
            }

            Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
                @Override
                public void onSuccess(@Nullable BitSet result) {
                    if (result == null) {
                        setAllToFailed(null);
                        return;
                    }

                    for (int i = 0; i < result.length(); i++) {
                        int resultIdx = upsertById.bulkIndices().get(i);

                        if (resultsRowCount[resultIdx] == null) {
                            resultsRowCount[resultIdx] = result.get(i) ? 1 : -2;
                        } else if (resultsRowCount[resultIdx] >= 0 && result.get(i)) {
                            resultsRowCount[resultIdx]++;
                        }
                    }

                    for (int i = 0; i < numResults; i++) {
                        SettableFuture<Long> future = resultList.get(i);
                        Integer rowCount = resultsRowCount[i];
                        if (rowCount != null && rowCount >= 0) {
                            future.set((long) rowCount);
                        } else {
                            // failure
                            future.set(Executor.ROWCOUNT_ERROR);
                        }
                    }

                    bulkShardProcessorContext.close();
                }

                private void setAllToFailed(@Nullable Throwable throwable) {
                    if (throwable == null) {
                        for (SettableFuture<Long> future : resultList) {
                            future.set(Executor.ROWCOUNT_ERROR);
                        }
                    } else {
                        for (SettableFuture<Long> future : resultList) {
                            future.setException(throwable);
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


    private SettableFuture<Long> createIndexAndExecuteUpsertRequest(final UpsertById.Item item) {
        final SettableFuture<Long> future = SettableFuture.create();
        transportCreateIndexAction.execute(
            new CreateIndexRequest(item.index()).cause("upsert single item"),
            new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    executeUpsertRequest(item, future);
                }

                @Override
                public void onFailure(Exception e) {
                    Throwable t = ExceptionsHelper.unwrapCause(e);
                    if (t instanceof IndexAlreadyExistsException) {
                        executeUpsertRequest(item, future);
                    } else {
                        future.setException(t);
                    }

                }
            });
        return future;
    }
}
