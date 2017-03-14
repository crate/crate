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

import io.crate.Constants;
import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowsBatchIterator;
import io.crate.executor.Executor;
import io.crate.executor.JobTask;
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

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
        autoCreateIndex = new AutoCreateIndex(settings, indexNameExpressionResolver);
    }

    @Override
    public void execute(final BatchConsumer consumer, Row parameters) {
        CompletableFuture<Long> result;
        if (upsertById.items().size() > 1) {
            try {
                result = executeBulkShardProcessor().get(0);
            } catch (Throwable throwable) {
                consumer.accept(null, throwable);
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
                consumer.accept(null, throwable);
                return;
            }
        }
        result.whenComplete((count, throwable) -> {
            consumer.accept(RowsBatchIterator.newInstance(new Row1(count)), throwable);
        });
    }

    private List<CompletableFuture<Long>> executeBulkShardProcessor() throws Throwable {
        List<CompletableFuture<Long>> resultList = initializeBulkShardProcessor(settings);
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
    public List<CompletableFuture<Long>> executeBulk() {
        try {
            List<CompletableFuture<Long>> resultList = executeBulkShardProcessor();
            return resultList;
        } catch (Throwable throwable) {
            return Collections.singletonList(CompletableFutures.failedFuture(throwable));
        }
    }

    private void executeUpsertRequest(final UpsertById.Item item, final CompletableFuture<Long> future) {
        CompletableFuture<Long> f = executeUpsertRequest(item);
        f.whenComplete((Long result, Throwable t) -> {
            if (t == null) {
                future.complete(result);
            } else {
                future.completeExceptionally(t);
            }
        });
    }

    private CompletableFuture<Long> executeUpsertRequest(final UpsertById.Item item) {
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
                return CompletableFuture.completedFuture(0L);
            } else {
                return CompletableFutures.failedFuture(e);
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
            return CompletableFutures.failedFuture(throwable);
        }
        return upsertByIdContext.resultFuture();
    }

    private void createJobExecutionContext(ExecutionSubContext context) throws Exception {
        assert jobExecutionContext == null : "jobExecutionContext must be null";
        JobExecutionContext.Builder contextBuilder = jobContextService.newBuilder(jobId());
        contextBuilder.addSubContext(context);
        jobExecutionContext = jobContextService.createContext(contextBuilder);
    }

    private List<CompletableFuture<Long>> initializeBulkShardProcessor(Settings settings) {
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
            final CompletableFuture<Long> futureResult = new CompletableFuture<>();
            List<CompletableFuture<Long>> resultList = new ArrayList<>(1);
            resultList.add(futureResult);
            bulkShardProcessor.result().whenComplete((BitSet result, Throwable t) -> {
                if (t == null) {
                    if (result == null) {
                        // unknown rowcount
                        futureResult.complete(Executor.ROWCOUNT_UNKNOWN);
                    } else {
                        futureResult.complete((long) result.cardinality());
                    }
                    bulkShardProcessorContext.close();
                } else {
                    futureResult.completeExceptionally(t);
                    bulkShardProcessorContext.close();
                }
            });
            return resultList;
        } else {
            final int numResults = upsertById.numBulkResponses();
            final Integer[] resultsRowCount = new Integer[numResults];
            final List<CompletableFuture<Long>> resultList = new ArrayList<>(numResults);
            for (int i = 0; i < numResults; i++) {
                resultList.add(new CompletableFuture<>());
            }

            bulkShardProcessor.result().whenComplete((BitSet result, Throwable t) -> {
                if (t == null) {
                    if (result == null) {
                        setAllToFailed(null, resultList);
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
                        CompletableFuture<Long> future = resultList.get(i);
                        Integer rowCount = resultsRowCount[i];
                        if (rowCount != null && rowCount >= 0) {
                            future.complete((long) rowCount);
                        } else {
                            // failure
                            future.complete(Executor.ROWCOUNT_ERROR);
                        }
                    }

                    bulkShardProcessorContext.close();
                } else {
                    setAllToFailed(t, resultList);
                    bulkShardProcessorContext.close();
                }
            });
            return resultList;
        }
    }

    private void setAllToFailed(@Nullable Throwable throwable, List<CompletableFuture<Long>> futures) {
        if (throwable == null) {
            for (CompletableFuture<Long> future : futures) {
                future.complete(Executor.ROWCOUNT_ERROR);
            }
        } else {
            for (CompletableFuture<Long> future : futures) {
                future.completeExceptionally(throwable);
            }
        }
    }

    private CompletableFuture<Long> createIndexAndExecuteUpsertRequest(final UpsertById.Item item) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        transportCreateIndexAction.execute(
            new CreateIndexRequest(item.index()).cause("upsert single item"),
            new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    executeUpsertRequest(item, future);
                }

                @Override
                public void onFailure(Throwable e) {
                    e = ExceptionsHelper.unwrapCause(e);
                    if (e instanceof IndexAlreadyExistsException) {
                        executeUpsertRequest(item, future);
                    } else {
                        future.completeExceptionally(e);
                    }

                }
            });
        return future;
    }
}
