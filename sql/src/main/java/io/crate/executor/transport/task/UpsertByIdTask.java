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
import io.crate.executor.transport.ShardUpsertResponse;
import io.crate.operation.collect.ShardingProjector;
import io.crate.planner.node.dml.UpsertByIdNode;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;

public class UpsertByIdTask extends JobTask {

    private final TransportShardUpsertActionDelegate transportShardUpsertActionDelegate;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final TransportBulkCreateIndicesAction transportBulkCreateIndicesAction;
    private final ClusterService clusterService;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final UpsertByIdNode node;
    private final List<ListenableFuture<TaskResult>> resultList;
    private final AutoCreateIndex autoCreateIndex;
    private final ShardingProjector shardingProjector;
    @Nullable
    private BulkShardProcessor<ShardUpsertRequest, ShardUpsertResponse> bulkShardProcessor;

    private static final ESLogger logger = Loggers.getLogger(UpsertByIdTask.class);

    public UpsertByIdTask(UUID jobId,
                          ClusterService clusterService,
                          Settings settings,
                          TransportShardUpsertActionDelegate transportShardUpsertActionDelegate,
                          TransportCreateIndexAction transportCreateIndexAction,
                          TransportBulkCreateIndicesAction transportBulkCreateIndicesAction,
                          BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                          UpsertByIdNode node) {
        super(jobId);
        this.transportShardUpsertActionDelegate = transportShardUpsertActionDelegate;
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.clusterService = clusterService;
        this.transportBulkCreateIndicesAction = transportBulkCreateIndicesAction;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.node = node;
        autoCreateIndex = new AutoCreateIndex(settings);
        shardingProjector = new ShardingProjector(
                node.primaryKeyIdents(),
                node.primaryKeySymbols(),
                node.routingSymbol());
        shardingProjector.startProjection();


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
        if (node.items().size() == 1) {
            // directly execute upsert request without usage of bulk processor
            @SuppressWarnings("unchecked")
            SettableFuture<TaskResult> futureResult = (SettableFuture)resultList.get(0);
            UpsertByIdNode.Item item = node.items().get(0);
            if (node.isPartitionedTable()
                    && autoCreateIndex.shouldAutoCreate(item.index(), clusterService.state())) {
                createIndexAndExecuteUpsertRequest(item, futureResult);
            } else {
                executeUpsertRequest(item, futureResult);
            }

        } else if (bulkShardProcessor != null) {
            for (UpsertByIdNode.Item item : node.items()) {
                bulkShardProcessor.add(
                        item.index(),
                        item,
                        item.version());
            }
            bulkShardProcessor.close();
        }
    }

    private void executeUpsertRequest(final UpsertByIdNode.Item item, final SettableFuture<TaskResult> futureResult) {
        shardingProjector.setNextRow(item);

        ShardId shardId = clusterService.operationRouting().indexShards(
                clusterService.state(),
                item.index(),
                Constants.DEFAULT_MAPPING_TYPE,
                shardingProjector.id(),
                shardingProjector.routing()
        ).shardId();

        AssignmentVisitor.AssignmentVisitorContext visitorContext = AssignmentVisitor.processAssignments(
                node.updateAssignments(),
                node.insertAssignments()
        );

        ShardUpsertRequest upsertRequest = new ShardUpsertRequest(
                shardId,
                visitorContext.dataTypes(),
                visitorContext.columnIndicesToStream(),
                node.updateAssignments(),
                node.insertAssignments());
        upsertRequest.continueOnError(false);
        upsertRequest.add(0, shardingProjector.id(), item, item.version(), shardingProjector.routing());

        transportShardUpsertActionDelegate.execute(upsertRequest, new ActionListener<ShardUpsertResponse>() {
            @Override
            public void onResponse(ShardUpsertResponse updateResponse) {
                int location = updateResponse.itemIndices().get(0);
                if (updateResponse.responses().get(location) != null) {
                    futureResult.set(TaskResult.ONE_ROW);
                } else {
                    ShardUpsertResponse.Failure failure = updateResponse.failures().get(location);
                    if (logger.isDebugEnabled()) {
                        if (failure.versionConflict()) {
                            logger.debug("Upsert of document with id {} failed because of a version conflict", failure.id());
                        } else {
                            logger.debug("Upsert of document with id {} failed {}", failure.id(), failure.message());
                        }
                    }
                    futureResult.set(TaskResult.ZERO);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                e = ExceptionsHelper.unwrapCause(e);
                if (node.insertAssignments() == null
                        && (e instanceof DocumentMissingException
                        || e instanceof VersionConflictEngineException)) {
                    // on updates, set affected row to 0 if document is not found or version conflicted
                    futureResult.set(TaskResult.ZERO);
                } else {
                    futureResult.setException(e);
                }
            }
        });
    }

    private List<ListenableFuture<TaskResult>> initializeBulkShardProcessor(Settings settings) {

        AssignmentVisitor.AssignmentVisitorContext visitorContext = AssignmentVisitor.processAssignments(
                node.updateAssignments(),
                node.insertAssignments()
        );
        assert node.updateAssignments() != null | node.insertAssignments() != null;
        BulkShardProcessor.BulkRequestBuilder<ShardUpsertRequest> bulkRequestBuilder = new ShardUpsertRequest.Builder(
                visitorContext.dataTypes(),
                visitorContext.columnIndicesToStream(),
                settings.getAsTime("insert_by_query.request_timeout", BulkShardRequest.DEFAULT_TIMEOUT),
                node.isBulkRequest() || node.updateAssignments() != null,
                false,
                node.updateAssignments(),
                node.insertAssignments(),
                null
        );
        bulkShardProcessor = new BulkShardProcessor<>(
                clusterService,
                settings,
                transportBulkCreateIndicesAction,
                shardingProjector,
                node.isPartitionedTable(),
                node.items().size(),
                bulkRetryCoordinatorPool,
                bulkRequestBuilder,
                transportShardUpsertActionDelegate);

        if (!node.isBulkRequest()) {
            final SettableFuture<TaskResult> futureResult = SettableFuture.create();
            List<ListenableFuture<TaskResult>> resultList = new ArrayList<>(1);
            resultList.add(futureResult);
            Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
                @Override
                public void onSuccess(@Nullable BitSet result) {
                    if (result == null) {
                        futureResult.set(TaskResult.ROW_COUNT_UNKNOWN);
                    } else {
                        futureResult.set(new RowCountResult(result.cardinality()));
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    futureResult.setException(t);
                }
            });
            return resultList;
        } else {
            final int numResults = node.items().size();
            final List<ListenableFuture<TaskResult>> resultList = new ArrayList<>(numResults);
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
                        SettableFuture<TaskResult> future = (SettableFuture<TaskResult>) resultList.get(i);
                        future.set(result.get(i) ? TaskResult.ONE_ROW : TaskResult.FAILURE);
                    }
                }

                private void setAllToFailed(@Nullable Throwable throwable) {
                    if (throwable == null) {
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
                }
            });
            return resultList;
        }
    }

    private void createIndexAndExecuteUpsertRequest(final UpsertByIdNode.Item item,
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
    public List<ListenableFuture<TaskResult>> result() {
        return resultList;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("UpsertByIdTask can't have an upstream result");
    }
}
