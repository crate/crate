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

package io.crate.execution.dml.upsert;

import io.crate.action.FutureActionListener;
import io.crate.action.LimitedExponentialBackoff;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.engine.indexing.ShardingUpsertExecutor;
import io.crate.execution.support.RetryListener;
import io.crate.metadata.IndexParts;
import io.crate.metadata.TransactionContext;
import io.crate.planner.node.dml.LegacyUpsertById;
import io.crate.planner.node.dml.UpdateById;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreatePartitionsRequest;
import org.elasticsearch.action.admin.indices.create.CreatePartitionsResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreatePartitionsAction;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.crate.concurrent.CompletableFutures.failedFuture;
import static io.crate.data.SentinelRow.SENTINEL;

public class LegacyUpsertByIdTask {

    private static final Logger LOGGER = LogManager.getLogger(UpdateById.class);
    private static final BackoffPolicy BACK_OFF_POLICY = LimitedExponentialBackoff.limitedExponential(1000);

    private final ClusterService clusterService;
    private final ShardUpsertRequest.Builder reqBuilder;
    private final TransportCreatePartitionsAction createIndicesAction;
    private final List<LegacyUpsertById.Item> items;
    private final ScheduledExecutorService scheduler;
    private final BulkRequestExecutor<ShardUpsertRequest> upsertAction;
    private final int numBulkResponses;
    private final List<Integer> bulkIndices;
    private final boolean isUpdate;
    private final boolean isDebugEnabled;
    private final boolean isPartitioned;
    private final UUID jobId;

    public LegacyUpsertByIdTask(TransactionContext txnCtx,
                                UUID jobId,
                                LegacyUpsertById upsertById,
                                ClusterService clusterService,
                                ScheduledExecutorService scheduler,
                                Settings settings,
                                BulkRequestExecutor<ShardUpsertRequest> transportShardUpsertAction,
                                TransportCreatePartitionsAction transportCreatePartitionsAction) {
        this.jobId = jobId;
        this.scheduler = scheduler;
        this.upsertAction = transportShardUpsertAction;
        this.createIndicesAction = transportCreatePartitionsAction;
        this.clusterService = clusterService;
        this.items = upsertById.items();
        this.bulkIndices = upsertById.bulkIndices();
        this.numBulkResponses = upsertById.numBulkResponses();
        this.isUpdate = upsertById.insertColumns() == null;
        this.isDebugEnabled = LOGGER.isDebugEnabled();
        this.isPartitioned = upsertById.isPartitioned();

        reqBuilder = new ShardUpsertRequest.Builder(
            txnCtx.sessionSettings(),
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(settings),
            upsertById.isIgnoreDuplicateKeys() ? DuplicateKeyAction.IGNORE : DuplicateKeyAction.UPDATE_OR_FAIL,
            upsertById.numBulkResponses() > 0 || items.size() > 1,
            upsertById.updateColumns(),
            upsertById.insertColumns(),
            jobId,
            false
        );
    }

    public void execute(final RowConsumer consumer) {
        doExecute().whenComplete((r, f) -> {
            if (f == null) {
                consumer.accept(InMemoryBatchIterator.of(new Row1((long) r.cardinality()), SENTINEL), null);
            } else {
                consumer.accept(null, f);
            }
        });
    }

    public List<CompletableFuture<Long>> executeBulk() {
        final List<CompletableFuture<Long>> results = prepareResultList(numBulkResponses);
        doExecute().whenComplete((responses, f) -> {
            if (f == null) {
                long[] resultRowCount = createBulkResponse(responses);
                for (int i = 0; i < numBulkResponses; i++) {
                    results.get(i).complete(resultRowCount[i]);
                }
            } else {
                for (CompletableFuture<Long> result : results) {
                    result.completeExceptionally(f);
                }
            }
        });
        return results;
    }

    /**
     * Create bulk-response depending on number of bulk responses
     * <pre>
     *     responses BitSet: [1, 1, 1, 1]
     *
     *     insert into t (x) values (?), (?)   -- bulkParams: [[1, 2], [3, 4]]
     *     Response:
     *      [2, 2]
     *
     *     insert into t (x) values (?)        -- bulkParams: [[1], [2], [3], [4]]
     *     Response:
     *      [1, 1, 1, 1]
     * </pre>
     */
    private long[] createBulkResponse(BitSet responses) {
        long[] resultRowCount = new long[numBulkResponses];
        Arrays.fill(resultRowCount, 0L);
        for (int i = 0; i < items.size(); i++) {
            int resultIdx = bulkIndices.get(i);
            if (responses.get(i)) {
                resultRowCount[resultIdx]++;
            } else {
                resultRowCount[resultIdx] = Row1.ERROR;
            }
        }
        return resultRowCount;
    }

    private static List<CompletableFuture<Long>> prepareResultList(int numResponses) {
        ArrayList<CompletableFuture<Long>> results = new ArrayList<>(numResponses);
        for (int i = 0; i < numResponses; i++) {
            results.add(new CompletableFuture<>());
        }
        return results;
    }

    private CompletableFuture<BitSet> doExecute() {
        MetaData metaData = clusterService.state().getMetaData();
        List<String> indicesToCreate = new ArrayList<>();
        for (LegacyUpsertById.Item item : items) {
            String index = item.index();
            if (isPartitioned && metaData.hasIndex(index) == false) {
                indicesToCreate.add(index);
            }
        }
        if (indicesToCreate.isEmpty() == false) {
            return createPendingIndices(indicesToCreate).thenCompose(resp -> createAndSendRequests());
        } else {
            return createAndSendRequests();
        }
    }

    private CompletableFuture<BitSet> createAndSendRequests() {
        Map<ShardId, ShardUpsertRequest> requestsByShard;
        try {
            requestsByShard = groupRequests();
        } catch (Throwable t) {
            return failedFuture(t);
        }
        if (requestsByShard.isEmpty()) {
            return CompletableFuture.completedFuture(new BitSet(0));
        }
        CompletableFuture<BitSet> result = new CompletableFuture<>();
        AtomicInteger numRequests = new AtomicInteger(requestsByShard.size());
        AtomicReference<Throwable> lastFailure = new AtomicReference<>(null);
        final BitSet responses = new BitSet();

        for (Iterator<Map.Entry<ShardId, ShardUpsertRequest>> it = requestsByShard.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<ShardId, ShardUpsertRequest> entry = it.next();
            ShardUpsertRequest request = entry.getValue();
            it.remove();

            ActionListener<ShardResponse> listener = new ActionListener<ShardResponse>() {
                @Override
                public void onResponse(ShardResponse shardResponse) {
                    Throwable failure = shardResponse.failure();
                    if (failure == null) {
                        synchronized (responses) {
                            ShardResponse.markResponseItemsAndFailures(shardResponse, responses);
                        }
                    } else {
                        lastFailure.set(failure);
                    }
                    countdown();
                }

                @Override
                public void onFailure(Exception e) {
                    lastFailure.set(e);
                    countdown();
                }

                private void countdown() {
                    if (numRequests.decrementAndGet() == 0) {
                        Throwable throwable = lastFailure.get();
                        if (throwable == null) {
                            result.complete(responses);
                        } else {
                            throwable = SQLExceptions.unwrap(throwable, t -> t instanceof RuntimeException);
                            // we want to report duplicate key exceptions
                            if (!SQLExceptions.isDocumentAlreadyExistsException(throwable) &&
                                (updateAffectedNoRows(throwable)
                                 || partitionWasDeleted(throwable, request.index())
                                 || mixedArgumentTypesFailure(throwable, request.items()))) {
                                result.complete(responses);
                            } else {
                                result.completeExceptionally(throwable);
                            }
                        }
                    }
                }
            };
            upsertAction.execute(
                request,
                new RetryListener<>(
                    scheduler,
                    actionListener -> upsertAction.execute(request, actionListener),
                    listener,
                    BACK_OFF_POLICY
                )
            );
        }

        return result;
    }

    private boolean mixedArgumentTypesFailure(Throwable throwable, List<ShardUpsertRequest.Item> items) {
        boolean mixedArgFailure =
            throwable instanceof ClassCastException || throwable instanceof NotSerializableExceptionWrapper;
        if (mixedArgFailure && isDebugEnabled) {
            LOGGER.debug("ShardUpsert: {} items failed", throwable, items.size());
        }
        return mixedArgFailure;
    }

    private boolean partitionWasDeleted(Throwable throwable, String index) {
        return throwable instanceof IndexNotFoundException && IndexParts.isPartitioned(index);
    }

    private boolean updateAffectedNoRows(Throwable throwable) {
        return isUpdate &&
               throwable instanceof DocumentMissingException || throwable instanceof VersionConflictEngineException;
    }

    private Map<ShardId, ShardUpsertRequest> groupRequests() {
        ClusterState state = clusterService.state();
        Map<ShardId, ShardUpsertRequest> requestsByShard = new HashMap<>();
        for (int i = 0; i < items.size(); i++) {
            LegacyUpsertById.Item item = items.get(i);

            String index = item.index();
            ShardId shardId;
            try {
                shardId = getShardId(state, index, item.id(), item.routing());
            } catch (IndexNotFoundException e) {
                if (IndexParts.isPartitioned(index)) {
                    continue;
                } else {
                    throw e;
                }
            }

            ShardUpsertRequest request = requestsByShard.get(shardId);
            if (request == null) {
                request = reqBuilder.newRequest(shardId);
                requestsByShard.put(shardId, request);
            }
            request.add(i,
                        new ShardUpsertRequest.Item(item.id(),
                                                    item.updateAssignments(),
                                                    item.insertValues(),
                                                    item.version(),
                                                    item.seqNo(),
                                                    item.primaryTerm()));
        }
        return requestsByShard;
    }

    private CompletableFuture<CreatePartitionsResponse> createPendingIndices(Collection<String> indices) {
        FutureActionListener<CreatePartitionsResponse, CreatePartitionsResponse> listener = new FutureActionListener<>(r -> r);
        createIndicesAction.execute(new CreatePartitionsRequest(indices, jobId), listener);
        return listener;
    }

    private ShardId getShardId(ClusterState state, String index, String id, String routing) {
        return clusterService.operationRouting().indexShards(
            state,
            index,
            id,
            routing
        ).shardId();
    }
}
