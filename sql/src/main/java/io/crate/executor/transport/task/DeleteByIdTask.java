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

package io.crate.executor.transport.task;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import io.crate.analyze.where.DocKeys;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.executor.JobTask;
import io.crate.executor.MultiActionListener;
import io.crate.executor.transport.ShardDeleteRequest;
import io.crate.executor.transport.ShardResponse;
import io.crate.executor.transport.TransportShardDeleteAction;
import io.crate.executor.transport.task.elasticsearch.ESGetTask;
import io.crate.metadata.Functions;
import io.crate.operation.projectors.sharding.ShardingUpsertExecutor;
import io.crate.planner.node.dml.DeleteById;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.crate.data.SentinelRow.SENTINEL;

public class DeleteByIdTask extends JobTask {

    private final TransportShardDeleteAction deleteAction;
    private final ShardDeleteRequest.Builder requestBuilder;
    private final DeleteById deleteById;
    private final ClusterService clusterService;
    private final Functions functions;

    public DeleteByIdTask(ClusterService clusterService,
                          Functions functions,
                          TransportShardDeleteAction deleteAction,
                          DeleteById deleteById) {
        super(deleteById.jobId());
        this.clusterService = clusterService;
        this.functions = functions;
        this.deleteAction = deleteAction;
        this.deleteById = deleteById;
        requestBuilder = new ShardDeleteRequest.Builder(
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(clusterService.state().metaData().settings()),
            jobId()
        );
    }

    @Override
    public void execute(final RowConsumer consumer, Row parameters) {
        HashMap<ShardId, ShardDeleteRequest> requests = new HashMap<>(deleteById.docKeys().size());
        addRequests(0, parameters, requests);
        if (requests.isEmpty()) {
            consumer.accept(InMemoryBatchIterator.of(new Row1(0L), SENTINEL), null);
            return;
        }
        MultiActionListener<ShardResponse, long[], ? super Row> listener = new MultiActionListener<>(
            requests.size(),
            () -> new long[]{0},
            DeleteByIdTask::updateRowCountOrFail,
            s -> new Row1(s[0]),
            new ActionListener<Row>() {
                @Override
                public void onResponse(Row r) {
                    consumer.accept(InMemoryBatchIterator.of(r, SENTINEL), null);
                }

                @Override
                public void onFailure(Exception e) {
                    consumer.accept(null, e);
                }
            }
        );
        for (ShardDeleteRequest request : requests.values()) {
            deleteAction.execute(request, listener);
        }
    }

    static void updateRowCountOrFail(long[] rowCount, ShardResponse response) {
        for (int i = 0; i < response.itemIndices().size(); i++) {
            ShardResponse.Failure failure = response.failures().get(i);
            if (failure == null) {
                rowCount[0] += 1;
            } else if (!failure.versionConflict() && !failure.message().contains("Document not found")) {
                throw new RuntimeException(failure.message());
            }
        }
    }

    private int addRequests(int location, Row parameters, HashMap<ShardId, ShardDeleteRequest> requests) {
        for (DocKeys.DocKey docKey : deleteById.docKeys()) {
            String id = docKey.getId(functions, parameters);
            if (id == null) {
                continue;
            }
            List<BytesRef> partitionValues = docKey.getPartitionValues(functions, parameters);
            String routing = docKey.getRouting(functions, parameters);
            String index = ESGetTask.indexName(deleteById.table(), partitionValues);
            ShardId shardId;
            try {
                shardId = getShardId(clusterService, index, id, routing);
            } catch (IndexNotFoundException e) {
                if (deleteById.table().isPartitioned()) {
                    continue;
                }
                throw e;
            }
            ShardDeleteRequest request = requests.get(shardId);
            if (request == null) {
                request = requestBuilder.newRequest(shardId);
                requests.put(shardId, request);
            }
            ShardDeleteRequest.Item item = new ShardDeleteRequest.Item(id);
            Optional<Long> version = docKey.version(functions, parameters);
            version.ifPresent(item::version);
            request.add(location, item);
            location++;
        }
        return location;
    }

    @Override
    public final List<CompletableFuture<Long>> executeBulk(List<Row> bulkParams) {
        HashMap<ShardId, ShardDeleteRequest> requests = new HashMap<>(bulkParams.size() * deleteById.docKeys().size());
        ArrayList<CompletableFuture<Long>> results = new ArrayList<>(bulkParams.size());
        /* Need to be able to map the responses to the correct result rowCount
         *
         * Example:
         *
         * bulkParams: [ [1, 2], [3, 4] ]
         *   shard0: [0, 2]
         *   shard1: [1]
         *   shard2: [3]
         * ->
         *  3 requests (per shard)
         *  2 bulkParams
         *  2 result futures
         *
         *  resultIdxByLocation:
         *      0 -> 0
         *      1 -> 0
         *      2 -> 1
         *      3 -> 1
         */
        int location = 0;
        IntIntHashMap resultIdxByLocation = new IntIntHashMap(bulkParams.size());
        for (int resultIdx = 0; resultIdx < bulkParams.size(); resultIdx++) {
            results.add(new CompletableFuture<>());
            int prevLocation = location;
            location = addRequests(location, bulkParams.get(resultIdx), requests);
            for (int loc = prevLocation;  loc < location; loc++) {
                resultIdxByLocation.put(loc, resultIdx);
            }
        }
        if (requests.isEmpty()) {
            results.forEach(s -> s.complete(0L));
            return results;
        }
        Collector<ShardResponse, ?, List<ShardResponse>> shardResponseListCollector = Collectors.toList();
        MultiActionListener<ShardResponse, ?, List<ShardResponse>> listener =
            new MultiActionListener<>(requests.size(), shardResponseListCollector, new ActionListener<List<ShardResponse>>() {

                @Override
                public void onResponse(List<ShardResponse> responses) {
                    long[] rowCounts = getRowCounts(responses, bulkParams.size(), resultIdxByLocation);
                    for (int i = 0; i < results.size(); i++) {
                        results.get(i).complete(rowCounts[i]);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    results.forEach(s -> s.completeExceptionally(e));
                }
            });
        for (ShardDeleteRequest request : requests.values()) {
            deleteAction.execute(request, listener);
        }
        return results;
    }

    static long[] getRowCounts(List<ShardResponse> responses,
                               int numResults,
                               IntIntHashMap resultIdxByLocation) {
        long[] rowCounts = new long[numResults];
        for (ShardResponse response : responses) {
            for (IntCursor locCur : response.itemIndices()) {
                ShardResponse.Failure failure = response.failures().get(locCur.index);
                if (failure == null) {
                    assert resultIdxByLocation.containsKey(locCur.value)
                        : "resultIdxByLocation map must have an entry for each location";
                    int resultIdx = resultIdxByLocation.get(locCur.value);
                    rowCounts[resultIdx] += 1;
                }
            }
        }
        return rowCounts;
    }

    private static ShardId getShardId(ClusterService clusterService,
                                      String index,
                                      String id,
                                      String routing) {
        return clusterService.operationRouting().indexShards(
            clusterService.state(),
            index,
            id,
            routing
        ).shardId();
    }
}
