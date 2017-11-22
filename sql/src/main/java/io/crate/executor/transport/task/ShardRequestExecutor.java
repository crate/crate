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

import com.carrotsearch.hppc.IntArrayList;
import io.crate.analyze.where.DocKeys;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.exceptions.SQLExceptions;
import io.crate.executor.MultiActionListener;
import io.crate.executor.transport.OneRowActionListener;
import io.crate.executor.transport.ShardResponse;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Utility class to group requests by shardId and execute them.
 * This is for `byId` type requests where {@link DocKeys} are available.
 */
class ShardRequestExecutor<Req> {

    private final ClusterService clusterService;
    private final Functions functions;
    private final DocTableInfo table;
    private final RequestGrouper<Req> grouper;
    private final BiConsumer<Req, ActionListener<ShardResponse>> transportAction;
    private final DocKeys docKeys;

    interface RequestGrouper<R> {

        R newRequest(ShardId shardId, String routing);

        /**
         * (optional): bind the parameters.
         *             this is called once per params in the bulkParameters
         */
        void bind(Row parameters);

        /**
         * Creates and adds a new item to the request; This is called once per docKey per params.
         */
        void addItem(R request, int location, String id, @Nullable Long version);
    }

    ShardRequestExecutor(ClusterService clusterService,
                         Functions functions,
                         DocTableInfo table,
                         RequestGrouper<Req> grouper,
                         BiConsumer<Req, ActionListener<ShardResponse>> transportAction,
                         DocKeys docKeys) {
        this.clusterService = clusterService;
        this.functions = functions;
        this.table = table;
        this.grouper = grouper;
        this.transportAction = transportAction;
        this.docKeys = docKeys;
    }

    void execute(RowConsumer consumer, Row parameters) {
        HashMap<ShardId, Req> requestsByShard = new HashMap<>();
        grouper.bind(parameters);
        addRequests(0, parameters, requestsByShard);
        MultiActionListener<ShardResponse, long[], ? super Row> listener = new MultiActionListener<>(
            requestsByShard.size(),
            () -> new long[]{0},
            ShardRequestExecutor::updateRowCountOrFail,
            rowCount -> new Row1(rowCount[0]),
            new OneRowActionListener<>(consumer, Function.identity())
        );
        for (Req request : requestsByShard.values()) {
            transportAction.accept(request, listener);
        }
    }

    List<CompletableFuture<Long>> executeBulk(List<Row> bulkParams) {
        HashMap<ShardId, Req> requests = new HashMap<>();
        IntArrayList bulkIndices = new IntArrayList(bulkParams.size() * docKeys.size());
        int location = 0;
        for (int resultIdx = 0; resultIdx < bulkParams.size(); resultIdx++) {
            int prevLocation = location;
            Row params = bulkParams.get(resultIdx);
            grouper.bind(params);
            location = addRequests(location, params, requests);
            for (int i = prevLocation; i < location; i++) {
                bulkIndices.add(resultIdx);
            }
        }
        BulkShardResponseListener listener =
            new BulkShardResponseListener(requests.size(), bulkParams.size(), bulkIndices);
        for (Req req : requests.values()) {
            transportAction.accept(req, listener);
        }
        return listener.rowCountFutures();
    }

    private int addRequests(int location, Row parameters, Map<ShardId, Req> requests) {
        for (DocKeys.DocKey docKey : docKeys) {
            String id = docKey.getId(functions, parameters);
            if (id == null) {
                continue;
            }
            String routing = docKey.getRouting(functions, parameters);
            List<BytesRef> partitionValues = docKey.getPartitionValues(functions, parameters);
            final String indexName;
            if (partitionValues == null) {
                indexName = table.ident().indexName();
            } else {
                indexName = IndexParts.toIndexName(table.ident(), PartitionName.encodeIdent(partitionValues));
            }
            final ShardId shardId;
            try {
                shardId = getShardId(clusterService, indexName, id, routing);
            } catch (IndexNotFoundException e) {
                if (table.isPartitioned()) {
                    continue;
                }
                throw e;
            }
            Req request = requests.get(shardId);
            if (request == null) {
                request = grouper.newRequest(shardId, routing);
                requests.put(shardId, request);
            }
            Long version = docKey.version(functions, parameters).orElse(null);
            grouper.addItem(request, location, id, version);
            location++;
        }
        return location;
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

    private static void updateRowCountOrFail(long[] rowCount, ShardResponse response) {
        Exception exception = response.failure();
        if (exception != null) {
            Throwable t = SQLExceptions.unwrap(exception, e -> e instanceof RuntimeException);
            if (!(t instanceof DocumentMissingException) && !(t instanceof VersionConflictEngineException)) {
                throw new RuntimeException(t);
            }
        }
        for (int i = 0; i < response.itemIndices().size(); i++) {
            ShardResponse.Failure failure = response.failures().get(i);
            if (failure == null) {
                rowCount[0] += 1;
            } else if (!failure.versionConflict() && !(failure.message().contains("Document not found") || failure.message().contains("document missing"))) {
                throw new RuntimeException(failure.message());
            }
        }
    }
}
