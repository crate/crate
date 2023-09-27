/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.dml;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.execution.engine.indexing.ShardDMLExecutor.maybeRaiseFailure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.analyze.where.DocKeys;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.data.RowN;
import io.crate.execution.support.MultiActionListener;
import io.crate.execution.support.OneRowActionListener;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryResults;

/**
 * Utility class to group requests by shardId and execute them.
 * This is for `byId` type requests where {@link DocKeys} are available.
 */
public class ShardRequestExecutor<Req> {

    private final ClusterService clusterService;
    private final TransactionContext txnCtx;
    private final NodeContext nodeCtx;
    private final DocTableInfo table;
    private final RequestGrouper<Req> grouper;
    private final BiConsumer<Req, ActionListener<ShardResponse>> transportAction;
    private final DocKeys docKeys;

    public interface RequestGrouper<R> {

        R newRequest(ShardId shardId);

        /**
         * (optional): bind the parameters.
         *             this is called once per params in the bulkParameters
         */
        void bind(Row parameters, SubQueryResults subQueryResults);

        /**
         * Creates and adds a new item to the request; This is called once per docKey per params.
         */
        void addItem(R request, int location, String id, long version, long seqNo, long primaryTerm);
    }

    public ShardRequestExecutor(ClusterService clusterService,
                                TransactionContext txnCtx,
                                NodeContext nodeCtx,
                                DocTableInfo table,
                                RequestGrouper<Req> grouper,
                                BiConsumer<Req, ActionListener<ShardResponse>> transportAction,
                                DocKeys docKeys) {
        this.clusterService = clusterService;
        this.txnCtx = txnCtx;
        this.nodeCtx = nodeCtx;
        this.table = table;
        this.grouper = grouper;
        this.transportAction = transportAction;
        this.docKeys = docKeys;
    }

    public void execute(RowConsumer consumer, Row parameters, SubQueryResults subQueryResults) {
        execute(consumer, parameters, subQueryResults, this::rowCountListener);
    }

    public void executeCollectValues(RowConsumer consumer, Row parameters, SubQueryResults subQueryResults) {
        execute(consumer, parameters, subQueryResults, this::resultSetListener);
    }

    private void execute(RowConsumer consumer,
                         Row parameters,
                         SubQueryResults subQueryResults,
                         BiFunction<RowConsumer, Integer, ActionListener<ShardResponse>> f) {
        HashMap<ShardId, Req> requestsByShard = new HashMap<>();
        grouper.bind(parameters, subQueryResults);
        addRequests(0, parameters, requestsByShard, subQueryResults);
        ActionListener<ShardResponse> listener = f.apply(consumer, requestsByShard.size());
        for (Req request : requestsByShard.values()) {
            transportAction.accept(request, listener);
        }
    }

    public List<CompletableFuture<Long>> executeBulk(List<Row> bulkParams, SubQueryResults subQueryResults) {
        HashMap<ShardId, Req> requests = new HashMap<>();
        IntArrayList bulkIndices = new IntArrayList(bulkParams.size() * docKeys.size());
        int location = 0;
        for (int resultIdx = 0; resultIdx < bulkParams.size(); resultIdx++) {
            int prevLocation = location;
            Row params = bulkParams.get(resultIdx);
            grouper.bind(params, subQueryResults);
            location = addRequests(location, params, requests, subQueryResults);
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

    private int addRequests(int location, Row parameters, Map<ShardId, Req> requests, SubQueryResults subQueryResults) {
        for (DocKeys.DocKey docKey : docKeys) {
            String id = docKey.getId(txnCtx, nodeCtx, parameters, subQueryResults);
            if (id == null) {
                continue;
            }
            String routing = docKey.getRouting(txnCtx, nodeCtx, parameters, subQueryResults);
            List<String> partitionValues = docKey.getPartitionValues(txnCtx, nodeCtx, parameters, subQueryResults);
            final String indexName;
            if (partitionValues == null) {
                indexName = table.ident().indexNameOrAlias();
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
                request = grouper.newRequest(shardId);
                requests.put(shardId, request);
            }
            long version = docKey
                .version(txnCtx, nodeCtx, parameters, subQueryResults)
                .orElse(Versions.MATCH_ANY);
            long seqNo = docKey
                .sequenceNo(txnCtx, nodeCtx, parameters, subQueryResults)
                .orElse(SequenceNumbers.UNASSIGNED_SEQ_NO);
            long primaryTerm = docKey
                .primaryTerm(txnCtx, nodeCtx, parameters, subQueryResults)
                .orElse(SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
            grouper.addItem(request, location, id, version, seqNo, primaryTerm);
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

    private ActionListener<ShardResponse> rowCountListener(RowConsumer consumer, int numberResponse) {
        return new MultiActionListener<>(
            numberResponse,
            () -> new long[]{0},
            this::countRows,
            rowCount -> new Row1(rowCount[0]),
            new OneRowActionListener<>(consumer, Function.identity())
        );
    }

    private ActionListener<ShardResponse> resultSetListener(RowConsumer consumer, int numberResponse) {
        return new MultiActionListener<>(
            numberResponse,
            ArrayList::new,
            this::collectResults,
            Function.identity(),
            new ActionListener<List<Row>>() {
                @Override
                public void onResponse(List<Row> rows) {
                    consumer.accept(InMemoryBatchIterator.of(rows, SENTINEL, false), null);
                }

                @Override
                public void onFailure(Exception e) {
                    consumer.accept(null, e);
                }
            }
        );
    }

    private void countRows(long[] rowCount, ShardResponse response) {
        updateOrFail(rowCount, response, (acc,b) -> acc[0] += 1);
    }

    private void collectResults(List<Row> values, ShardResponse response) {
        updateOrFail(values, response, (acc, res) -> {
            List<Object[]> resultRows = res.getResultRows();
            if (resultRows != null) {
                resultRows.forEach(x -> acc.add(new RowN(x)));
            }
        });
    }

    private static <A> void updateOrFail(A acc, ShardResponse response, BiConsumer<A, ShardResponse> f) {
        maybeRaiseFailure(response.failure());
        for (int i = 0; i < response.itemIndices().size(); i++) {
            ShardResponse.Failure failure = response.failures().get(i);
            if (failure == null) {
                f.accept(acc, response);
            } else if (!failure.versionConflict() && !(failure.message().contains("Document not found") || failure.message().contains("document missing"))) {
                throw new RuntimeException(failure.message());
            }
        }
    }
}
