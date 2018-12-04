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

import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dml.ShardRequestExecutor;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.engine.indexing.ShardingUpsertExecutor;
import io.crate.expression.symbol.Assignments;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Functions;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.node.dml.UpdateById;
import io.crate.planner.operators.SubQueryResults;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;


public class UpdateByIdTask {

    private final TransactionContext txnCtx;
    private final ClusterService clusterService;
    private final Functions functions;
    private final TransportShardUpsertAction shardUpsertAction;
    private final UpdateById updateById;
    private final Function<Boolean, ShardUpsertRequest.Builder> createBuilder;
    private final Assignments assignments;

    public UpdateByIdTask(UUID jobId,
                          TransactionContext txnCtx,
                          ClusterService clusterService,
                          Functions functions,
                          TransportShardUpsertAction shardUpsertAction,
                          UpdateById updateById) {
        this.txnCtx = txnCtx;
        this.clusterService = clusterService;
        this.functions = functions;
        this.shardUpsertAction = shardUpsertAction;
        this.updateById = updateById;
        assignments = Assignments.convert(updateById.assignmentByTargetCol());
        createBuilder = (continueOnError) ->
            new ShardUpsertRequest.Builder(
                txnCtx.userName(),
                txnCtx.currentSchema(),
                ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(clusterService.state().metaData().settings()),
                DuplicateKeyAction.UPDATE_OR_FAIL,
                continueOnError,
                assignments.targetNames(),
                null, // missing assignments are for INSERT .. ON DUPLICATE KEY UPDATE
                jobId,
                false
            );
    }

    public void execute(RowConsumer consumer, Row parameters, SubQueryResults subQueryResults) {
        UpdateRequests updateRequests = new UpdateRequests(createBuilder.apply(true), updateById.table(), assignments);
        ShardRequestExecutor<ShardUpsertRequest> executor = new ShardRequestExecutor<>(
            clusterService,
            txnCtx,
            functions,
            updateById.table(),
            updateRequests,
            shardUpsertAction::execute,
            updateById.docKeys()
        );
        executor.execute(consumer, parameters, subQueryResults);
    }

    public List<CompletableFuture<Long>> executeBulk(List<Row> bulkParams, SubQueryResults subQueryResults) {
        UpdateRequests updateRequests = new UpdateRequests(createBuilder.apply(true), updateById.table(), assignments);
        ShardRequestExecutor<ShardUpsertRequest> executor = new ShardRequestExecutor<>(
            clusterService,
            txnCtx,
            functions,
            updateById.table(),
            updateRequests,
            shardUpsertAction::execute,
            updateById.docKeys()
        );
        return executor.executeBulk(bulkParams, subQueryResults);
    }

    private static class UpdateRequests implements ShardRequestExecutor.RequestGrouper<ShardUpsertRequest> {

        private final ShardUpsertRequest.Builder requestBuilder;
        private final DocTableInfo table;
        private final Assignments assignments;

        private Symbol[] assignmentSources;

        UpdateRequests(ShardUpsertRequest.Builder requestBuilder, DocTableInfo table, Assignments assignments) {
            this.requestBuilder = requestBuilder;
            this.table = table;
            this.assignments = assignments;
        }

        @Override
        public ShardUpsertRequest newRequest(ShardId shardId) {
            return requestBuilder.newRequest(shardId);
        }

        @Override
        public void bind(Row parameters, SubQueryResults subQueryResults) {
            assignmentSources = assignments.bindSources(table, parameters, subQueryResults);
        }

        @Override
        public void addItem(ShardUpsertRequest request, int location, String id, Long version) {
            ShardUpsertRequest.Item item = new ShardUpsertRequest.Item(id, assignmentSources, null, version);
            request.add(location, item);
        }
    }
}
