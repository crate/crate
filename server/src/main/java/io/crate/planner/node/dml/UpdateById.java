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

package io.crate.planner.node.dml;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.where.DocKeys;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dml.ShardRequestExecutor;
import io.crate.execution.dml.upsert.ShardUpsertAction;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.engine.indexing.ShardingUpsertExecutor;
import io.crate.expression.symbol.Assignments;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;

public final class UpdateById implements Plan {

    private final DocTableInfo table;
    private final Map<Reference, Symbol> assignmentByTargetCol;
    private final DocKeys docKeys;
    private final Assignments assignments;
    @Nullable
    private final Symbol[] returnValues;

    public UpdateById(DocTableInfo table,
                      LinkedHashMap<Reference, Symbol> assignmentByTargetCol,
                      DocKeys docKeys,
                      @Nullable List<Symbol> returnValues,
                      NodeContext nodeCtx) {
        this.table = table;
        this.assignments = Assignments.convert(assignmentByTargetCol, nodeCtx);
        this.assignmentByTargetCol = assignmentByTargetCol;
        this.docKeys = docKeys;
        this.returnValues = returnValues == null ? null : returnValues.toArray(new Symbol[0]);
    }

    @VisibleForTesting
    public Map<Reference, Symbol> assignmentByTargetCol() {
        return assignmentByTargetCol;
    }

    @VisibleForTesting
    public DocKeys docKeys() {
        return docKeys;
    }

    @Override
    public StatementType type() {
        return StatementType.UPDATE;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        ShardRequestExecutor<ShardUpsertRequest> executor = createExecutor(dependencies, plannerContext);

        if (returnValues == null) {
            executor.execute(consumer, params, subQueryResults);
        } else {
            executor.executeCollectValues(consumer, params, subQueryResults);
        }
    }

    @Override
    public List<CompletableFuture<Long>> executeBulk(DependencyCarrier dependencies,
                                                     PlannerContext plannerContext,
                                                     List<Row> bulkParams,
                                                     SubQueryResults subQueryResults) {
        return createExecutor(dependencies, plannerContext)
            .executeBulk(bulkParams, subQueryResults);
    }

    private ShardRequestExecutor<ShardUpsertRequest> createExecutor(DependencyCarrier dependencies,
                                                                    PlannerContext plannerContext) {
        ClusterService clusterService = dependencies.clusterService();
        CoordinatorTxnCtx txnCtx = plannerContext.transactionContext();
        ShardUpsertRequest.Builder requestBuilder = new ShardUpsertRequest.Builder(
            txnCtx.sessionSettings(),
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.get(clusterService.state().metadata().settings()),
            ShardUpsertRequest.DuplicateKeyAction.UPDATE_OR_FAIL,
            true,
            assignments.targetNames(),
            null, // missing assignments are for INSERT .. ON DUPLICATE KEY UPDATE
            returnValues,
            plannerContext.jobId()
        );
        UpdateRequests updateRequests = new UpdateRequests(requestBuilder, table, assignments);
        return new ShardRequestExecutor<>(
            clusterService,
            txnCtx,
            dependencies.nodeContext(),
            table,
            updateRequests,
            (request, listener) -> dependencies.client().execute(ShardUpsertAction.INSTANCE, request)
                .whenComplete(listener),
            docKeys
        );
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
        public void addItem(ShardUpsertRequest request,
                            int location,
                            String id,
                            long version,
                            long seqNo,
                            long primaryTerm) {
            ShardUpsertRequest.Item item = ShardUpsertRequest.Item.forUpdate(
                id,
                assignmentSources,
                version,
                seqNo,
                primaryTerm
            );
            request.add(location, item);
        }
    }
}
