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

package io.crate.planner.node.dml;

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.where.DocKeys;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dml.ShardRequestExecutor;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.engine.indexing.ShardingUpsertExecutor;
import io.crate.expression.symbol.Assignments;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class UpdateById implements Plan {

    private final DocTableInfo table;
    private final Map<Reference, Symbol> assignmentByTargetCol;
    private final DocKeys docKeys;
    private final Assignments assignments;

    public UpdateById(DocTableInfo table, Map<Reference, Symbol> assignmentByTargetCol, DocKeys docKeys) {
        this.table = table;
        this.assignments = Assignments.convert(assignmentByTargetCol);
        this.assignmentByTargetCol = assignmentByTargetCol;
        this.docKeys = docKeys;
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
        createExecutor(dependencies, plannerContext)
            .execute(consumer, params, subQueryResults);
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
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(clusterService.state().metaData().settings()),
            ShardUpsertRequest.DuplicateKeyAction.UPDATE_OR_FAIL,
            true,
            assignments.targetNames(),
            null, // missing assignments are for INSERT .. ON DUPLICATE KEY UPDATE
            plannerContext.jobId(),
            false
        );
        UpdateRequests updateRequests = new UpdateRequests(requestBuilder, table, assignments);
        return new ShardRequestExecutor<>(
            clusterService,
            txnCtx,
            dependencies.functions(),
            table,
            updateRequests,
            dependencies.transportActionProvider().transportShardUpsertAction()::execute,
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
                            Long version,
                            Long seqNo,
                            Long primaryTerm) {
            ShardUpsertRequest.Item item = new ShardUpsertRequest.Item(id,
                                                                       assignmentSources,
                                                                       null,
                                                                       version,
                                                                       seqNo,
                                                                       primaryTerm);
            request.add(location, item);
        }
    }
}
