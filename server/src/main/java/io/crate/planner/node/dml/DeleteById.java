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

import io.crate.analyze.where.DocKeys;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dml.ShardRequestExecutor;
import io.crate.execution.dml.delete.ShardDeleteAction;
import io.crate.execution.dml.delete.ShardDeleteRequest;
import io.crate.execution.engine.indexing.ShardingUpsertExecutor;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;

import org.elasticsearch.cluster.service.ClusterService;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class DeleteById implements Plan {

    private final DocTableInfo table;
    private final DocKeys docKeys;

    public DeleteById(DocTableInfo table, DocKeys docKeys) {
        this.table = table;
        this.docKeys = docKeys;
    }

    public DocTableInfo table() {
        return table;
    }

    public DocKeys docKeys() {
        return docKeys;
    }

    @Override
    public StatementType type() {
        return StatementType.DELETE;
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
            .executeBulk(bulkParams, subQueryResults, false);
    }

    private ShardRequestExecutor<ShardDeleteRequest> createExecutor(DependencyCarrier dependencies,
                                                                    PlannerContext plannerContext) {
        ClusterService clusterService = dependencies.clusterService();
        TimeValue requestTimeout = ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING
            .get(clusterService.state().metadata().settings());
        return new ShardRequestExecutor<>(
            clusterService,
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            table,
            new DeleteRequests(plannerContext.jobId(), requestTimeout),
            (request, listener) -> dependencies.client().execute(ShardDeleteAction.INSTANCE, request)
                .whenComplete(listener),
            docKeys
        );
    }

    static class DeleteRequests implements ShardRequestExecutor.RequestGrouper<ShardDeleteRequest> {

        private final UUID jobId;
        private final TimeValue requestTimeout;

        DeleteRequests(UUID jobId, TimeValue requestTimeout) {
            this.jobId = jobId;
            this.requestTimeout = requestTimeout;
        }

        @Override
        public ShardDeleteRequest newRequest(ShardId shardId) {
            ShardDeleteRequest request = new ShardDeleteRequest(shardId, jobId);
            request.timeout(requestTimeout);
            return request;
        }

        @Override
        public void bind(Row parameters, SubQueryResults subQueryResults) {
        }

        @Override
        public void addItem(ShardDeleteRequest request,
                            int location,
                            String id,
                            long version,
                            long seqNo,
                            long primaryTerm) {
            ShardDeleteRequest.Item item = new ShardDeleteRequest.Item(id);
            item.version(version);
            item.seqNo(seqNo);
            item.primaryTerm(primaryTerm);
            request.add(location, item);
        }
    }
}
