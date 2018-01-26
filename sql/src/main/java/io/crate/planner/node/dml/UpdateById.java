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

import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dml.upsert.UpdateByIdTask;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class UpdateById implements Plan {

    private final DocTableInfo table;
    private final Map<Reference, Symbol> assignmentByTargetCol;
    private final DocKeys docKeys;

    public UpdateById(DocTableInfo table, Map<Reference, Symbol> assignmentByTargetCol, DocKeys docKeys) {
        this.table = table;
        this.assignmentByTargetCol = assignmentByTargetCol;
        this.docKeys = docKeys;
    }

    public DocKeys docKeys() {
        return docKeys;
    }

    public DocTableInfo table() {
        return table;
    }

    public Map<Reference, Symbol> assignmentByTargetCol() {
        return assignmentByTargetCol;
    }

    @Override
    public void execute(DependencyCarrier executor,
                        PlannerContext plannerCtx,
                        RowConsumer consumer,
                        Row params,
                        Map<SelectSymbol, Object> valuesBySubQuery) {
        UpdateByIdTask task = new UpdateByIdTask(
            plannerCtx.jobId(),
            executor.clusterService(),
            executor.functions(),
            executor.transportActionProvider().transportShardUpsertAction(),
            this
        );
        task.execute(consumer, params, valuesBySubQuery);
    }

    @Override
    public List<CompletableFuture<Long>> executeBulk(DependencyCarrier executor,
                                                     PlannerContext plannerContext,
                                                     List<Row> bulkParams,
                                                     Map<SelectSymbol, Object> valuesBySubQuery) {
        UpdateByIdTask task = new UpdateByIdTask(
            plannerContext.jobId(),
            executor.clusterService(),
            executor.functions(),
            executor.transportActionProvider().transportShardUpsertAction(),
            this
        );
        return task.executeBulk(bulkParams, valuesBySubQuery);
    }
}
