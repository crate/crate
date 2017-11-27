/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.where.DocKeys;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.executor.transport.task.DeleteByIdTask;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;

import java.util.List;
import java.util.Map;
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
    public void execute(DependencyCarrier executor,
                        PlannerContext plannerContext,
                        RowConsumer consumer,
                        Row params,
                        Map<SelectSymbol, Object> valuesBySubQuery) {
        DeleteByIdTask task = new DeleteByIdTask(
            plannerContext.jobId(),
            executor.clusterService(),
            executor.functions(),
            executor.transportActionProvider().transportShardDeleteAction(),
            this
        );
        task.execute(consumer, params, valuesBySubQuery);
    }

    @Override
    public List<CompletableFuture<Long>> executeBulk(DependencyCarrier executor,
                                                     PlannerContext plannerContext,
                                                     List<Row> bulkParams,
                                                     Map<SelectSymbol, Object> valuesBySubQuery) {
        DeleteByIdTask task = new DeleteByIdTask(
            plannerContext.jobId(),
            executor.clusterService(),
            executor.functions(),
            executor.transportActionProvider().transportShardDeleteAction(),
            this
        );
        return task.executeBulk(bulkParams, valuesBySubQuery);
    }
}
