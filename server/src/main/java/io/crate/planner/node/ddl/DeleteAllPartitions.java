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

package io.crate.planner.node.ddl;

import java.util.List;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;

import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.data.SentinelRow;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;

public final class DeleteAllPartitions implements Plan {

    private final List<String> partitions;

    public DeleteAllPartitions(List<String> partitions) {
        this.partitions = partitions;
    }

    public List<String> partitions() {
        return partitions;
    }

    @Override
    public StatementType type() {
        return StatementType.DELETE;
    }

    @Override
    public void executeOrFail(DependencyCarrier executor,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        if (partitions.isEmpty()) {
            consumer.accept(InMemoryBatchIterator.of(new Row1(0L), SentinelRow.SENTINEL), null);
        } else {
            /*
             * table is partitioned, in case of concurrent "delete from partitions"
             * it could be that some partitions are already deleted,
             * so ignore it if some are missing
             */
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(partitions().toArray(new String[0]))
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
            executor.client().execute(DeleteIndexAction.INSTANCE, deleteIndexRequest)
                .whenComplete(new OneRowActionListener<>(consumer, r -> Row1.ROW_COUNT_UNKNOWN));
        }
    }
}
