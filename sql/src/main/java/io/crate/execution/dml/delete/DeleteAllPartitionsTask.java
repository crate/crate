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

package io.crate.execution.dml.delete;

import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.node.ddl.DeleteAllPartitions;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.IndicesOptions;

import static io.crate.data.SentinelRow.SENTINEL;

public final class DeleteAllPartitionsTask {

    private final TransportDeleteIndexAction deleteIndexAction;
    private final DeleteIndexRequest request;

    public DeleteAllPartitionsTask(DeleteAllPartitions plan, TransportDeleteIndexAction deleteIndexAction) {
        this.request = new DeleteIndexRequest(plan.partitions().toArray(new String[0]));
        /*
         * table is partitioned, in case of concurrent "delete from partitions"
         * it could be that some partitions are already deleted,
         * so ignore it if some are missing
         */
        this.request.indicesOptions(IndicesOptions.lenientExpandOpen());
        this.deleteIndexAction = deleteIndexAction;
    }

    public void execute(RowConsumer consumer) {
        if (request.indices().length == 0) {
            consumer.accept(InMemoryBatchIterator.of(new Row1(0L), SENTINEL), null);
        } else {
            deleteIndexAction.execute(
                request, new OneRowActionListener<>(consumer, r -> Row1.ROW_COUNT_UNKNOWN));
        }
    }
}
