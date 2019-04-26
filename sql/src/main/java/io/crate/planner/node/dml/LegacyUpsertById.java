/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dml.upsert.LegacyUpsertByIdTask;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * @deprecated This should be replaced with a variant that doesn't depend on parameter values.
 *             Similar to how {@link DeleteById} and {@link UpdateById} work.
 */
@Deprecated
public class LegacyUpsertById implements Plan {

    /**
     * A single update item.
     */
    public static class Item {

        private final String index;
        private final String id;
        private final String routing;
        private Long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        private Long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
        private long version = Versions.MATCH_ANY;
        @Nullable
        private final Symbol[] updateAssignments;
        @Nullable
        private Object[] insertValues;

        Item(String index,
             String id,
             String routing,
             @Nullable Symbol[] updateAssignments,
             @Nullable Long version,
             @Nullable Long seqNo,
             @Nullable Long primaryTerm,
             @Nullable Object[] insertValues) {
            this.index = index;
            this.id = id;
            this.routing = routing;
            this.updateAssignments = updateAssignments;
            if (version != null) {
                this.version = version;
            }
            if (seqNo != null) {
                this.seqNo = seqNo;
            }
            if (primaryTerm != null) {
                this.primaryTerm = primaryTerm;
            }
            this.insertValues = insertValues;
        }

        public String index() {
            return index;
        }

        public String id() {
            return id;
        }

        public String routing() {
            return routing;
        }

        public long version() {
            return version;
        }

        public Long seqNo() {
            return seqNo;
        }

        public Long primaryTerm() {
            return primaryTerm;
        }

        @Nullable
        public Symbol[] updateAssignments() {
            return updateAssignments;
        }

        @Nullable
        public Object[] insertValues() {
            return insertValues;
        }
    }


    private final int numBulkResponses;
    private final boolean isPartitioned;
    private final List<Item> items;
    private final List<Integer> bulkIndices;

    private final boolean ignoreDuplicateKeys;

    @Nullable
    private final String[] updateColumns;
    @Nullable
    private final Reference[] insertColumns;

    public LegacyUpsertById(int numBulkResponses,
                            boolean isPartitioned,
                            List<Integer> bulkIndices,
                            boolean ignoreDuplicateKeys,
                            @Nullable String[] updateColumns,
                            @Nullable Reference[] insertColumns) {
        this.numBulkResponses = numBulkResponses;
        this.isPartitioned = isPartitioned;
        this.bulkIndices = bulkIndices;
        this.ignoreDuplicateKeys = ignoreDuplicateKeys;
        this.updateColumns = updateColumns;
        this.insertColumns = insertColumns;
        this.items = new ArrayList<>();
    }

    public boolean isIgnoreDuplicateKeys() {
        return ignoreDuplicateKeys;
    }

    @Nullable
    public String[] updateColumns() {
        return updateColumns;
    }

    @Nullable
    public Reference[] insertColumns() {
        return insertColumns;
    }

    public int numBulkResponses() {
        return numBulkResponses;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public List<Integer> bulkIndices() {
        return bulkIndices;
    }

    public void add(String index,
                    String id,
                    String routing,
                    @Nullable Symbol[] updateAssignments,
                    @Nullable Long version,
                    @Nullable Long seqNo,
                    @Nullable Long primaryTerm,
                    @Nullable Object[] insertValues) {
        items.add(new Item(index, id, routing, updateAssignments, version, seqNo, primaryTerm, insertValues));
    }

    public List<Item> items() {
        return items;
    }

    @Override
    public StatementType type() {
        return StatementType.INSERT;
    }

    @Override
    public void executeOrFail(DependencyCarrier executor,
                              PlannerContext plannerCtx,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        LegacyUpsertByIdTask task = new LegacyUpsertByIdTask(
            plannerCtx.transactionContext(),
            plannerCtx.jobId(),
            this,
            executor.clusterService(),
            executor.scheduler(),
            executor.settings(),
            executor.transportActionProvider().transportShardUpsertAction()::execute,
            executor.transportActionProvider().transportBulkCreateIndicesAction()
        );
        task.execute(consumer);
    }

    @Override
    public List<CompletableFuture<Long>> executeBulk(DependencyCarrier executor,
                                                     PlannerContext plannerContext,
                                                     List<Row> bulkParams,
                                                     SubQueryResults subQueryResults) {
        LegacyUpsertByIdTask task = new LegacyUpsertByIdTask(
            plannerContext.transactionContext(),
            plannerContext.jobId(),
            this,
            executor.clusterService(),
            executor.scheduler(),
            executor.settings(),
            executor.transportActionProvider().transportShardUpsertAction()::execute,
            executor.transportActionProvider().transportBulkCreateIndicesAction()
        );
        return task.executeBulk();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LegacyUpsertById that = (LegacyUpsertById) o;
        return numBulkResponses == that.numBulkResponses &&
               isPartitioned == that.isPartitioned &&
               ignoreDuplicateKeys == that.ignoreDuplicateKeys &&
               Objects.equals(items, that.items) &&
               Objects.equals(bulkIndices, that.bulkIndices) &&
               Arrays.equals(updateColumns, that.updateColumns) &&
               Arrays.equals(insertColumns, that.insertColumns);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(numBulkResponses, isPartitioned, items, bulkIndices, ignoreDuplicateKeys);
        result = 31 * result + Arrays.hashCode(updateColumns);
        result = 31 * result + Arrays.hashCode(insertColumns);
        return result;
    }
}
