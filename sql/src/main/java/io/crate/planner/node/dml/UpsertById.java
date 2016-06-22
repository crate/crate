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

import com.carrotsearch.hppc.ObjectIntHashMap;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.planner.PlanAndPlannedAnalyzedRelation;
import io.crate.planner.PlanVisitor;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.uid.Versions;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class UpsertById extends PlanAndPlannedAnalyzedRelation {

    @Override
    public void addProjection(Projection projection) {
        throw new UnsupportedOperationException("Adding a projection to upsertById is not supported");
    }

    @Override
    public boolean resultIsDistributed() {
        return false;
    }

    @Override
    public UpstreamPhase resultPhase() {
        throw new UnsupportedOperationException("UpsertById doesn't have a resultPhase");
    }

    /**
     * A single update item.
     */
    public static class Item {

        private final String index;
        private final String id;
        private final String routing;
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
                    @Nullable Object[] insertValues) {
            this.index = index;
            this.id = id;
            this.routing = routing;
            this.updateAssignments = updateAssignments;
            if (version != null) {
                this.version = version;
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

        @Nullable
        public Symbol[] updateAssignments() {
            return updateAssignments;
        }

        @Nullable
        public Object[] insertValues() {
            return insertValues;
        }
    }


    private final UUID jobId;
    private final boolean partitionedTable;
    private final int numBulkResponses;
    private final List<Item> items;
    private final int executionPhaseId;
    private final ObjectIntHashMap<String> idToBulkResultIdx;

    @Nullable
    private final String[] updateColumns;
    @Nullable
    private final Reference[] insertColumns;

    public UpsertById(UUID jobId,
                      int executionPhaseId,
                      boolean partitionedTable,
                      int numBulkResponses,
                      ObjectIntHashMap<String> idToBulkResultIdx,
                      @Nullable String[] updateColumns,
                      @Nullable Reference[] insertColumns) {
        this.jobId = jobId;
        this.partitionedTable = partitionedTable;
        this.numBulkResponses = numBulkResponses;
        this.idToBulkResultIdx = idToBulkResultIdx;
        this.updateColumns = updateColumns;
        this.insertColumns = insertColumns;
        this.items = new ArrayList<>();
        this.executionPhaseId = executionPhaseId;
    }

    public UpsertById(UUID jobId,
                      int executionPhaseId,
                      boolean partitionedTable,
                      int numBulkResponses,
                      @Nullable String[] updateColumns,
                      @Nullable Reference[] insertColumns) {
        this(jobId, executionPhaseId, partitionedTable, numBulkResponses, new ObjectIntHashMap<String>(),
            updateColumns, insertColumns);
    }

    @Nullable
    public String[] updateColumns() {
        return updateColumns;
    }

    @Nullable
    public Reference[] insertColumns() {
        return insertColumns;
    }

    public boolean isPartitionedTable() {
        return partitionedTable;
    }

    public int numBulkResponses() {
        return numBulkResponses;
    }

    public int getBulkResultIdxForId(String id) {
        return idToBulkResultIdx.get(id);
    }

    public boolean setBulkResultIdxForId(String id, int bulkResultIdx) {
        return idToBulkResultIdx.putIfAbsent(id, bulkResultIdx);
    }

    public void add(String index,
                    String id,
                    String routing,
                    Symbol[] updateAssignments,
                    @Nullable Long version) {
        add(index, id, routing, updateAssignments, version, null);
    }

    public void add(String index,
                    String id,
                    String routing,
                    Symbol[] updateAssignments,
                    @Nullable Long version,
                    @Nullable Object[] insertValues) {
        items.add(new Item(index, id, routing, updateAssignments, version, insertValues));
    }

    public List<Item> items() {
        return items;
    }

    public int executionPhaseId() {
        return executionPhaseId;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitUpsertById(this, context);
    }

    @Override
    public UUID jobId() {
        return jobId;
    }
}
