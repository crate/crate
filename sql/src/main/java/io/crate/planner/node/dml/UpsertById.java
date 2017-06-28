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

import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.planner.PlanVisitor;
import io.crate.planner.UnnestablePlan;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.uid.Versions;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class UpsertById extends UnnestablePlan {

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
    private final int numBulkResponses;
    private final List<Item> items;
    private final List<Integer> bulkIndices;

    @Nullable
    private final String[] updateColumns;
    @Nullable
    private final Reference[] insertColumns;

    public UpsertById(UUID jobId,
                      int numBulkResponses,
                      List<Integer> bulkIndices,
                      @Nullable String[] updateColumns,
                      @Nullable Reference[] insertColumns) {
        this.jobId = jobId;
        this.numBulkResponses = numBulkResponses;
        this.bulkIndices = bulkIndices;
        this.updateColumns = updateColumns;
        this.insertColumns = insertColumns;
        this.items = new ArrayList<>();
    }

    public static UpsertById forUpdate(UUID jobId, int numBulkResponses, @Nullable String[] updateColumns) {
        return new UpsertById(jobId, numBulkResponses, new ArrayList<>(), updateColumns, null);
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

    public List<Integer> bulkIndices() {
        return bulkIndices;
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

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitUpsertById(this, context);
    }

    @Override
    public UUID jobId() {
        return jobId;
    }
}
