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

import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.uid.Versions;

import java.util.ArrayList;
import java.util.List;

public class UpsertByIdNode extends DMLPlanNode {

    /**
     * A single update item.
     */
    public static class Item {

        private final String index;
        private final String id;
        private final String routing;
        private final Symbol[] assignments;
        private long version = Versions.MATCH_ANY;
        @Nullable
        private Object[] missingAssignments;

        Item(String index,
                    String id,
                    String routing,
                    Symbol[] assignments,
                    @Nullable Long version,
                    @Nullable Object[] missingAssignments) {
            this.index = index;
            this.id = id;
            this.routing = routing;
            this.assignments = assignments;
            if (version != null) {
                this.version = version;
            }
            this.missingAssignments = missingAssignments;
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

        public Symbol[] assignments() {
            return assignments;
        }

        @Nullable
        public Object[] missingAssignments() {
            return missingAssignments;
        }
    }


    private final boolean partitionedTable;
    private final boolean bulkRequest;
    private final List<Item> items;
    private final String[] assignmentsColumns;
    @Nullable
    private final Reference[] missingAssignmentsColumns;

    public UpsertByIdNode(boolean partitionedTable,
                          boolean bulkRequest,
                          String[] assignmentsColumns,
                          @Nullable Reference[] missingAssignmentsColumns) {
        this.partitionedTable = partitionedTable;
        this.bulkRequest = bulkRequest;
        this.assignmentsColumns = assignmentsColumns;
        this.missingAssignmentsColumns = missingAssignmentsColumns;
        this.items = new ArrayList<>();
    }

    public String[] assignmentsColumns() {
        return assignmentsColumns;
    }

    @Nullable
    public Reference[] missingAssignmentsColumns() {
        return missingAssignmentsColumns;
    }

    public boolean isPartitionedTable() {
        return partitionedTable;
    }

    public boolean isBulkRequest() {
        return bulkRequest;
    }

    public void add(String index,
                    String id,
                    String routing,
                    Symbol[] assignments,
                    @Nullable Long version) {
        add(index, id, routing, assignments, version, null);
    }

    public void add(String index,
                    String id,
                    String routing,
                    Symbol[] assignments,
                    @Nullable Long version,
                    @Nullable Object[] missingAssignments) {
        items.add(new Item(index, id, routing, assignments, version, missingAssignments));
    }

    public List<Item> items() {
        return items;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitUpsertByIdNode(this, context);
    }
}
