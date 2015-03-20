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

import io.crate.core.collections.Row;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.uid.Versions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpsertByIdNode extends DMLPlanNode {

    /**
     * A single update item.
     * TODO: use DockKeys here or a Bucket
     */
    public static class Item implements Row {

        private final String index;
        private long version = Versions.MATCH_ANY;
        private Object[] row;

        Item(String index, Object[] row, @Nullable Long version) {
            this.index = index;
            if (version != null) {
                this.version = version;
            }
            this.row = row;
        }

        public String index() {
            return index;
        }

        public long version() {
            return version;
        }

        @Override
        public int size() {
            return row.length;
        }

        @Override
        public Object get(int index) {
            return row[index];
        }

        @Override
        public Object[] materialize() {
            return row;
        }
    }


    private final boolean partitionedTable;
    private final boolean bulkRequest;
    private final List<Item> items;
    private final List<ColumnIdent> primaryKeyIdents;
    private final List<Symbol> primaryKeySymbols;
    @Nullable
    private final Symbol routingSymbol;
    @Nullable
    private final Map<Reference, Symbol> updateAssignments;
    @Nullable
    private final Map<Reference, Symbol> insertAssignments;

    public UpsertByIdNode(boolean partitionedTable,
                          boolean bulkRequest,
                          List<ColumnIdent> primaryKeyIdents,
                          List<Symbol> primaryKeySymbols,
                          @Nullable Symbol routingSymbol,
                          @Nullable Map<Reference, Symbol> updateAssignments,
                          @Nullable Map<Reference, Symbol> insertAssignments) {
        this.partitionedTable = partitionedTable;
        this.bulkRequest = bulkRequest;
        this.primaryKeyIdents = primaryKeyIdents;
        this.primaryKeySymbols = primaryKeySymbols;
        this.routingSymbol = routingSymbol;
        this.updateAssignments = updateAssignments;
        this.insertAssignments = insertAssignments;
        this.items = new ArrayList<>();
    }

    public List<ColumnIdent> primaryKeyIdents() {
        return primaryKeyIdents;
    }

    public List<Symbol> primaryKeySymbols() {
        return primaryKeySymbols;
    }

    @Nullable
    public Symbol routingSymbol() {
        return routingSymbol;
    }

    @Nullable
    public Map<Reference, Symbol> updateAssignments() {
        return updateAssignments;
    }

    @Nullable
    public Map<Reference, Symbol> insertAssignments() {
        return insertAssignments;
    }

    public boolean isPartitionedTable() {
        return partitionedTable;
    }

    public boolean isBulkRequest() {
        return bulkRequest;
    }

    public void add(String index, Object[] row, @Nullable Long version) {
        items.add(new Item(index, row, version));
    }

    public List<Item> items() {
        return items;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitUpsertByIdNode(this, context);
    }
}
