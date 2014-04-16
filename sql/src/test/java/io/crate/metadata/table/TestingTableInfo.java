/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.DataType;
import io.crate.PartitionName;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.*;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.DynamicReference;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestingTableInfo extends AbstractTableInfo {

    private final Routing routing;
    private final String clusteredBy;

    public static Builder builder(TableIdent ident, RowGranularity granularity, Routing routing) {
        return new Builder(ident, granularity, routing);
    }

    public static class Builder {

        private final ImmutableList.Builder<ReferenceInfo> columns = ImmutableList.builder();
        private final ImmutableMap.Builder<ColumnIdent, ReferenceInfo> references = ImmutableMap.builder();
        private final ImmutableList.Builder<ReferenceInfo> partitionedByColumns = ImmutableList.builder();
        private final ImmutableList.Builder<String> primaryKey = ImmutableList.builder();
        private final ImmutableList.Builder<String> partitionedBy = ImmutableList.builder();
        private final ImmutableList.Builder<PartitionName> partitions = ImmutableList.builder();
        private String clusteredBy;


        private final RowGranularity granularity;
        private final TableIdent ident;
        private final Routing routing;
        private boolean isAlias = false;

        public Builder(TableIdent ident, RowGranularity granularity, Routing routing) {
            this.granularity = granularity;
            this.routing = routing;
            this.ident = ident;
        }

        public Builder add(String column, DataType type, List<String> path) {
            return add(column, type, path, ReferenceInfo.ObjectType.DYNAMIC);
        }
        public Builder add(String column, DataType type, List<String> path, ReferenceInfo.ObjectType objectType) {
            return add(column, type, path, objectType, false);
        }
        public Builder add(String column, DataType type, List<String> path,
                           boolean partitionBy) {
            return add(column, type, path, ReferenceInfo.ObjectType.DYNAMIC, partitionBy);
        }

        public Builder add(String column, DataType type, List<String> path,
                           ReferenceInfo.ObjectType objectType, boolean partitionBy) {
            RowGranularity rowGranularity = granularity;
            if (partitionBy) {
                rowGranularity = RowGranularity.SHARD;
            }
            ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(ident, column, path), rowGranularity, type, objectType);
            if (info.ident().isColumn()) {
                columns.add(info);
            }
            references.put(info.ident().columnIdent(), info);
            if (partitionBy) {
                partitionedByColumns.add(info);
                partitionedBy.add(info.ident().columnIdent().fqn());
            }
            return this;
        }

        public Builder addPrimaryKey(String column) {
            primaryKey.add(column);
            return this;
        }

        public Builder clusteredBy(String clusteredBy) {
            this.clusteredBy = clusteredBy;
            return this;
        }

        public Builder isAlias(boolean isAlias) {
            this.isAlias = isAlias;
            return this;
        }

        public Builder addPartitions(String... partitionNames) {
            for (String partitionName : partitionNames) {
                PartitionName partition = PartitionName.fromString(partitionName, ident.name());
                partitions.add(partition);
            }
            return this;
        }

        public TableInfo build() {
            return new TestingTableInfo(columns.build(), partitionedByColumns.build(),
                    references.build(), ident,
                    granularity, routing, primaryKey.build(), clusteredBy, isAlias,
                    partitionedBy.build(), partitions.build());
        }


    }


    private final List<ReferenceInfo> columns;
    private final List<ReferenceInfo> partitionedByColumns;
    private final Map<ColumnIdent, ReferenceInfo> references;
    private final TableIdent ident;
    private final RowGranularity granularity;
    private final List<String> primaryKey;
    private final boolean isAlias;
    private final boolean hasAutoGeneratedPrimaryKey;
    private final List<String> partitionedBy;
    private final List<PartitionName> partitions;


    public TestingTableInfo(List<ReferenceInfo> columns,
                            List<ReferenceInfo> partitionedByColumns,
                            Map<ColumnIdent, ReferenceInfo> references,
                            TableIdent ident, RowGranularity granularity,
                            Routing routing,
                            List<String> primaryKey,
                            String clusteredBy,
                            boolean isAlias,
                            List<String> partitionedBy,
                            List<PartitionName> partitions) {
        this.columns = columns;
        this.partitionedByColumns = partitionedByColumns;
        this.references = references;
        this.ident = ident;
        this.granularity = granularity;
        this.routing = routing;
        this.primaryKey = primaryKey;
        this.clusteredBy = clusteredBy;
        this.isAlias = isAlias;
        this.hasAutoGeneratedPrimaryKey = (primaryKey == null || primaryKey.size()==0);
        this.partitionedBy = partitionedBy;
        this.partitions = partitions;
    }

    @Override
    public ReferenceInfo getColumnInfo(ColumnIdent columnIdent) {
        return references.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }


    @Override
    public List<ReferenceInfo> partitionedByColumns() {
        return partitionedByColumns;
    }

    @Override
    public boolean isPartitioned() {
        return !partitionedByColumns.isEmpty();
    }

    @Override
    public RowGranularity rowGranularity() {
        return granularity;
    }

    @Override
    public TableIdent ident() {
        return ident;
    }

    @Override
    public Routing getRouting(WhereClause whereClause) {
        return routing;
    }

    @Override
    public List<String> primaryKey() {
        return primaryKey;
    }

    @Override
    public boolean hasAutoGeneratedPrimaryKey() {
        return hasAutoGeneratedPrimaryKey;
    }

    @Override
    public String clusteredBy() {
        return clusteredBy;
    }

    @Override
    public boolean isAlias() {
        return isAlias;
    }

    @Override
    public String[] concreteIndices() {
        return new String[]{ident.name()};
    }

    @Override
    public DynamicReference getDynamic(ColumnIdent ident) {
        if (!ident.isColumn()) {
            ColumnIdent parentIdent = ident.getParent();
            ReferenceInfo parentInfo = getColumnInfo(parentIdent);
            if (parentInfo != null && parentInfo.objectType() == ReferenceInfo.ObjectType.STRICT) {
                throw new ColumnUnknownException(ident().name(), ident.fqn());
            }
        }
        return new DynamicReference(new ReferenceIdent(ident(), ident), rowGranularity());
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return references.values().iterator();
    }

    @Override
    public List<String> partitionedBy() {
        return partitionedBy;
    }

    @Override
    public List<PartitionName> partitions() {
        return partitions;
    }

}
