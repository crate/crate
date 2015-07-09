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
import io.crate.analyze.AlterPartitionedTableParameterInfo;
import io.crate.analyze.TableParameterInfo;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocIndexMetaData;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.DynamicReference;
import io.crate.types.DataType;
import org.mockito.Answers;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestingTableInfo extends AbstractDynamicTableInfo {

    private final Routing routing;
    private final ColumnIdent clusteredBy;

    public static Builder builder(TableIdent ident, RowGranularity granularity, Routing routing) {
        return new Builder(ident, granularity, routing);
    }

    public static class Builder {

        private final ImmutableList.Builder<ReferenceInfo> columns = ImmutableList.builder();
        private final ImmutableMap.Builder<ColumnIdent, ReferenceInfo> references = ImmutableMap.builder();
        private final ImmutableList.Builder<ReferenceInfo> partitionedByColumns = ImmutableList.builder();
        private final ImmutableList.Builder<ColumnIdent> primaryKey = ImmutableList.builder();
        private final ImmutableList.Builder<ColumnIdent> partitionedBy = ImmutableList.builder();
        private final ImmutableList.Builder<PartitionName> partitions = ImmutableList.builder();
        private final ImmutableMap.Builder<ColumnIdent, IndexReferenceInfo> indexColumns = ImmutableMap.builder();
        private ColumnIdent clusteredBy;


        private final RowGranularity granularity;
        private final TableIdent ident;
        private final Routing routing;
        private boolean isAlias = false;
        private ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;

        private SchemaInfo schemaInfo = mock(SchemaInfo.class, Answers.RETURNS_MOCKS.get());

        public Builder(TableIdent ident, RowGranularity granularity, Routing routing) {
            this.granularity = granularity;
            this.routing = routing;
            this.ident = ident;
        }

        private ReferenceInfo genInfo(ColumnIdent columnIdent, DataType type) {
            return new ReferenceInfo(
                    new ReferenceIdent(ident, columnIdent.name(), columnIdent.path()),
                    RowGranularity.DOC, type
            );
        }

        private void addDocSysColumns() {
            for (Map.Entry<ColumnIdent, DataType> entry : DocSysColumns.COLUMN_IDENTS.entrySet()) {
                references.put(
                        entry.getKey(),
                        genInfo(entry.getKey(), entry.getValue())
                );
            }
        }

        public Builder add(String column, DataType type, List<String> path) {
            return add(column, type, path, ColumnPolicy.DYNAMIC);
        }
        public Builder add(String column, DataType type, List<String> path, ColumnPolicy columnPolicy) {
            return add(column, type, path, columnPolicy, ReferenceInfo.IndexType.NOT_ANALYZED, false);
        }
        public Builder add(String column, DataType type, List<String> path, ReferenceInfo.IndexType indexType) {
            return add(column, type, path, ColumnPolicy.DYNAMIC, indexType, false);
        }
        public Builder add(String column, DataType type, List<String> path,
                           boolean partitionBy) {
            return add(column, type, path, ColumnPolicy.DYNAMIC,
                    ReferenceInfo.IndexType.NOT_ANALYZED, partitionBy);
        }

        public Builder add(String column, DataType type, List<String> path,
                           ColumnPolicy columnPolicy, ReferenceInfo.IndexType indexType,
                           boolean partitionBy) {
            RowGranularity rowGranularity = granularity;
            if (partitionBy) {
                rowGranularity = RowGranularity.PARTITION;
            }
            ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(ident, column, path),
                    rowGranularity, type, columnPolicy, indexType);
            if (info.ident().isColumn()) {
                columns.add(info);
            }
            references.put(info.ident().columnIdent(), info);
            if (partitionBy) {
                partitionedByColumns.add(info);
                partitionedBy.add(info.ident().columnIdent());
            }
            return this;
        }

        public Builder addIndex(ColumnIdent columnIdent, ReferenceInfo.IndexType indexType) {
            IndexReferenceInfo.Builder builder = new IndexReferenceInfo.Builder()
                    .ident(new ReferenceIdent(ident, columnIdent))
                    .indexType(indexType);
            indexColumns.put(columnIdent, builder.build());
            return this;
        }

        public Builder addPrimaryKey(String column) {
            primaryKey.add(ColumnIdent.fromPath(column));
            return this;
        }

        public Builder clusteredBy(String clusteredBy) {
            this.clusteredBy = ColumnIdent.fromPath(clusteredBy);
            return this;
        }

        public Builder isAlias(boolean isAlias) {
            this.isAlias = isAlias;
            return this;
        }

        public Builder schemaInfo(SchemaInfo schemaInfo) {
            this.schemaInfo = schemaInfo;
            return this;
        }

        public Builder addPartitions(String... partitionNames) {
            for (String partitionName : partitionNames) {
                PartitionName partition = PartitionName.fromString(partitionName, ident.schema(), ident.name());
                partitions.add(partition);
            }
            return this;
        }

        public TableInfo build() {
            addDocSysColumns();
            return new TestingTableInfo(
                    columns.build(),
                    partitionedByColumns.build(),
                    indexColumns.build(),
                    references.build(),
                    ident,
                    granularity,
                    routing,
                    primaryKey.build(),
                    clusteredBy,
                    isAlias,
                    partitionedBy.build(),
                    partitions.build(),
                    columnPolicy,
                    schemaInfo == null ? mock(SchemaInfo.class, Answers.RETURNS_MOCKS.get()) : schemaInfo);
        }

    }


    private final List<ReferenceInfo> columns;
    private final List<ReferenceInfo> partitionedByColumns;
    private final Map<ColumnIdent, IndexReferenceInfo> indexColumns;
    private final Map<ColumnIdent, ReferenceInfo> references;
    private final TableIdent ident;
    private final RowGranularity granularity;
    private final List<ColumnIdent> primaryKey;
    private final boolean isAlias;
    private boolean hasAutoGeneratedPrimaryKey = false;
    private final List<ColumnIdent> partitionedBy;
    private final List<PartitionName> partitions;
    private final ColumnPolicy columnPolicy;
    private final TableParameterInfo tableParameterInfo;


    public TestingTableInfo(List<ReferenceInfo> columns,
                            List<ReferenceInfo> partitionedByColumns,
                            Map<ColumnIdent, IndexReferenceInfo> indexColumns,
                            Map<ColumnIdent, ReferenceInfo> references,
                            TableIdent ident, RowGranularity granularity,
                            Routing routing,
                            List<ColumnIdent> primaryKey,
                            ColumnIdent clusteredBy,
                            boolean isAlias,
                            List<ColumnIdent> partitionedBy,
                            List<PartitionName> partitions,
                            ColumnPolicy columnPolicy,
                            SchemaInfo schemaInfo
                            ) {
        super(schemaInfo);
        this.columns = columns;
        this.partitionedByColumns = partitionedByColumns;
        this.indexColumns = indexColumns;
        this.references = references;
        this.ident = ident;
        this.granularity = granularity;
        this.routing = routing;
        if (primaryKey == null || primaryKey.isEmpty()){
            if ((clusteredBy == null || clusteredBy.equals("_id")) && partitionedBy.isEmpty()){
                this.primaryKey = ImmutableList.of(DocIndexMetaData.ID_IDENT);
                this.hasAutoGeneratedPrimaryKey = true;
            } else {
                this.primaryKey = ImmutableList.of();
            }
        } else {
            this.primaryKey = primaryKey;
        }
        this.clusteredBy = clusteredBy;
        this.isAlias = isAlias;
        this.columnPolicy = columnPolicy;
        this.partitionedBy = partitionedBy;
        this.partitions = partitions;
        if (partitionedByColumns.isEmpty()) {
            tableParameterInfo = new TableParameterInfo();
        } else {
            tableParameterInfo = new AlterPartitionedTableParameterInfo();
        }
    }

    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
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
    public IndexReferenceInfo indexColumn(ColumnIdent ident) {
        return indexColumns.get(ident);
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
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return routing;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return primaryKey;
    }

    @Override
    public boolean hasAutoGeneratedPrimaryKey() {
        return hasAutoGeneratedPrimaryKey;
    }

    @Override
    public ColumnIdent clusteredBy() {
        return clusteredBy;
    }

    @Override
    public boolean isAlias() {
        return isAlias;
    }

    @Override
    public String[] concreteIndices() {
        return new String[]{ident.esName()};
    }

    @Override
    public DynamicReference getDynamic(ColumnIdent ident) {
        if (!ident.isColumn()) {
            ColumnIdent parentIdent = ident.getParent();
            ReferenceInfo parentInfo = getReferenceInfo(parentIdent);
            if (parentInfo != null && parentInfo.columnPolicy() == ColumnPolicy.STRICT) {
                throw new ColumnUnknownException(ident.sqlFqn());
            }
        }
        return new DynamicReference(new ReferenceIdent(ident(), ident), rowGranularity());
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return references.values().iterator();
    }

    @Override
    public List<ColumnIdent> partitionedBy() {
        return partitionedBy;
    }

    @Override
    public List<PartitionName> partitions() {
        return partitions;
    }

    @Override
    public ColumnPolicy columnPolicy() {
        return columnPolicy;
    }

    @Override
    public TableParameterInfo tableParameterInfo () {
        return tableParameterInfo;
    }

    @Override
    public SchemaInfo schemaInfo() {
        final SchemaInfo schemaInfo = super.schemaInfo();
        when(schemaInfo.name()).thenReturn(ident.schema());
        return schemaInfo;
    }
}
