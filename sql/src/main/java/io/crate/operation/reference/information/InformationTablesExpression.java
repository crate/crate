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

package io.crate.operation.reference.information;

import io.crate.metadata.*;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.information.InformationTablesTableInfo;
import io.crate.metadata.table.TableInfo;
import org.apache.lucene.util.BytesRef;

import java.util.List;

public abstract class InformationTablesExpression<T> extends RowContextCollectorExpression<TableInfo, T> {

    public static final TablesSchemaNameExpression SCHEMA_NAME_EXPRESSION = new TablesSchemaNameExpression();
    public static final TablesTableNameExpression TABLE_NAME_EXPRESSION = new TablesTableNameExpression();
    public static final TablesNumberOfShardsExpression NUMBER_OF_SHARDS_EXPRESSION = new TablesNumberOfShardsExpression();
    public static final TablesNumberOfReplicasExpression NUMBER_OF_REPLICAS_EXPRESSION = new TablesNumberOfReplicasExpression();
    public static final TablesClusteredByExpression CLUSTERED_BY_EXPRESSION = new TablesClusteredByExpression();
    public static final TablesPartitionByExpression PARTITION_BY_EXPRESSION = new TablesPartitionByExpression();
    public static final TablesBlobPathExpression BLOB_PATH_EXPRESSION = new TablesBlobPathExpression();
    public static final TablesSettingsExpression SETTINGS_EXPRESSION = new TablesSettingsExpression();

    public InformationTablesExpression(ReferenceInfo info) {
        super(info);
    }

    public static class TablesSchemaNameExpression extends InformationTablesExpression<BytesRef> {

        static final BytesRef DOC_SCHEMA_INFO = new BytesRef(ReferenceInfos.DEFAULT_SCHEMA_NAME);

        public TablesSchemaNameExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.SCHEMA_NAME);
        }

        @Override
        public BytesRef value() {
            String schema = row.ident().schema();
            if (schema == null) {
                return DOC_SCHEMA_INFO;
            }
            return new BytesRef(row.ident().schema());
        }
    }

    public static class TablesTableNameExpression extends InformationTablesExpression<BytesRef> {

        public TablesTableNameExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_NAME);
        }

        @Override
        public BytesRef value() {
            assert row.ident().name() != null : "table name should never be null";
            return new BytesRef(row.ident().name());
        }
    }

    public static class TablesNumberOfShardsExpression extends InformationTablesExpression<Integer> {

        protected TablesNumberOfShardsExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.NUMBER_OF_SHARDS);
        }

        @Override
        public Integer value() {
            return row.numberOfShards();
        }
    }

    public static class TablesNumberOfReplicasExpression extends InformationTablesExpression<BytesRef> {

        public TablesNumberOfReplicasExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.NUMBER_OF_REPLICAS);
        }

        @Override
        public BytesRef value() {
            return row.numberOfReplicas();
        }
    }

    public static class TablesClusteredByExpression extends InformationTablesExpression<BytesRef> {
        public TablesClusteredByExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.CLUSTERED_BY);
        }

        @Override
        public BytesRef value() {
            ColumnIdent clusteredBy = row.clusteredBy();
            if (clusteredBy == null) {
                return null;
            }
            return new BytesRef(clusteredBy.fqn());
        }
    }

    public static class TablesPartitionByExpression extends InformationTablesExpression<BytesRef[]> {
        public TablesPartitionByExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.PARTITIONED_BY);
        }

        @Override
        public BytesRef[] value() {
            List<ColumnIdent> partitionedBy = row.partitionedBy();
            if (partitionedBy == null || partitionedBy.isEmpty()) {
                return null;
            }

            BytesRef[] partitions = new BytesRef[partitionedBy.size()];
            for (int i = 0; i < partitions.length; i++) {
                partitions[i] = new BytesRef(partitionedBy.get(i).fqn());
            }
            return partitions;
        }
    }

    public static class TablesBlobPathExpression extends InformationTablesExpression<BytesRef> {

        public TablesBlobPathExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.BLOBS_PATH);
        }

        @Override
        public BytesRef value() {
            if (row instanceof BlobTableInfo) {
                return ((BlobTableInfo) row).blobsPath();
            }
            return null;
        }
    }

}

