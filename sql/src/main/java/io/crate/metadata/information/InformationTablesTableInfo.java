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

package io.crate.metadata.information;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.information.TablesSettingsExpression;
import io.crate.expression.reference.information.TablesVersionExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.ShardedTable;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.Version;

import java.util.List;
import java.util.Map;

public class InformationTablesTableInfo extends InformationTableInfo {

    public static final String NAME = "tables";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static final String SELF_REFERENCING_COLUMN_NAME = "_id";
    private static final String REFERENCE_GENERATION = "SYSTEM GENERATED";

    public static class Columns {
        static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        static final ColumnIdent TABLE_CATALOG = new ColumnIdent("table_catalog");
        static final ColumnIdent TABLE_TYPE = new ColumnIdent("table_type");
        static final ColumnIdent NUMBER_OF_SHARDS = new ColumnIdent("number_of_shards");
        static final ColumnIdent NUMBER_OF_REPLICAS = new ColumnIdent("number_of_replicas");
        static final ColumnIdent CLUSTERED_BY = new ColumnIdent("clustered_by");
        static final ColumnIdent PARTITIONED_BY = new ColumnIdent("partitioned_by");
        static final ColumnIdent BLOBS_PATH = new ColumnIdent("blobs_path");
        static final ColumnIdent COLUMN_POLICY = new ColumnIdent("column_policy");
        static final ColumnIdent ROUTING_HASH_FUNCTION = new ColumnIdent("routing_hash_function");
        static final ColumnIdent TABLE_VERSION = new ColumnIdent("version");
        static final ColumnIdent CLOSED = new ColumnIdent("closed");
        static final ColumnIdent REFERENCE_GENERATION = new ColumnIdent("reference_generation");
        static final ColumnIdent SELF_REFERENCING_COLUMN_NAME = new ColumnIdent("self_referencing_column_name");
        static final ColumnIdent TABLE_SETTINGS = new ColumnIdent("settings");
    }

    private static ColumnRegistrar createColumnRegistrar() {
        return new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.TABLE_SCHEMA, DataTypes.STRING)
            .register(Columns.TABLE_NAME, DataTypes.STRING)
            .register(Columns.TABLE_CATALOG, DataTypes.STRING)
            .register(Columns.TABLE_TYPE, DataTypes.STRING)
            .register(Columns.NUMBER_OF_SHARDS, DataTypes.INTEGER)
            .register(Columns.NUMBER_OF_REPLICAS, DataTypes.STRING)
            .register(Columns.CLUSTERED_BY, DataTypes.STRING)
            .register(Columns.PARTITIONED_BY, DataTypes.STRING_ARRAY)
            .register(Columns.BLOBS_PATH, DataTypes.STRING)
            .register(Columns.COLUMN_POLICY, DataTypes.STRING)
            .register(Columns.ROUTING_HASH_FUNCTION, DataTypes.STRING)
            .register(Columns.TABLE_VERSION, ObjectType.builder()
                .setInnerType(Version.Property.CREATED.toString(), DataTypes.STRING)
                .setInnerType(Version.Property.UPGRADED.toString(), DataTypes.STRING)
                .build())
            .register(Columns.CLOSED, DataTypes.BOOLEAN)
            .register(Columns.REFERENCE_GENERATION, DataTypes.STRING)
            .register(Columns.SELF_REFERENCING_COLUMN_NAME, DataTypes.STRING)
            .register(Columns.TABLE_SETTINGS, ObjectType.builder()
                .setInnerType("refresh_interval", DataTypes.LONG)
                .setInnerType("blocks", ObjectType.builder()
                    .setInnerType("read_only", DataTypes.BOOLEAN)
                    .setInnerType("read", DataTypes.BOOLEAN)
                    .setInnerType("write", DataTypes.BOOLEAN)
                    .setInnerType("metadata", DataTypes.BOOLEAN)
                    .build())
                .setInnerType("translog", ObjectType.builder()
                    .setInnerType("flush_threshold_size", DataTypes.LONG)
                    .setInnerType("sync_interval", DataTypes.LONG)
                    .build())
                .setInnerType("routing", ObjectType.builder()
                    .setInnerType("allocation", ObjectType.builder()
                        .setInnerType("enable", DataTypes.STRING)
                        .setInnerType("total_shards_per_node", DataTypes.INTEGER)
                        .setInnerType("require", ObjectType.untyped())
                        .setInnerType("include", ObjectType.untyped())
                        .setInnerType("exclude", ObjectType.untyped())
                        .build())
                    .build())
                .setInnerType("warmer", ObjectType.builder()
                    .setInnerType("enabled", DataTypes.BOOLEAN)
                    .build())
                .setInnerType("write", ObjectType.builder()
                    .setInnerType("wait_for_active_shards", DataTypes.STRING)
                    .build())
                .setInnerType("unassigned", ObjectType.builder()
                    .setInnerType("node_left", ObjectType.builder()
                        .setInnerType("delayed_timeout", DataTypes.LONG)
                        .build())
                    .build())
                .setInnerType("mapping", ObjectType.builder()
                    .setInnerType("total_fields", ObjectType.builder()
                        .setInnerType("limit", DataTypes.INTEGER)
                        .build())
                    .build())
                .build());
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<RelationInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<RelationInfo>>builder()
            .put(InformationTablesTableInfo.Columns.TABLE_SCHEMA,
                () -> NestableCollectExpression.forFunction(r -> r.ident().schema()))
            .put(InformationTablesTableInfo.Columns.TABLE_NAME,
                () -> NestableCollectExpression.forFunction(r -> r.ident().name()))
            .put(InformationTablesTableInfo.Columns.TABLE_CATALOG,
                () -> NestableCollectExpression.forFunction(r -> r.ident().schema()))
            .put(InformationTablesTableInfo.Columns.TABLE_TYPE,
                () -> NestableCollectExpression.forFunction(r -> r.relationType().pretty()))
            .put(InformationTablesTableInfo.Columns.NUMBER_OF_SHARDS,
                () -> NestableCollectExpression.forFunction(row -> {
                    if (row instanceof ShardedTable) {
                        return ((ShardedTable) row).numberOfShards();
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS,
                () -> NestableCollectExpression.forFunction(row -> {
                    if (row instanceof ShardedTable) {
                        return ((ShardedTable) row).numberOfReplicas();
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.CLUSTERED_BY,
                () -> NestableCollectExpression.forFunction(row -> {
                    if (row instanceof ShardedTable) {
                        ColumnIdent clusteredBy = ((ShardedTable) row).clusteredBy();
                        if (clusteredBy == null) {
                            return null;
                        }
                        return clusteredBy.fqn();
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.PARTITIONED_BY,
                () -> NestableCollectExpression.forFunction(row -> {
                    if (row instanceof DocTableInfo) {
                        List<ColumnIdent> partitionedBy = ((DocTableInfo) row).partitionedBy();
                        if (partitionedBy == null || partitionedBy.isEmpty()) {
                            return null;
                        }

                        String[] partitions = new String[partitionedBy.size()];
                        for (int i = 0; i < partitions.length; i++) {
                            partitions[i] = partitionedBy.get(i).fqn();
                        }
                        return partitions;
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.COLUMN_POLICY,
                () -> NestableCollectExpression.forFunction(row -> {
                    if (row instanceof DocTableInfo) {
                        return ((DocTableInfo) row).columnPolicy().lowerCaseName();
                    }
                    return ColumnPolicy.STRICT.lowerCaseName();
                }))
            .put(InformationTablesTableInfo.Columns.BLOBS_PATH,
                () -> NestableCollectExpression.forFunction(row -> {
                    if (row instanceof BlobTableInfo) {
                        return ((BlobTableInfo) row).blobsPath();
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.ROUTING_HASH_FUNCTION,
                () -> NestableCollectExpression.forFunction(row -> {
                    if (row instanceof ShardedTable) {
                        return IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME;
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.CLOSED,
                () -> NestableCollectExpression.forFunction(row -> {
                    if (row instanceof ShardedTable) {
                        return ((ShardedTable) row).isClosed();
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.SELF_REFERENCING_COLUMN_NAME,
                () -> NestableCollectExpression.forFunction(row -> {
                    if (row instanceof ShardedTable) {
                        return SELF_REFERENCING_COLUMN_NAME;
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.REFERENCE_GENERATION,
                () -> NestableCollectExpression.forFunction(r -> REFERENCE_GENERATION))
            .put(InformationTablesTableInfo.Columns.TABLE_VERSION, TablesVersionExpression::new)
            .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS, TablesSettingsExpression::new
            ).build();
    }

    InformationTablesTableInfo() {
        super(
            IDENT,
            createColumnRegistrar(),
            ImmutableList.of(Columns.TABLE_CATALOG, Columns.TABLE_SCHEMA, Columns.TABLE_NAME)
        );
    }
}
