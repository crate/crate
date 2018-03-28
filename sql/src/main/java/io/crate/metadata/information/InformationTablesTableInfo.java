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
import io.crate.Version;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.RelationName;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.ShardedTable;
import io.crate.expression.reference.information.TablesSettingsExpression;
import io.crate.expression.reference.information.TablesVersionExpression;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.Map;

public class InformationTablesTableInfo extends InformationTableInfo {

    public static final String NAME = "tables";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static final BytesRef SELF_REFERENCING_COLUMN_NAME = new BytesRef("_id");
    private static final BytesRef REFERENCE_GENERATION = new BytesRef("SYSTEM GENERATED");

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
        static final ColumnIdent TABLE_VERSION_CREATED = new ColumnIdent("version",
            ImmutableList.of(Version.Property.CREATED.toString()));
        static final ColumnIdent TABLE_VERSION_CREATED_CRATEDB = new ColumnIdent("version",
            ImmutableList.of(Version.Property.CREATED.toString(), Version.CRATEDB_VERSION_KEY));
        static final ColumnIdent TABLE_VERSION_CREATED_ES = new ColumnIdent("version",
            ImmutableList.of(Version.Property.CREATED.toString(), Version.ES_VERSION_KEY));
        static final ColumnIdent TABLE_VERSION_UPGRADED = new ColumnIdent("version",
            ImmutableList.of(Version.Property.UPGRADED.toString()));
        static final ColumnIdent TABLE_VERSION_UPGRADED_CRATEDB = new ColumnIdent("version",
            ImmutableList.of(Version.Property.UPGRADED.toString(), Version.CRATEDB_VERSION_KEY));
        static final ColumnIdent TABLE_VERSION_UPGRADED_ES = new ColumnIdent("version",
            ImmutableList.of(Version.Property.UPGRADED.toString(), Version.ES_VERSION_KEY));
        static final ColumnIdent CLOSED = new ColumnIdent("closed");
        static final ColumnIdent REFERENCE_GENERATION = new ColumnIdent("reference_generation");
        static final ColumnIdent SELF_REFERENCING_COLUMN_NAME = new ColumnIdent("self_referencing_column_name");
        static final ColumnIdent TABLE_SETTINGS = new ColumnIdent("settings");
        static final ColumnIdent TABLE_SETTINGS_BLOCKS = new ColumnIdent("settings",
            ImmutableList.of("blocks"));
        static final ColumnIdent TABLE_SETTINGS_BLOCKS_READ_ONLY = new ColumnIdent("settings",
            ImmutableList.of("blocks", "read_only"));
        static final ColumnIdent TABLE_SETTINGS_BLOCKS_READ = new ColumnIdent("settings",
            ImmutableList.of("blocks", "read"));
        static final ColumnIdent TABLE_SETTINGS_BLOCKS_WRITE = new ColumnIdent("settings",
            ImmutableList.of("blocks", "write"));
        static final ColumnIdent TABLE_SETTINGS_BLOCKS_METADATA = new ColumnIdent("settings",
            ImmutableList.of("blocks", "metadata"));
        static final ColumnIdent TABLE_SETTINGS_ROUTING = new ColumnIdent("settings",
            ImmutableList.of("routing"));
        static final ColumnIdent TABLE_SETTINGS_ROUTING_ALLOCATION = new ColumnIdent("settings",
            ImmutableList.of("routing", "allocation"));
        static final ColumnIdent TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE = new ColumnIdent("settings",
            ImmutableList.of("routing", "allocation", "enable"));
        static final ColumnIdent TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE = new ColumnIdent("settings",
            ImmutableList.of("routing", "allocation", "total_shards_per_node"));
        static final ColumnIdent TABLE_SETTINGS_WARMER = new ColumnIdent("settings",
            ImmutableList.of("warmer"));
        static final ColumnIdent TABLE_SETTINGS_WARMER_ENABLED = new ColumnIdent("settings",
            ImmutableList.of("warmer", "enabled"));
        static final ColumnIdent TABLE_SETTINGS_MAPPING = new ColumnIdent("settings", ImmutableList.of("mapping"));
        static final ColumnIdent TABLE_SETTINGS_MAPPING_TOTAL_FIELDS = new ColumnIdent("settings",
            ImmutableList.of("mapping", "total_fields"));
        static final ColumnIdent TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT = new ColumnIdent("settings",
            ImmutableList.of("mapping", "total_fields", "limit"));

        static final ColumnIdent TABLE_SETTINGS_TRANSLOG = new ColumnIdent("settings",
            ImmutableList.of("translog"));
        static final ColumnIdent TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE = new ColumnIdent("settings",
            ImmutableList.of("translog", "flush_threshold_size"));
        static final ColumnIdent TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL = new ColumnIdent("settings",
            ImmutableList.of("translog", "sync_interval"));

        static final ColumnIdent TABLE_SETTINGS_REFRESH_INTERVAL = new ColumnIdent("settings",
            ImmutableList.of("refresh_interval"));

        static final ColumnIdent TABLE_SETTINGS_WRITE = new ColumnIdent("settings",
            ImmutableList.of("write"));

        static final ColumnIdent TABLE_SETTINGS_WRITE_WAIT_FOT_ACTIVE_SHARDS = new ColumnIdent("settings",
            ImmutableList.of("write", "wait_for_active_shards"));

        static final ColumnIdent TABLE_SETTINGS_UNASSIGNED = new ColumnIdent("settings",
            ImmutableList.of("unassigned"));
        static final ColumnIdent TABLE_SETTINGS_UNASSIGNED_NODE_LEFT = new ColumnIdent("settings",
            ImmutableList.of("unassigned", "node_left"));
        static final ColumnIdent TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT = new ColumnIdent("settings",
            ImmutableList.of("unassigned", "node_left", "delayed_timeout"));
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
            .register(Columns.TABLE_VERSION, DataTypes.OBJECT)
            .register(Columns.TABLE_VERSION_CREATED, DataTypes.OBJECT)
            .register(Columns.TABLE_VERSION_CREATED_CRATEDB, DataTypes.STRING)
            .register(Columns.TABLE_VERSION_CREATED_ES, DataTypes.STRING)
            .register(Columns.TABLE_VERSION_UPGRADED, DataTypes.OBJECT)
            .register(Columns.TABLE_VERSION_UPGRADED_CRATEDB, DataTypes.STRING)
            .register(Columns.TABLE_VERSION_UPGRADED_ES, DataTypes.STRING)
            .register(Columns.CLOSED, DataTypes.BOOLEAN)
            .register(Columns.REFERENCE_GENERATION, DataTypes.STRING)
            .register(Columns.SELF_REFERENCING_COLUMN_NAME, DataTypes.STRING)
            .register(Columns.TABLE_SETTINGS, DataTypes.OBJECT)
            .register(Columns.TABLE_SETTINGS_BLOCKS, DataTypes.OBJECT)
            .register(Columns.TABLE_SETTINGS_BLOCKS_READ_ONLY, DataTypes.BOOLEAN)
            .register(Columns.TABLE_SETTINGS_BLOCKS_READ, DataTypes.BOOLEAN)
            .register(Columns.TABLE_SETTINGS_BLOCKS_WRITE, DataTypes.BOOLEAN)
            .register(Columns.TABLE_SETTINGS_BLOCKS_METADATA, DataTypes.BOOLEAN)
            .register(Columns.TABLE_SETTINGS_TRANSLOG, DataTypes.OBJECT)
            .register(Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE, DataTypes.LONG)
            .register(Columns.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL, DataTypes.LONG)
            .register(Columns.TABLE_SETTINGS_REFRESH_INTERVAL, DataTypes.LONG)
            .register(Columns.TABLE_SETTINGS_ROUTING, DataTypes.OBJECT)
            .register(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION, DataTypes.OBJECT)
            .register(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE, DataTypes.STRING)
            .register(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE, DataTypes.INTEGER)
            .register(Columns.TABLE_SETTINGS_WARMER, DataTypes.OBJECT)
            .register(Columns.TABLE_SETTINGS_WARMER_ENABLED, DataTypes.BOOLEAN)
            .register(Columns.TABLE_SETTINGS_WRITE, DataTypes.OBJECT)
            .register(Columns.TABLE_SETTINGS_WRITE_WAIT_FOT_ACTIVE_SHARDS, DataTypes.STRING)
            .register(Columns.TABLE_SETTINGS_UNASSIGNED, DataTypes.OBJECT)
            .register(Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT, DataTypes.OBJECT)
            .register(Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, DataTypes.LONG)
            .register(Columns.TABLE_SETTINGS_MAPPING, DataTypes.OBJECT)
            .register(Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS, DataTypes.OBJECT)
            .register(Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT, DataTypes.INTEGER);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<RelationInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<RelationInfo>>builder()
            .put(InformationTablesTableInfo.Columns.TABLE_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.ident().schema()))
            .put(InformationTablesTableInfo.Columns.TABLE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.ident().name()))
            .put(InformationTablesTableInfo.Columns.TABLE_CATALOG,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.ident().schema()))
            .put(InformationTablesTableInfo.Columns.TABLE_TYPE,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.relationType().pretty()))
            .put(InformationTablesTableInfo.Columns.NUMBER_OF_SHARDS,
                () -> RowContextCollectorExpression.forFunction(row -> {
                    if (row instanceof ShardedTable) {
                        return ((ShardedTable) row).numberOfShards();
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS,
                () -> RowContextCollectorExpression.objToBytesRef(row -> {
                    if (row instanceof ShardedTable) {
                        return ((ShardedTable) row).numberOfReplicas();
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.CLUSTERED_BY,
                () -> RowContextCollectorExpression.objToBytesRef(row -> {
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
                () -> RowContextCollectorExpression.forFunction(row -> {
                    if (row instanceof DocTableInfo) {
                        List<ColumnIdent> partitionedBy = ((DocTableInfo) row).partitionedBy();
                        if (partitionedBy == null || partitionedBy.isEmpty()) {
                            return null;
                        }

                        BytesRef[] partitions = new BytesRef[partitionedBy.size()];
                        for (int i = 0; i < partitions.length; i++) {
                            partitions[i] = new BytesRef(partitionedBy.get(i).fqn());
                        }
                        return partitions;
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.COLUMN_POLICY,
                () -> RowContextCollectorExpression.objToBytesRef(row -> {
                    if (row instanceof DocTableInfo) {
                        return ((DocTableInfo) row).columnPolicy().value();
                    }
                    return ColumnPolicy.STRICT.value();
                }))
            .put(InformationTablesTableInfo.Columns.BLOBS_PATH,
                () -> RowContextCollectorExpression.forFunction(row -> {
                    if (row instanceof BlobTableInfo) {
                        return ((BlobTableInfo) row).blobsPath();
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.ROUTING_HASH_FUNCTION,
                () -> RowContextCollectorExpression.objToBytesRef(row -> {
                    if (row instanceof ShardedTable) {
                        return IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME;
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.CLOSED,
                () -> RowContextCollectorExpression.forFunction(row -> {
                    if (row instanceof ShardedTable) {
                        return ((ShardedTable) row).isClosed();
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.SELF_REFERENCING_COLUMN_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(row -> {
                    if (row instanceof ShardedTable) {
                        return SELF_REFERENCING_COLUMN_NAME;
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.REFERENCE_GENERATION,
                () -> RowContextCollectorExpression.objToBytesRef(r -> REFERENCE_GENERATION))
            .put(InformationTablesTableInfo.Columns.TABLE_VERSION, TablesVersionExpression::new)
            .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS, TablesSettingsExpression::new
            ).build();
    }

    InformationTablesTableInfo() {
        super(
            IDENT,
            createColumnRegistrar(),
            ImmutableList.of(Columns.TABLE_SCHEMA, Columns.TABLE_NAME)
        );
    }
}
