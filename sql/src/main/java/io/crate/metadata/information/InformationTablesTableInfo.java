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
import com.google.common.collect.ImmutableSortedMap;
import io.crate.Version;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.ShardedTable;
import io.crate.metadata.table.TableInfo;
import io.crate.expression.reference.information.TablesSettingsExpression;
import io.crate.expression.reference.information.TablesVersionExpression;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.Map;

public class InformationTablesTableInfo extends InformationTableInfo {

    public static final String NAME = "tables";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    static final BytesRef TABLE_TYPE = new BytesRef("BASE TABLE");
    static final BytesRef SELF_REFERENCING_COLUMN_NAME = new BytesRef("_id");
    static final BytesRef REFERENCE_GENERATION = new BytesRef("SYSTEM GENERATED");


    public static class Columns {
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        static final ColumnIdent TABLE_CATALOG = new ColumnIdent("table_catalog");
        static final ColumnIdent TABLE_TYPE = new ColumnIdent("table_type");
        static final ColumnIdent PARTITION_IDENT = new ColumnIdent("partition_ident");
        public static final ColumnIdent VALUES = new ColumnIdent("values");
        static final ColumnIdent NUMBER_OF_SHARDS = new ColumnIdent("number_of_shards");
        public static final ColumnIdent NUMBER_OF_REPLICAS = new ColumnIdent("number_of_replicas");
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


    public static class References {
        static final Reference TABLE_SCHEMA = createRef(Columns.TABLE_SCHEMA, DataTypes.STRING);
        public static final Reference TABLE_NAME = createRef(Columns.TABLE_NAME, DataTypes.STRING);
        static final Reference TABLE_CATALOG = createRef(Columns.TABLE_CATALOG, DataTypes.STRING);
        static final Reference TABLE_TYPE = createRef(Columns.TABLE_TYPE, DataTypes.STRING);
        static final Reference NUMBER_OF_SHARDS = createRef(Columns.NUMBER_OF_SHARDS, DataTypes.INTEGER);
        public static final Reference NUMBER_OF_REPLICAS = createRef(Columns.NUMBER_OF_REPLICAS, DataTypes.STRING);
        static final Reference CLUSTERED_BY = createRef(Columns.CLUSTERED_BY, DataTypes.STRING);
        static final Reference PARTITIONED_BY = createRef(Columns.PARTITIONED_BY, new ArrayType(DataTypes.STRING));
        static final Reference BLOBS_PATH = createRef(Columns.BLOBS_PATH, DataTypes.STRING);
        static final Reference COLUMN_POLICY = createRef(Columns.COLUMN_POLICY, DataTypes.STRING);
        static final Reference ROUTING_HASH_FUNCTION = createRef(Columns.ROUTING_HASH_FUNCTION, DataTypes.STRING);
        static final Reference TABLE_VERSION = createRef(Columns.TABLE_VERSION, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_CREATED = createRef(
            Columns.TABLE_VERSION_CREATED, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_CREATED_CRATEDB = createRef(
            Columns.TABLE_VERSION_CREATED_CRATEDB, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_CREATED_ES = createRef(
            Columns.TABLE_VERSION_CREATED_ES, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_UPGRADED = createRef(
            Columns.TABLE_VERSION_UPGRADED, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_UPGRADED_CRATEDB = createRef(
            Columns.TABLE_VERSION_UPGRADED_CRATEDB, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_UPGRADED_ES = createRef(
            Columns.TABLE_VERSION_UPGRADED_ES, DataTypes.OBJECT);
        static final Reference CLOSED = createRef(Columns.CLOSED, DataTypes.BOOLEAN);
        static final Reference REFERENCE_GENERATION = createRef(Columns.REFERENCE_GENERATION, DataTypes.STRING);
        static final Reference SELF_REFERENCING_COLUMN_NAME = createRef(Columns.SELF_REFERENCING_COLUMN_NAME,
            DataTypes.STRING);

        static final Reference TABLE_SETTINGS = createRef(Columns.TABLE_SETTINGS, DataTypes.OBJECT);

        static final Reference TABLE_SETTINGS_REFRESH_INTERVAL = createRef(
            Columns.TABLE_SETTINGS_REFRESH_INTERVAL, DataTypes.LONG);

        static final Reference TABLE_SETTINGS_WRITE = createRef(
            Columns.TABLE_SETTINGS_WRITE, DataTypes.OBJECT);

        static final Reference TABLE_SETTINGS_WRITE_WAIT_FOT_ACTIVE_SHARDS = createRef(
            Columns.TABLE_SETTINGS_WRITE_WAIT_FOT_ACTIVE_SHARDS, DataTypes.STRING);

        static final Reference TABLE_SETTINGS_BLOCKS = createRef(
            Columns.TABLE_SETTINGS_BLOCKS, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_BLOCKS_READ_ONLY = createRef(
            Columns.TABLE_SETTINGS_BLOCKS_READ_ONLY, DataTypes.BOOLEAN);
        static final Reference TABLE_SETTINGS_BLOCKS_READ = createRef(
            Columns.TABLE_SETTINGS_BLOCKS_READ, DataTypes.BOOLEAN);
        static final Reference TABLE_SETTINGS_BLOCKS_WRITE = createRef(
            Columns.TABLE_SETTINGS_BLOCKS_WRITE, DataTypes.BOOLEAN);
        static final Reference TABLE_SETTINGS_BLOCKS_METADATA = createRef(
            Columns.TABLE_SETTINGS_BLOCKS_METADATA, DataTypes.BOOLEAN);

        static final Reference TABLE_SETTINGS_TRANSLOG = createRef(
            Columns.TABLE_SETTINGS_TRANSLOG, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE = createRef(
            Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE, DataTypes.LONG);

        static final Reference TABLE_SETTINGS_ROUTING = createRef(
            Columns.TABLE_SETTINGS_ROUTING, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_ROUTING_ALLOCATION = createRef(
            Columns.TABLE_SETTINGS_ROUTING_ALLOCATION, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE = createRef(
            Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE, DataTypes.STRING);
        static final Reference TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE = createRef(
            Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE, DataTypes.INTEGER);

        static final Reference TABLE_SETTINGS_MAPPING = createRef(
            Columns.TABLE_SETTINGS_MAPPING, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_MAPPING_TOTAL_FIELDS = createRef(
            Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT = createRef(
            Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT, DataTypes.INTEGER);

        static final Reference TABLE_SETTINGS_WARMER = createRef(
            Columns.TABLE_SETTINGS_WARMER, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_WARMER_ENABLED = createRef(
            Columns.TABLE_SETTINGS_WARMER_ENABLED, DataTypes.BOOLEAN);

        static final Reference TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL = createRef(
            Columns.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL, DataTypes.LONG);

        static final Reference TABLE_SETTINGS_UNASSIGNED = createRef(
            Columns.TABLE_SETTINGS_UNASSIGNED, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_UNASSIGNED_NODE_LEFT = createRef(
            Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT = createRef(
            Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, DataTypes.LONG);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<TableInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<TableInfo>>builder()
            .put(InformationTablesTableInfo.Columns.TABLE_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.ident().schema()))
            .put(InformationTablesTableInfo.Columns.TABLE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.ident().name()))
            .put(InformationTablesTableInfo.Columns.TABLE_CATALOG,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.ident().schema()))
            .put(InformationTablesTableInfo.Columns.TABLE_TYPE,
                () -> RowContextCollectorExpression.objToBytesRef(r -> TABLE_TYPE))
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
                        return new BytesRef(clusteredBy.fqn());
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
                        return new BytesRef(((DocTableInfo) row).columnPolicy().value());
                    }
                    return new BytesRef(ColumnPolicy.STRICT.value());
                }))
            .put(InformationTablesTableInfo.Columns.BLOBS_PATH,
                () -> RowContextCollectorExpression.objToBytesRef(row -> {
                    if (row instanceof BlobTableInfo) {
                        return ((BlobTableInfo) row).blobsPath();
                    }
                    return null;
                }))
            .put(InformationTablesTableInfo.Columns.ROUTING_HASH_FUNCTION,
                () -> RowContextCollectorExpression.objToBytesRef(row -> {
                    if (row instanceof ShardedTable) {
                        return new BytesRef(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME);
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

    private static Reference createRef(ColumnIdent columnIdent, DataType dataType) {
        return new Reference(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    InformationTablesTableInfo() {
        super(
            IDENT,
            ImmutableList.of(Columns.TABLE_SCHEMA, Columns.TABLE_NAME),
            ImmutableSortedMap.<ColumnIdent, Reference>naturalOrder()
                .put(Columns.TABLE_SCHEMA, References.TABLE_SCHEMA)
                .put(Columns.TABLE_NAME, References.TABLE_NAME)
                .put(Columns.TABLE_CATALOG, References.TABLE_CATALOG)
                .put(Columns.TABLE_TYPE, References.TABLE_TYPE)
                .put(Columns.NUMBER_OF_SHARDS, References.NUMBER_OF_SHARDS)
                .put(Columns.NUMBER_OF_REPLICAS, References.NUMBER_OF_REPLICAS)
                .put(Columns.CLUSTERED_BY, References.CLUSTERED_BY)
                .put(Columns.PARTITIONED_BY, References.PARTITIONED_BY)
                .put(Columns.BLOBS_PATH, References.BLOBS_PATH)
                .put(Columns.COLUMN_POLICY, References.COLUMN_POLICY)
                .put(Columns.ROUTING_HASH_FUNCTION, References.ROUTING_HASH_FUNCTION)
                .put(Columns.TABLE_VERSION, References.TABLE_VERSION)
                .put(Columns.TABLE_VERSION_CREATED, References.TABLE_VERSION_CREATED)
                .put(Columns.TABLE_VERSION_CREATED_CRATEDB, References.TABLE_VERSION_CREATED_CRATEDB)
                .put(Columns.TABLE_VERSION_CREATED_ES, References.TABLE_VERSION_CREATED_ES)
                .put(Columns.TABLE_VERSION_UPGRADED, References.TABLE_VERSION_UPGRADED)
                .put(Columns.TABLE_VERSION_UPGRADED_CRATEDB, References.TABLE_VERSION_UPGRADED_CRATEDB)
                .put(Columns.TABLE_VERSION_UPGRADED_ES, References.TABLE_VERSION_UPGRADED_ES)
                .put(Columns.CLOSED, References.CLOSED)
                .put(Columns.REFERENCE_GENERATION, References.REFERENCE_GENERATION)
                .put(Columns.SELF_REFERENCING_COLUMN_NAME, References.SELF_REFERENCING_COLUMN_NAME)
                .put(Columns.TABLE_SETTINGS, References.TABLE_SETTINGS)
                .put(Columns.TABLE_SETTINGS_BLOCKS, References.TABLE_SETTINGS_BLOCKS)
                .put(Columns.TABLE_SETTINGS_BLOCKS_READ_ONLY, References.TABLE_SETTINGS_BLOCKS_READ_ONLY)
                .put(Columns.TABLE_SETTINGS_BLOCKS_READ, References.TABLE_SETTINGS_BLOCKS_READ)
                .put(Columns.TABLE_SETTINGS_BLOCKS_WRITE, References.TABLE_SETTINGS_BLOCKS_WRITE)
                .put(Columns.TABLE_SETTINGS_BLOCKS_METADATA, References.TABLE_SETTINGS_BLOCKS_METADATA)
                .put(Columns.TABLE_SETTINGS_TRANSLOG, References.TABLE_SETTINGS_TRANSLOG)
                .put(Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE, References.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE)
                .put(Columns.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL, References.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL)
                .put(Columns.TABLE_SETTINGS_REFRESH_INTERVAL, References.TABLE_SETTINGS_REFRESH_INTERVAL)
                .put(Columns.TABLE_SETTINGS_ROUTING, References.TABLE_SETTINGS_ROUTING)
                .put(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION, References.TABLE_SETTINGS_ROUTING_ALLOCATION)
                .put(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE, References.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE)
                .put(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE, References.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE)
                .put(Columns.TABLE_SETTINGS_WARMER, References.TABLE_SETTINGS_WARMER)
                .put(Columns.TABLE_SETTINGS_WARMER_ENABLED, References.TABLE_SETTINGS_WARMER_ENABLED)
                .put(Columns.TABLE_SETTINGS_WRITE, References.TABLE_SETTINGS_WRITE)
                .put(Columns.TABLE_SETTINGS_WRITE_WAIT_FOT_ACTIVE_SHARDS, References.TABLE_SETTINGS_WRITE_WAIT_FOT_ACTIVE_SHARDS)
                .put(Columns.TABLE_SETTINGS_UNASSIGNED, References.TABLE_SETTINGS_UNASSIGNED)
                .put(Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT, References.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT)
                .put(Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, References.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT)
                .put(Columns.TABLE_SETTINGS_MAPPING, References.TABLE_SETTINGS_MAPPING)
                .put(Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS, References.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS)
                .put(Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT, References.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT)
                .build(),
            ImmutableList.of(
                References.BLOBS_PATH,
                References.CLOSED,
                References.CLUSTERED_BY,
                References.COLUMN_POLICY,
                References.NUMBER_OF_REPLICAS,
                References.NUMBER_OF_SHARDS,
                References.PARTITIONED_BY,
                References.REFERENCE_GENERATION,
                References.ROUTING_HASH_FUNCTION,
                References.SELF_REFERENCING_COLUMN_NAME,
                References.TABLE_SETTINGS,
                References.TABLE_CATALOG,
                References.TABLE_NAME,
                References.TABLE_SCHEMA,
                References.TABLE_TYPE,
                References.TABLE_VERSION
            )
        );
    }
}
