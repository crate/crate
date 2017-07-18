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
import com.google.common.collect.ImmutableSortedMap;
import io.crate.metadata.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

public class InformationPartitionsTableInfo extends InformationTableInfo {

    public static final String NAME = "table_partitions";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class PartitionsTableColumns {
        public static final ColumnIdent SCHEMA_NAME = new ColumnIdent("schema_name");
    }

    public static class References {
        public static final Reference TABLE_NAME = createRef(Columns.TABLE_NAME, DataTypes.STRING);
        public static final Reference SCHEMA_NAME = createRef(PartitionsTableColumns.SCHEMA_NAME, DataTypes.STRING);
        public static final Reference PARTITION_IDENT = createRef(Columns.PARTITION_IDENT, DataTypes.STRING);
        public static final Reference VALUES = createRef(Columns.VALUES, DataTypes.OBJECT);
        public static final Reference NUMBER_OF_SHARDS = createRef(Columns.NUMBER_OF_SHARDS, DataTypes.INTEGER);
        public static final Reference NUMBER_OF_REPLICAS = createRef(Columns.NUMBER_OF_REPLICAS, DataTypes.STRING);
        public static final Reference ROUTING_HASH_FUNCTION = createRef(Columns.ROUTING_HASH_FUNCTION, DataTypes.STRING);
        public static final Reference TABLE_VERSION = createRef(Columns.TABLE_VERSION, DataTypes.OBJECT);
        public static final Reference TABLE_VERSION_CREATED = createRef(
            Columns.TABLE_VERSION_CREATED, DataTypes.OBJECT);
        public static final Reference TABLE_VERSION_CREATED_CRATEDB = createRef(
            Columns.TABLE_VERSION_CREATED_CRATEDB, DataTypes.OBJECT);
        public static final Reference TABLE_VERSION_CREATED_ES = createRef(
            Columns.TABLE_VERSION_CREATED_ES, DataTypes.OBJECT);
        public static final Reference TABLE_VERSION_UPGRADED = createRef(
            Columns.TABLE_VERSION_UPGRADED, DataTypes.OBJECT);
        public static final Reference TABLE_VERSION_UPGRADED_CRATEDB = createRef(
            Columns.TABLE_VERSION_UPGRADED_CRATEDB, DataTypes.OBJECT);
        public static final Reference TABLE_VERSION_UPGRADED_ES = createRef(
            Columns.TABLE_VERSION_UPGRADED_ES, DataTypes.OBJECT);
        public static final Reference CLOSED = createRef(Columns.CLOSED, DataTypes.BOOLEAN);

        public static final Reference TABLE_SETTINGS = createRef(Columns.TABLE_SETTINGS, DataTypes.OBJECT);

        public static final Reference TABLE_SETTINGS_BLOCKS = createRef(
            Columns.TABLE_SETTINGS_BLOCKS, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_BLOCKS_READ_ONLY = createRef(
            Columns.TABLE_SETTINGS_BLOCKS_READ_ONLY, DataTypes.BOOLEAN);
        public static final Reference TABLE_SETTINGS_BLOCKS_READ = createRef(
            Columns.TABLE_SETTINGS_BLOCKS_READ, DataTypes.BOOLEAN);
        public static final Reference TABLE_SETTINGS_BLOCKS_WRITE = createRef(
            Columns.TABLE_SETTINGS_BLOCKS_WRITE, DataTypes.BOOLEAN);
        public static final Reference TABLE_SETTINGS_BLOCKS_METADATA = createRef(
            Columns.TABLE_SETTINGS_BLOCKS_METADATA, DataTypes.BOOLEAN);

        public static final Reference TABLE_SETTINGS_TRANSLOG = createRef(
            Columns.TABLE_SETTINGS_TRANSLOG, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE = createRef(
            Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE, DataTypes.LONG);

        public static final Reference TABLE_SETTINGS_ROUTING = createRef(
            Columns.TABLE_SETTINGS_ROUTING, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_ROUTING_ALLOCATION = createRef(
            Columns.TABLE_SETTINGS_ROUTING_ALLOCATION, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE = createRef(
            Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE, DataTypes.STRING);
        public static final Reference TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE = createRef(
            Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE, DataTypes.INTEGER);

        public static final Reference TABLE_SETTINGS_MAPPING = createRef(
            Columns.TABLE_SETTINGS_MAPPING, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_MAPPING_TOTAL_FIELDS = createRef(
            Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT = createRef(
            Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT, DataTypes.INTEGER);

        public static final Reference TABLE_SETTINGS_RECOVERY = createRef(
            Columns.TABLE_SETTINGS_RECOVERY, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS = createRef(
            Columns.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS, DataTypes.STRING);
        public static final Reference TABLE_SETTINGS_WARMER = createRef(
            Columns.TABLE_SETTINGS_WARMER, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_WARMER_ENABLED = createRef(
            Columns.TABLE_SETTINGS_WARMER_ENABLED, DataTypes.BOOLEAN);

        public static final Reference TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL = createRef(
            Columns.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL, DataTypes.STRING);

        public static final Reference TABLE_SETTINGS_UNASSIGNED = createRef(
            Columns.TABLE_SETTINGS_UNASSIGNED, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_UNASSIGNED_NODE_LEFT = createRef(
            Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT = createRef(
            Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, DataTypes.LONG);
    }

    private static Reference createRef(ColumnIdent columnIdent, DataType dataType) {
        return new Reference(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    protected InformationPartitionsTableInfo(ClusterService clusterService) {
        super(clusterService,
            IDENT,
            ImmutableList.<ColumnIdent>of(),
            ImmutableSortedMap.<ColumnIdent, Reference>naturalOrder()
                .put(Columns.TABLE_NAME, References.TABLE_NAME)
                .put(PartitionsTableColumns.SCHEMA_NAME, References.SCHEMA_NAME)
                .put(Columns.PARTITION_IDENT, References.PARTITION_IDENT)
                .put(Columns.VALUES, References.VALUES)
                .put(Columns.NUMBER_OF_SHARDS, References.NUMBER_OF_SHARDS)
                .put(Columns.NUMBER_OF_REPLICAS, References.NUMBER_OF_REPLICAS)
                .put(Columns.ROUTING_HASH_FUNCTION, References.ROUTING_HASH_FUNCTION)
                .put(Columns.TABLE_VERSION, References.TABLE_VERSION)
                .put(Columns.TABLE_VERSION_CREATED, References.TABLE_VERSION_CREATED)
                .put(Columns.TABLE_VERSION_CREATED_CRATEDB, References.TABLE_VERSION_CREATED_CRATEDB)
                .put(Columns.TABLE_VERSION_CREATED_ES, References.TABLE_VERSION_CREATED_ES)
                .put(Columns.TABLE_VERSION_UPGRADED, References.TABLE_VERSION_UPGRADED)
                .put(Columns.TABLE_VERSION_UPGRADED_CRATEDB, References.TABLE_VERSION_UPGRADED_CRATEDB)
                .put(Columns.TABLE_VERSION_UPGRADED_ES, References.TABLE_VERSION_UPGRADED_ES)
                .put(Columns.CLOSED, References.CLOSED)
                .put(Columns.TABLE_SETTINGS, References.TABLE_SETTINGS)
                .put(Columns.TABLE_SETTINGS_BLOCKS, References.TABLE_SETTINGS_BLOCKS)
                .put(Columns.TABLE_SETTINGS_BLOCKS_READ_ONLY, References.TABLE_SETTINGS_BLOCKS_READ_ONLY)
                .put(Columns.TABLE_SETTINGS_BLOCKS_READ, References.TABLE_SETTINGS_BLOCKS_READ)
                .put(Columns.TABLE_SETTINGS_BLOCKS_WRITE, References.TABLE_SETTINGS_BLOCKS_WRITE)
                .put(Columns.TABLE_SETTINGS_BLOCKS_METADATA, References.TABLE_SETTINGS_BLOCKS_METADATA)
                .put(Columns.TABLE_SETTINGS_TRANSLOG, References.TABLE_SETTINGS_TRANSLOG)
                .put(Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE, References.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE)
                .put(Columns.TABLE_SETTINGS_ROUTING, References.TABLE_SETTINGS_ROUTING)
                .put(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION, References.TABLE_SETTINGS_ROUTING_ALLOCATION)
                .put(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE, References.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE)
                .put(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE, References.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE)
                .put(Columns.TABLE_SETTINGS_MAPPING, References.TABLE_SETTINGS_MAPPING)
                .put(Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS, References.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS)
                .put(Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT, References.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT)
                .put(Columns.TABLE_SETTINGS_RECOVERY, References.TABLE_SETTINGS_RECOVERY)
                .put(Columns.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS, References.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS)
                .put(Columns.TABLE_SETTINGS_WARMER, References.TABLE_SETTINGS_WARMER)
                .put(Columns.TABLE_SETTINGS_WARMER_ENABLED, References.TABLE_SETTINGS_WARMER_ENABLED)
                .put(Columns.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL, References.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL)
                .put(Columns.TABLE_SETTINGS_UNASSIGNED, References.TABLE_SETTINGS_UNASSIGNED)
                .put(Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT, References.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT)
                .put(Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, References.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT)
                .build(),
            ImmutableList.of(
                References.CLOSED,
                References.NUMBER_OF_REPLICAS,
                References.NUMBER_OF_SHARDS,
                References.PARTITION_IDENT,
                References.ROUTING_HASH_FUNCTION,
                References.SCHEMA_NAME,
                References.TABLE_SETTINGS,
                References.TABLE_NAME,
                References.VALUES,
                References.TABLE_VERSION
            )
        );
    }
}
