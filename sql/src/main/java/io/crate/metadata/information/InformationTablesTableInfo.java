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
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

public class InformationTablesTableInfo extends InformationTableInfo {

    public static final String NAME = "tables";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class References {
        public static final Reference TABLE_SCHEMA = createRef(Columns.TABLE_SCHEMA, DataTypes.STRING);
        public static final Reference TABLE_NAME = createRef(Columns.TABLE_NAME, DataTypes.STRING);
        public static final Reference TABLE_CATALOG = createRef(Columns.TABLE_CATALOG, DataTypes.STRING);
        public static final Reference TABLE_TYPE = createRef(Columns.TABLE_TYPE, DataTypes.STRING);
        public static final Reference NUMBER_OF_SHARDS = createRef(Columns.NUMBER_OF_SHARDS, DataTypes.INTEGER);
        public static final Reference NUMBER_OF_REPLICAS = createRef(Columns.NUMBER_OF_REPLICAS, DataTypes.STRING);
        public static final Reference CLUSTERED_BY = createRef(Columns.CLUSTERED_BY, DataTypes.STRING);
        public static final Reference PARTITIONED_BY = createRef(Columns.PARTITIONED_BY, new ArrayType(DataTypes.STRING));
        public static final Reference BLOBS_PATH = createRef(Columns.BLOBS_PATH, DataTypes.STRING);
        public static final Reference COLUMN_POLICY = createRef(Columns.COLUMN_POLICY, DataTypes.STRING);
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
        public static final Reference REFERENCE_GENERATION = createRef(Columns.REFERENCE_GENERATION, DataTypes.STRING);
        public static final Reference SELF_REFERENCING_COLUMN_NAME = createRef(Columns.SELF_REFERENCING_COLUMN_NAME,
            DataTypes.STRING);

        public static final Reference TABLE_SETTINGS = createRef(Columns.TABLE_SETTINGS, DataTypes.OBJECT);

        public static final Reference TABLE_SETTINGS_REFRESH_INTERVAL = createRef(
            Columns.TABLE_SETTINGS_REFRESH_INTERVAL, DataTypes.LONG);

        public static final Reference TABLE_SETTINGS_WRITE = createRef(
            Columns.TABLE_SETTINGS_WRITE, DataTypes.OBJECT);

        public static final Reference TABLE_SETTINGS_WRITE_WAIT_FOT_ACTIVE_SHARDS = createRef(
            Columns.TABLE_SETTINGS_WRITE_WAIT_FOT_ACTIVE_SHARDS, DataTypes.STRING);

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

        public static final Reference TABLE_SETTINGS_RECOVERY = createRef(
            Columns.TABLE_SETTINGS_RECOVERY, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS = createRef(
            Columns.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS, DataTypes.STRING);
        public static final Reference TABLE_SETTINGS_WARMER = createRef(
            Columns.TABLE_SETTINGS_WARMER, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_WARMER_ENABLED = createRef(
            Columns.TABLE_SETTINGS_WARMER_ENABLED, DataTypes.BOOLEAN);

        public static final Reference TABLE_SETTINGS_MAPPING = createRef(
            Columns.TABLE_SETTINGS_MAPPING, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_MAPPING_TOTAL_FIELDS = createRef(
            Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS, DataTypes.OBJECT);
        public static final Reference TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT = createRef(
            Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT, DataTypes.INTEGER);

        public static final Reference TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL = createRef(
            Columns.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL, DataTypes.LONG);

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

    public InformationTablesTableInfo(ClusterService clusterService) {
        super(clusterService,
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
                .put(Columns.TABLE_SETTINGS_RECOVERY, References.TABLE_SETTINGS_RECOVERY)
                .put(Columns.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS, References.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS)
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
