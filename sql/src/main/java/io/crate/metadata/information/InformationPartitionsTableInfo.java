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
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionInfo;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocIndexMetaData;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.operation.reference.partitioned.PartitionsSettingsExpression;
import io.crate.operation.reference.partitioned.PartitionsVersionExpression;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.Map;

public class InformationPartitionsTableInfo extends InformationTableInfo {

    public static final String NAME = "table_partitions";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        static final ColumnIdent SCHEMA_NAME = new ColumnIdent("schema_name");
    }

    public static class References {
        public static final Reference TABLE_NAME = createRef(InformationTablesTableInfo.Columns.TABLE_NAME, DataTypes.STRING);
        static final Reference SCHEMA_NAME = createRef(Columns.SCHEMA_NAME, DataTypes.STRING);
        static final Reference PARTITION_IDENT = createRef(InformationTablesTableInfo.Columns.PARTITION_IDENT, DataTypes.STRING);
        public static final Reference VALUES = createRef(InformationTablesTableInfo.Columns.VALUES, DataTypes.OBJECT);
        static final Reference NUMBER_OF_SHARDS = createRef(InformationTablesTableInfo.Columns.NUMBER_OF_SHARDS, DataTypes.INTEGER);
        public static final Reference NUMBER_OF_REPLICAS = createRef(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS, DataTypes.STRING);
        static final Reference ROUTING_HASH_FUNCTION = createRef(InformationTablesTableInfo.Columns.ROUTING_HASH_FUNCTION, DataTypes.STRING);
        static final Reference TABLE_VERSION = createRef(InformationTablesTableInfo.Columns.TABLE_VERSION, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_CREATED = createRef(
            InformationTablesTableInfo.Columns.TABLE_VERSION_CREATED, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_CREATED_CRATEDB = createRef(
            InformationTablesTableInfo.Columns.TABLE_VERSION_CREATED_CRATEDB, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_CREATED_ES = createRef(
            InformationTablesTableInfo.Columns.TABLE_VERSION_CREATED_ES, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_UPGRADED = createRef(
            InformationTablesTableInfo.Columns.TABLE_VERSION_UPGRADED, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_UPGRADED_CRATEDB = createRef(
            InformationTablesTableInfo.Columns.TABLE_VERSION_UPGRADED_CRATEDB, DataTypes.OBJECT);
        static final Reference TABLE_VERSION_UPGRADED_ES = createRef(
            InformationTablesTableInfo.Columns.TABLE_VERSION_UPGRADED_ES, DataTypes.OBJECT);
        static final Reference CLOSED = createRef(InformationTablesTableInfo.Columns.CLOSED, DataTypes.BOOLEAN);

        static final Reference TABLE_SETTINGS = createRef(InformationTablesTableInfo.Columns.TABLE_SETTINGS, DataTypes.OBJECT);

        static final Reference TABLE_SETTINGS_BLOCKS = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_BLOCKS_READ_ONLY = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_READ_ONLY, DataTypes.BOOLEAN);
        static final Reference TABLE_SETTINGS_BLOCKS_READ = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_READ, DataTypes.BOOLEAN);
        static final Reference TABLE_SETTINGS_BLOCKS_WRITE = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_WRITE, DataTypes.BOOLEAN);
        static final Reference TABLE_SETTINGS_BLOCKS_METADATA = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_METADATA, DataTypes.BOOLEAN);

        static final Reference TABLE_SETTINGS_TRANSLOG = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_TRANSLOG, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE, DataTypes.LONG);

        static final Reference TABLE_SETTINGS_ROUTING = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_ROUTING_ALLOCATION = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING_ALLOCATION, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE, DataTypes.STRING);
        static final Reference TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE, DataTypes.INTEGER);

        static final Reference TABLE_SETTINGS_MAPPING = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_MAPPING, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_MAPPING_TOTAL_FIELDS = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT, DataTypes.INTEGER);

        static final Reference TABLE_SETTINGS_RECOVERY = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_RECOVERY, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS, DataTypes.STRING);
        static final Reference TABLE_SETTINGS_WARMER = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_WARMER, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_WARMER_ENABLED = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_WARMER_ENABLED, DataTypes.BOOLEAN);

        static final Reference TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL, DataTypes.STRING);

        static final Reference TABLE_SETTINGS_UNASSIGNED = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_UNASSIGNED, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_UNASSIGNED_NODE_LEFT = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT, DataTypes.OBJECT);
        static final Reference TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT = createRef(
            InformationTablesTableInfo.Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, DataTypes.LONG);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<PartitionInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<PartitionInfo>>builder()
            .put(InformationTablesTableInfo.Columns.TABLE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.name().tableIdent().name()))
            .put(Columns.SCHEMA_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.name().tableIdent().schema()))
            .put(InformationTablesTableInfo.Columns.TABLE_CATALOG,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.name().tableIdent().schema()))
            .put(InformationTablesTableInfo.Columns.TABLE_TYPE,
                () -> RowContextCollectorExpression.objToBytesRef(r -> InformationTablesTableInfo.TABLE_TYPE))
            .put(InformationTablesTableInfo.Columns.PARTITION_IDENT,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.name().ident()))
            .put(InformationTablesTableInfo.Columns.VALUES,
                () -> RowContextCollectorExpression.forFunction(PartitionInfo::values))
            .put(InformationTablesTableInfo.Columns.NUMBER_OF_SHARDS,
                () -> RowContextCollectorExpression.forFunction(PartitionInfo::numberOfShards))
            .put(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS,
                () -> RowContextCollectorExpression.objToBytesRef(PartitionInfo::numberOfReplicas))
            .put(InformationTablesTableInfo.Columns.ROUTING_HASH_FUNCTION,
                () -> RowContextCollectorExpression.objToBytesRef(r -> DocIndexMetaData.getRoutingHashFunctionPrettyName(r.routingHashFunction())))
            .put(InformationTablesTableInfo.Columns.CLOSED,
                () -> RowContextCollectorExpression.forFunction(PartitionInfo::isClosed))
            .put(InformationTablesTableInfo.Columns.TABLE_VERSION, PartitionsVersionExpression::new)
            .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS, PartitionsSettingsExpression::new)
            .put(InformationTablesTableInfo.Columns.SELF_REFERENCING_COLUMN_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> InformationTablesTableInfo.SELF_REFERENCING_COLUMN_NAME))
            .put(InformationTablesTableInfo.Columns.REFERENCE_GENERATION,
                () -> RowContextCollectorExpression.objToBytesRef(r -> InformationTablesTableInfo.REFERENCE_GENERATION))
            .build();
    }


    private static Reference createRef(ColumnIdent columnIdent, DataType dataType) {
        return new Reference(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    InformationPartitionsTableInfo(ClusterService clusterService) {
        super(clusterService,
            IDENT,
            ImmutableList.of(),
            ImmutableSortedMap.<ColumnIdent, Reference>naturalOrder()
                .put(InformationTablesTableInfo.Columns.TABLE_NAME, References.TABLE_NAME)
                .put(Columns.SCHEMA_NAME, References.SCHEMA_NAME)
                .put(InformationTablesTableInfo.Columns.PARTITION_IDENT, References.PARTITION_IDENT)
                .put(InformationTablesTableInfo.Columns.VALUES, References.VALUES)
                .put(InformationTablesTableInfo.Columns.NUMBER_OF_SHARDS, References.NUMBER_OF_SHARDS)
                .put(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS, References.NUMBER_OF_REPLICAS)
                .put(InformationTablesTableInfo.Columns.ROUTING_HASH_FUNCTION, References.ROUTING_HASH_FUNCTION)
                .put(InformationTablesTableInfo.Columns.TABLE_VERSION, References.TABLE_VERSION)
                .put(InformationTablesTableInfo.Columns.TABLE_VERSION_CREATED, References.TABLE_VERSION_CREATED)
                .put(InformationTablesTableInfo.Columns.TABLE_VERSION_CREATED_CRATEDB, References.TABLE_VERSION_CREATED_CRATEDB)
                .put(InformationTablesTableInfo.Columns.TABLE_VERSION_CREATED_ES, References.TABLE_VERSION_CREATED_ES)
                .put(InformationTablesTableInfo.Columns.TABLE_VERSION_UPGRADED, References.TABLE_VERSION_UPGRADED)
                .put(InformationTablesTableInfo.Columns.TABLE_VERSION_UPGRADED_CRATEDB, References.TABLE_VERSION_UPGRADED_CRATEDB)
                .put(InformationTablesTableInfo.Columns.TABLE_VERSION_UPGRADED_ES, References.TABLE_VERSION_UPGRADED_ES)
                .put(InformationTablesTableInfo.Columns.CLOSED, References.CLOSED)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS, References.TABLE_SETTINGS)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS, References.TABLE_SETTINGS_BLOCKS)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_READ_ONLY, References.TABLE_SETTINGS_BLOCKS_READ_ONLY)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_READ, References.TABLE_SETTINGS_BLOCKS_READ)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_WRITE, References.TABLE_SETTINGS_BLOCKS_WRITE)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_METADATA, References.TABLE_SETTINGS_BLOCKS_METADATA)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_TRANSLOG, References.TABLE_SETTINGS_TRANSLOG)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE, References.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING, References.TABLE_SETTINGS_ROUTING)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING_ALLOCATION, References.TABLE_SETTINGS_ROUTING_ALLOCATION)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE, References.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE, References.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_MAPPING, References.TABLE_SETTINGS_MAPPING)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS, References.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT, References.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_RECOVERY, References.TABLE_SETTINGS_RECOVERY)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS, References.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_WARMER, References.TABLE_SETTINGS_WARMER)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_WARMER_ENABLED, References.TABLE_SETTINGS_WARMER_ENABLED)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL, References.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_UNASSIGNED, References.TABLE_SETTINGS_UNASSIGNED)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT, References.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT)
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, References.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT)
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
