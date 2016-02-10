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
import org.elasticsearch.cluster.ClusterService;

public class InformationTablesTableInfo extends InformationTableInfo {

    public static final String NAME = "tables";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class ReferenceInfos {
        public static final ReferenceInfo SCHEMA_NAME = info(Columns.SCHEMA_NAME, DataTypes.STRING);
        public static final ReferenceInfo TABLE_NAME = info(Columns.TABLE_NAME, DataTypes.STRING);
        public static final ReferenceInfo NUMBER_OF_SHARDS = info(Columns.NUMBER_OF_SHARDS, DataTypes.INTEGER);
        public static final ReferenceInfo NUMBER_OF_REPLICAS = info(Columns.NUMBER_OF_REPLICAS, DataTypes.STRING);
        public static final ReferenceInfo CLUSTERED_BY = info(Columns.CLUSTERED_BY, DataTypes.STRING);
        public static final ReferenceInfo PARTITIONED_BY = info(Columns.PARTITIONED_BY, new ArrayType(DataTypes.STRING));
        public static final ReferenceInfo BLOBS_PATH = info(Columns.BLOBS_PATH, DataTypes.STRING);
        public static final ReferenceInfo COLUMN_POLICY = info(Columns.COLUMN_POLICY, DataTypes.STRING);

        public static final ReferenceInfo TABLE_SETTINGS = info(Columns.TABLE_SETTINGS, DataTypes.OBJECT);

        public static final ReferenceInfo TABLE_SETTINGS_REFRESH_INTERVAL = info(
                Columns.TABLE_SETTINGS_REFRESH_INTERVAL, DataTypes.LONG);

        public static final ReferenceInfo TABLE_SETTINGS_BLOCKS = info(
                Columns.TABLE_SETTINGS_BLOCKS, DataTypes.OBJECT);
        public static final ReferenceInfo TABLE_SETTINGS_BLOCKS_READ_ONLY = info(
                Columns.TABLE_SETTINGS_BLOCKS_READ_ONLY, DataTypes.BOOLEAN);
        public static final ReferenceInfo TABLE_SETTINGS_BLOCKS_READ = info(
                Columns.TABLE_SETTINGS_BLOCKS_READ, DataTypes.BOOLEAN);
        public static final ReferenceInfo TABLE_SETTINGS_BLOCKS_WRITE = info(
                Columns.TABLE_SETTINGS_BLOCKS_WRITE, DataTypes.BOOLEAN);
        public static final ReferenceInfo TABLE_SETTINGS_BLOCKS_METADATA = info(
                Columns.TABLE_SETTINGS_BLOCKS_METADATA, DataTypes.BOOLEAN);

        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG = info(
                Columns.TABLE_SETTINGS_TRANSLOG, DataTypes.OBJECT);
        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_OPS = info(
                Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_OPS, DataTypes.INTEGER);
        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE = info(
                Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE, DataTypes.LONG);
        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_PERIOD = info(
                Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_PERIOD, DataTypes.LONG);
        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG_DISABLE_FLUSH = info(
                Columns.TABLE_SETTINGS_TRANSLOG_DISABLE_FLUSH, DataTypes.BOOLEAN);
        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG_INTERVAL = info(
                Columns.TABLE_SETTINGS_TRANSLOG_INTERVAL, DataTypes.LONG);
        
        public static final ReferenceInfo TABLE_SETTINGS_ROUTING= info(
                Columns.TABLE_SETTINGS_ROUTING, DataTypes.OBJECT);
        public static final ReferenceInfo TABLE_SETTINGS_ROUTING_ALLOCATION = info(
                Columns.TABLE_SETTINGS_ROUTING_ALLOCATION, DataTypes.OBJECT);
        public static final ReferenceInfo TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE = info(
                Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE, DataTypes.STRING);
        public static final ReferenceInfo TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE = info(
                Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE, DataTypes.INTEGER);

        public static final ReferenceInfo TABLE_SETTINGS_RECOVERY = info(
                Columns.TABLE_SETTINGS_RECOVERY, DataTypes.OBJECT);
        public static final ReferenceInfo TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS = info(
                Columns.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS, DataTypes.STRING);
        public static final ReferenceInfo TABLE_SETTINGS_WARMER = info(
                Columns.TABLE_SETTINGS_WARMER, DataTypes.OBJECT);
        public static final ReferenceInfo TABLE_SETTINGS_WARMER_ENABLED = info(
                Columns.TABLE_SETTINGS_WARMER_ENABLED, DataTypes.BOOLEAN);

        public static final ReferenceInfo TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL = info(
                Columns.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL, DataTypes.LONG);

        public static final ReferenceInfo TABLE_SETTINGS_UNASSIGNED = info(
                Columns.TABLE_SETTINGS_UNASSIGNED, DataTypes.OBJECT);
        public static final ReferenceInfo TABLE_SETTINGS_UNASSIGNED_NODE_LEFT = info(
                Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT, DataTypes.OBJECT);
        public static final ReferenceInfo TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT = info(
                Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, DataTypes.LONG);
    }

    private static ReferenceInfo info(ColumnIdent columnIdent, DataType dataType) {
        return new ReferenceInfo(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    public InformationTablesTableInfo(ClusterService clusterService) {
        super(clusterService,
                IDENT,
                ImmutableList.of(Columns.SCHEMA_NAME, Columns.TABLE_NAME),
                ImmutableSortedMap.<ColumnIdent, ReferenceInfo>naturalOrder()
                   .put(Columns.SCHEMA_NAME, ReferenceInfos.SCHEMA_NAME)
                   .put(Columns.TABLE_NAME, ReferenceInfos.TABLE_NAME)
                   .put(Columns.NUMBER_OF_SHARDS, ReferenceInfos.NUMBER_OF_SHARDS)
                   .put(Columns.NUMBER_OF_REPLICAS, ReferenceInfos.NUMBER_OF_REPLICAS)
                   .put(Columns.CLUSTERED_BY, ReferenceInfos.CLUSTERED_BY)
                   .put(Columns.PARTITIONED_BY, ReferenceInfos.PARTITIONED_BY)
                   .put(Columns.BLOBS_PATH, ReferenceInfos.BLOBS_PATH)
                   .put(Columns.COLUMN_POLICY, ReferenceInfos.COLUMN_POLICY)
                   .put(Columns.TABLE_SETTINGS, ReferenceInfos.TABLE_SETTINGS)
                   .put(Columns.TABLE_SETTINGS_BLOCKS, ReferenceInfos.TABLE_SETTINGS_BLOCKS)
                   .put(Columns.TABLE_SETTINGS_BLOCKS_READ_ONLY, ReferenceInfos.TABLE_SETTINGS_BLOCKS_READ_ONLY)
                   .put(Columns.TABLE_SETTINGS_BLOCKS_READ, ReferenceInfos.TABLE_SETTINGS_BLOCKS_READ)
                   .put(Columns.TABLE_SETTINGS_BLOCKS_WRITE, ReferenceInfos.TABLE_SETTINGS_BLOCKS_WRITE)
                   .put(Columns.TABLE_SETTINGS_BLOCKS_METADATA, ReferenceInfos.TABLE_SETTINGS_BLOCKS_METADATA)
                   .put(Columns.TABLE_SETTINGS_TRANSLOG, ReferenceInfos.TABLE_SETTINGS_TRANSLOG)
                   .put(Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_OPS, ReferenceInfos.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_OPS)
                   .put(Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE, ReferenceInfos.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE)
                   .put(Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_PERIOD, ReferenceInfos.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_PERIOD)
                   .put(Columns.TABLE_SETTINGS_TRANSLOG_DISABLE_FLUSH, ReferenceInfos.TABLE_SETTINGS_TRANSLOG_DISABLE_FLUSH)
                   .put(Columns.TABLE_SETTINGS_TRANSLOG_INTERVAL, ReferenceInfos.TABLE_SETTINGS_TRANSLOG_INTERVAL)
                   .put(Columns.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL, ReferenceInfos.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL)
                   .put(Columns.TABLE_SETTINGS_REFRESH_INTERVAL, ReferenceInfos.TABLE_SETTINGS_REFRESH_INTERVAL)
                   .put(Columns.TABLE_SETTINGS_ROUTING, ReferenceInfos.TABLE_SETTINGS_ROUTING)
                   .put(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION, ReferenceInfos.TABLE_SETTINGS_ROUTING_ALLOCATION)
                   .put(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE, ReferenceInfos.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE)
                   .put(Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE, ReferenceInfos.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE)
                   .put(Columns.TABLE_SETTINGS_RECOVERY, ReferenceInfos.TABLE_SETTINGS_RECOVERY)
                   .put(Columns.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS, ReferenceInfos.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS)
                   .put(Columns.TABLE_SETTINGS_WARMER, ReferenceInfos.TABLE_SETTINGS_WARMER)
                   .put(Columns.TABLE_SETTINGS_WARMER_ENABLED, ReferenceInfos.TABLE_SETTINGS_WARMER_ENABLED)
                   .put(Columns.TABLE_SETTINGS_UNASSIGNED, ReferenceInfos.TABLE_SETTINGS_UNASSIGNED)
                   .put(Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT, ReferenceInfos.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT)
                   .put(Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, ReferenceInfos.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT)
                .build(),
                ImmutableList.of(
                        ReferenceInfos.BLOBS_PATH,
                        ReferenceInfos.CLUSTERED_BY,
                        ReferenceInfos.COLUMN_POLICY,
                        ReferenceInfos.NUMBER_OF_REPLICAS,
                        ReferenceInfos.NUMBER_OF_SHARDS,
                        ReferenceInfos.PARTITIONED_BY,
                        ReferenceInfos.SCHEMA_NAME,
                        ReferenceInfos.TABLE_SETTINGS,
                        ReferenceInfos.TABLE_NAME
                )
        );
    }
}
