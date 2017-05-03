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

package io.crate.metadata.information;

import com.google.common.collect.ImmutableList;
import io.crate.Version;
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.metadata.table.StaticTableInfo;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import java.util.Map;

public class InformationTableInfo extends StaticTableInfo {

    private final ClusterService clusterService;

    public static class Columns {
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        public static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        public static final ColumnIdent PARTITION_IDENT = new ColumnIdent("partition_ident");
        public static final ColumnIdent VALUES = new ColumnIdent("values");
        public static final ColumnIdent NUMBER_OF_SHARDS = new ColumnIdent("number_of_shards");
        public static final ColumnIdent NUMBER_OF_REPLICAS = new ColumnIdent("number_of_replicas");
        public static final ColumnIdent CLUSTERED_BY = new ColumnIdent("clustered_by");
        public static final ColumnIdent PARTITIONED_BY = new ColumnIdent("partitioned_by");
        public static final ColumnIdent BLOBS_PATH = new ColumnIdent("blobs_path");
        public static final ColumnIdent COLUMN_POLICY = new ColumnIdent("column_policy");
        public static final ColumnIdent ROUTING_HASH_FUNCTION = new ColumnIdent("routing_hash_function");
        public static final ColumnIdent TABLE_VERSION = new ColumnIdent("version");
        public static final ColumnIdent TABLE_VERSION_CREATED = new ColumnIdent("version",
            ImmutableList.of(Version.Property.CREATED.toString()));
        public static final ColumnIdent TABLE_VERSION_CREATED_CRATEDB = new ColumnIdent("version",
            ImmutableList.of(Version.Property.CREATED.toString(), Version.CRATEDB_VERSION_KEY));
        public static final ColumnIdent TABLE_VERSION_CREATED_ES = new ColumnIdent("version",
            ImmutableList.of(Version.Property.CREATED.toString(), Version.ES_VERSION_KEY));
        public static final ColumnIdent TABLE_VERSION_UPGRADED = new ColumnIdent("version",
            ImmutableList.of(Version.Property.UPGRADED.toString()));
        public static final ColumnIdent TABLE_VERSION_UPGRADED_CRATEDB = new ColumnIdent("version",
            ImmutableList.of(Version.Property.UPGRADED.toString(), Version.CRATEDB_VERSION_KEY));
        public static final ColumnIdent TABLE_VERSION_UPGRADED_ES = new ColumnIdent("version",
            ImmutableList.of(Version.Property.UPGRADED.toString(), Version.ES_VERSION_KEY));
        public static final ColumnIdent CLOSED = new ColumnIdent("closed");
        public static final ColumnIdent TABLE_SETTINGS = new ColumnIdent("settings");
        public static final ColumnIdent TABLE_SETTINGS_BLOCKS = new ColumnIdent("settings",
            ImmutableList.of("blocks"));
        public static final ColumnIdent TABLE_SETTINGS_BLOCKS_READ_ONLY = new ColumnIdent("settings",
            ImmutableList.of("blocks", "read_only"));
        public static final ColumnIdent TABLE_SETTINGS_BLOCKS_READ = new ColumnIdent("settings",
            ImmutableList.of("blocks", "read"));
        public static final ColumnIdent TABLE_SETTINGS_BLOCKS_WRITE = new ColumnIdent("settings",
            ImmutableList.of("blocks", "write"));
        public static final ColumnIdent TABLE_SETTINGS_BLOCKS_METADATA = new ColumnIdent("settings",
            ImmutableList.of("blocks", "metadata"));
        public static final ColumnIdent TABLE_SETTINGS_ROUTING = new ColumnIdent("settings",
            ImmutableList.of("routing"));
        public static final ColumnIdent TABLE_SETTINGS_ROUTING_ALLOCATION = new ColumnIdent("settings",
            ImmutableList.of("routing", "allocation"));
        public static final ColumnIdent TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE = new ColumnIdent("settings",
            ImmutableList.of("routing", "allocation", "enable"));
        public static final ColumnIdent TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE = new ColumnIdent("settings",
            ImmutableList.of("routing", "allocation", "total_shards_per_node"));
        public static final ColumnIdent TABLE_SETTINGS_RECOVERY = new ColumnIdent("settings",
            ImmutableList.of("recovery"));
        public static final ColumnIdent TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS = new ColumnIdent("settings",
            ImmutableList.of("recovery", "initial_shards"));
        public static final ColumnIdent TABLE_SETTINGS_WARMER = new ColumnIdent("settings",
            ImmutableList.of("warmer"));
        public static final ColumnIdent TABLE_SETTINGS_WARMER_ENABLED = new ColumnIdent("settings",
            ImmutableList.of("warmer", "enabled"));

        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG = new ColumnIdent("settings",
            ImmutableList.of("translog"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE = new ColumnIdent("settings",
            ImmutableList.of("translog", "flush_threshold_size"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL = new ColumnIdent("settings",
            ImmutableList.of("translog", "sync_interval"));

        public static final ColumnIdent TABLE_SETTINGS_REFRESH_INTERVAL = new ColumnIdent("settings",
            ImmutableList.of("refresh_interval"));
        public static final ColumnIdent TABLE_SETTINGS_UNASSIGNED = new ColumnIdent("settings",
            ImmutableList.of("unassigned"));
        public static final ColumnIdent TABLE_SETTINGS_UNASSIGNED_NODE_LEFT = new ColumnIdent("settings",
            ImmutableList.of("unassigned", "node_left"));
        public static final ColumnIdent TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT = new ColumnIdent("settings",
            ImmutableList.of("unassigned", "node_left", "delayed_timeout"));
    }

    protected InformationTableInfo(ClusterService clusterService,
                                   TableIdent ident,
                                   ImmutableList<ColumnIdent> primaryKeyIdentList,
                                   Map<ColumnIdent, Reference> references) {
        this(clusterService, ident, primaryKeyIdentList, references, null);
    }

    protected InformationTableInfo(ClusterService clusterService,
                                   TableIdent ident,
                                   ImmutableList<ColumnIdent> primaryKeyIdentList,
                                   Map<ColumnIdent, Reference> references,
                                   @Nullable ImmutableList<Reference> columns) {
        super(ident, references, columns, primaryKeyIdentList);
        this.clusterService = clusterService;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return Routing.forTableOnSingleNode(ident(), clusterService.localNode().getId());
    }
}
