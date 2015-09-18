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
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.AbstractTableInfo;
import io.crate.planner.RowGranularity;
import org.elasticsearch.cluster.ClusterService;

import javax.annotation.Nullable;
import java.util.*;

public class InformationTableInfo extends AbstractTableInfo {

    private final ClusterService clusterService;
    protected final TableIdent ident;
    private final ImmutableList<ColumnIdent> primaryKeyIdentList;

    private final ImmutableMap<ColumnIdent, ReferenceInfo> references;
    private final ImmutableList<ReferenceInfo> columns;

    public static class Columns {
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        public static final ColumnIdent SCHEMA_NAME = new ColumnIdent("schema_name");
        public static final ColumnIdent PARTITION_IDENT = new ColumnIdent("partition_ident");
        public static final ColumnIdent VALUES = new ColumnIdent("values");
        public static final ColumnIdent NUMBER_OF_SHARDS = new ColumnIdent("number_of_shards");
        public static final ColumnIdent NUMBER_OF_REPLICAS = new ColumnIdent("number_of_replicas");
        public static final ColumnIdent CLUSTERED_BY = new ColumnIdent("clustered_by");
        public static final ColumnIdent PARTITIONED_BY = new ColumnIdent("partitioned_by");
        public static final ColumnIdent BLOBS_PATH = new ColumnIdent("blobs_path");
        public static final ColumnIdent COLUMN_POLICY = new ColumnIdent("column_policy");
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
        public static final ColumnIdent TABLE_SETTINGS_ROUTING= new ColumnIdent("settings",
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
                ImmutableList.of("recovery","initial_shards"));
        public static final ColumnIdent TABLE_SETTINGS_WARMER = new ColumnIdent("settings",
                ImmutableList.of("warmer"));
        public static final ColumnIdent TABLE_SETTINGS_WARMER_ENABLED = new ColumnIdent("settings",
                ImmutableList.of("warmer", "enabled"));
        public static final ColumnIdent TABLE_SETTINGS_GATEWAY = new ColumnIdent("settings",
                ImmutableList.of("gateway"));
        public static final ColumnIdent TABLE_SETTINGS_GATEWAY_LOCAL = new ColumnIdent("settings",
                ImmutableList.of("gateway", "local"));
        public static final ColumnIdent TABLE_SETTINGS_GATEWAY_LOCAL_SYNC = new ColumnIdent("settings",
                ImmutableList.of("gateway", "local", "sync"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG = new ColumnIdent("settings",
                ImmutableList.of("translog"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_OPS = new ColumnIdent("settings",
                ImmutableList.of("translog", "flush_threshold_ops"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE = new ColumnIdent("settings",
                ImmutableList.of("translog", "flush_threshold_size"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_PERIOD = new ColumnIdent("settings",
                ImmutableList.of("translog", "flush_threshold_period"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG_DISABLE_FLUSH = new ColumnIdent("settings",
                ImmutableList.of("translog", "disable_flush"));
        public static final ColumnIdent TABLE_SETTINGS_TRANSLOG_INTERVAL = new ColumnIdent("settings",
                ImmutableList.of("translog", "interval"));
        public static final ColumnIdent TABLE_SETTINGS_REFRESH_INTERVAL = new ColumnIdent("settings",
                ImmutableList.of("refresh_interval"));
        public static final ColumnIdent TABLE_SETTINGS_UNASSIGNED = new ColumnIdent("settings",
                ImmutableList.of("unassigned"));
        public static final ColumnIdent TABLE_SETTINGS_UNASSIGNED_NODE_LEFT = new ColumnIdent("settings",
                ImmutableList.of("unassigned", "node_left"));
        public static final ColumnIdent TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT = new ColumnIdent("settings",
                ImmutableList.of("unassigned", "node_left", "delayed_timeout"));
    }

    protected InformationTableInfo(InformationSchemaInfo schemaInfo,
                                   ClusterService clusterService,
                                   TableIdent ident,
                                   ImmutableList<ColumnIdent> primaryKeyIdentList,
                                   LinkedHashMap<ColumnIdent, ReferenceInfo> references) {
        this(schemaInfo, clusterService, ident, primaryKeyIdentList, references, null);
    }

    protected InformationTableInfo(InformationSchemaInfo schemaInfo,
                                   ClusterService clusterService,
                                   TableIdent ident,
                                   ImmutableList<ColumnIdent> primaryKeyIdentList,
                                   LinkedHashMap<ColumnIdent, ReferenceInfo> references,
                                   @Nullable ImmutableList<ReferenceInfo> columns) {
        super(schemaInfo);
        this.clusterService = clusterService;
        this.ident = ident;
        this.primaryKeyIdentList = primaryKeyIdentList;
        this.references = ImmutableMap.copyOf(references);
        this.columns = columns != null ? columns : ImmutableList.copyOf(references.values());
    }

    @Nullable
    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        return references.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        Map<String, List<Integer>> tableLocation = new TreeMap<>();
        tableLocation.put(ident.fqn(), null);
        locations.put(clusterService.localNode().id(), tableLocation);
        return new Routing(locations);
    }

    @Override
    public TableIdent ident() {
        return ident;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return primaryKeyIdentList;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return references.values().iterator();
    }
}
