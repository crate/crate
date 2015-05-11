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

package io.crate.metadata.sys;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.*;
import io.crate.metadata.settings.CrateSettings;
import io.crate.planner.RowGranularity;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.*;

public class SysClusterTableInfo extends SysTableInfo {

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "cluster");
    public static final Routing ROUTING = new Routing(
            TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder().put(
                    NULL_NODE_ID,
                    TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(IDENT.fqn(), null).map()
            ).map()
    );
    private static final String[] PARTITIONS = new String[]{IDENT.name()};

    public static final Map<ColumnIdent, ReferenceInfo> INFOS = new LinkedHashMap<>();
    private static final LinkedHashSet<ReferenceInfo> columns = new LinkedHashSet<>();

    static {
        register("id", DataTypes.STRING, null);
        register("name", DataTypes.STRING, null);
        register("master_node", DataTypes.STRING, null);
        register("settings", DataTypes.OBJECT, null);

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.STATS.name()));
        register("settings", DataTypes.INTEGER, ImmutableList.of(CrateSettings.STATS.name(),
                CrateSettings.STATS_JOBS_LOG_SIZE.name()));
        register("settings", DataTypes.INTEGER, ImmutableList.of(CrateSettings.STATS.name(),
                CrateSettings.STATS_OPERATIONS_LOG_SIZE.name()));
        register("settings", DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.STATS.name(),
                CrateSettings.STATS_ENABLED.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.DISCOVERY.name()));
        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.DISCOVERY.name(),
                CrateSettings.DISCOVERY_ZEN.name()));
        register("settings", DataTypes.INTEGER, ImmutableList.of(CrateSettings.DISCOVERY.name(),
                CrateSettings.DISCOVERY_ZEN.name(),
                CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.DISCOVERY.name(),
                CrateSettings.DISCOVERY_ZEN.name(),
                CrateSettings.DISCOVERY_ZEN_PING_TIMEOUT.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.DISCOVERY.name(),
                CrateSettings.DISCOVERY_ZEN.name(),
                CrateSettings.DISCOVERY_ZEN_PUBLISH_TIMEOUT.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.GRACEFUL_STOP.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.GRACEFUL_STOP.name(),
                CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.name()));
        register("settings", DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.GRACEFUL_STOP.name(),
                CrateSettings.GRACEFUL_STOP_REALLOCATE.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.GRACEFUL_STOP.name(),
                CrateSettings.GRACEFUL_STOP_TIMEOUT.name()));
        register("settings", DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.GRACEFUL_STOP.name(),
                CrateSettings.GRACEFUL_STOP_FORCE.name()));
        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name()));
        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_ENABLE.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_ALLOW_REBALANCE.name()));
        register("settings", DataTypes.INTEGER, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE.name()));
        register("settings", DataTypes.INTEGER, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES.name()));
        register("settings", DataTypes.INTEGER, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_INCLUDE.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_INCLUDE.name(),
                CrateSettings.ROUTING_ALLOCATION_INCLUDE_IP.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_INCLUDE.name(),
                CrateSettings.ROUTING_ALLOCATION_INCLUDE_ID.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_INCLUDE.name(),
                CrateSettings.ROUTING_ALLOCATION_INCLUDE_HOST.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_INCLUDE.name(),
                CrateSettings.ROUTING_ALLOCATION_INCLUDE_NAME.name()));
        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_EXCLUDE.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_EXCLUDE.name(),
                CrateSettings.ROUTING_ALLOCATION_EXCLUDE_IP.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_EXCLUDE.name(),
                CrateSettings.ROUTING_ALLOCATION_EXCLUDE_ID.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_EXCLUDE.name(),
                CrateSettings.ROUTING_ALLOCATION_EXCLUDE_HOST.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_EXCLUDE.name(),
                CrateSettings.ROUTING_ALLOCATION_EXCLUDE_NAME.name()));
        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_REQUIRE.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_REQUIRE.name(),
                CrateSettings.ROUTING_ALLOCATION_REQUIRE_IP.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_REQUIRE.name(),
                CrateSettings.ROUTING_ALLOCATION_REQUIRE_ID.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_REQUIRE.name(),
                CrateSettings.ROUTING_ALLOCATION_REQUIRE_HOST.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_REQUIRE.name(),
                CrateSettings.ROUTING_ALLOCATION_REQUIRE_NAME.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_BALANCE.name()));
        register("settings", DataTypes.FLOAT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_BALANCE.name(),
                CrateSettings.ROUTING_ALLOCATION_BALANCE_SHARD.name()));
        register("settings", DataTypes.FLOAT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_BALANCE.name(),
                CrateSettings.ROUTING_ALLOCATION_BALANCE_INDEX.name()));
        register("settings", DataTypes.FLOAT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_BALANCE.name(),
                CrateSettings.ROUTING_ALLOCATION_BALANCE_PRIMARY.name()));
        register("settings", DataTypes.FLOAT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_BALANCE.name(),
                CrateSettings.ROUTING_ALLOCATION_BALANCE_THRESHOLD.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_DISK.name()));
        register("settings", DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_DISK.name(),
                CrateSettings.ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED.name()));
        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_DISK.name(),
                CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_DISK.name(),
                CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK.name(),
                CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_LOW.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_DISK.name(),
                CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK.name(),
                CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_HIGH.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name()));
        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name()));
        register("settings", DataTypes.INTEGER, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name(),
                CrateSettings.INDICES_RECOVERY_CONCURRENT_STREAMS.name()));
        register("settings", DataTypes.LONG, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name(),
                CrateSettings.INDICES_RECOVERY_FILE_CHUNK_SIZE.name()));
        register("settings", DataTypes.INTEGER, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name(),
                CrateSettings.INDICES_RECOVERY_TRANSLOG_OPS.name()));
        register("settings", DataTypes.LONG, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name(),
                CrateSettings.INDICES_RECOVERY_TRANSLOG_SIZE.name()));
        register("settings", DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name(),
                CrateSettings.INDICES_RECOVERY_COMPRESS.name()));
        register("settings", DataTypes.LONG, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name(),
                CrateSettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name(),
                CrateSettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name(),
                CrateSettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name(),
                CrateSettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name(),
                CrateSettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_RECOVERY.name(),
                CrateSettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_STORE.name()));
        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_STORE.name(),
                CrateSettings.INDICES_STORE_THROTTLE.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_STORE.name(),
                CrateSettings.INDICES_STORE_THROTTLE.name(),
                CrateSettings.INDICES_STORE_THROTTLE_TYPE.name()));
        register("settings", DataTypes.LONG, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_STORE.name(),
                CrateSettings.INDICES_STORE_THROTTLE.name(),
                CrateSettings.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_FIELDDATA.name()));
        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_FIELDDATA.name(),
                CrateSettings.INDICES_FIELDDATA_BREAKER.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_FIELDDATA.name(),
                CrateSettings.INDICES_FIELDDATA_BREAKER.name(),
                CrateSettings.INDICES_FIELDDATA_BREAKER_LIMIT.name()));
        register("settings", DataTypes.DOUBLE, ImmutableList.of(CrateSettings.INDICES.name(),
                CrateSettings.INDICES_FIELDDATA.name(),
                CrateSettings.INDICES_FIELDDATA_BREAKER.name(),
                CrateSettings.INDICES_FIELDDATA_BREAKER_OVERHEAD.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.CLUSTER_INFO.name()));
        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.CLUSTER_INFO.name(),
                CrateSettings.CLUSTER_INFO_UPDATE.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                CrateSettings.CLUSTER_INFO.name(),
                CrateSettings.CLUSTER_INFO_UPDATE.name(),
                CrateSettings.CLUSTER_INFO_UPDATE_INTERVAL.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.BULK.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.BULK.name(),
                CrateSettings.BULK_REQUEST_TIMEOUT.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.BULK.name(),
                CrateSettings.BULK_PARTITION_CREATION_TIMEOUT.name()));
    }

    @Inject
    protected SysClusterTableInfo(ClusterService clusterService, SysSchemaInfo sysSchemaInfo) {
        super(clusterService, sysSchemaInfo);
    }

    public static ReferenceInfo register(String column, DataType type, List<String> path) {
        ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(IDENT, column, path), RowGranularity.CLUSTER, type);
        if (info.ident().isColumn()) {
            columns.add(info);
        }
        INFOS.put(info.ident().columnIdent(), info);
        return info;
    }

    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        return INFOS.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return ROUTING;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return ImmutableList.of();
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.CLUSTER;
    }

    @Override
    public TableIdent ident() {
        return IDENT;
    }

    @Override
    public String[] concreteIndices() {
        return PARTITIONS;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return INFOS.values().iterator();
    }
}
