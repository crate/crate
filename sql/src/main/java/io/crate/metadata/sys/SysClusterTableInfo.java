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
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.operation.reference.sys.cluster.ClusterLoggingOverridesExpression;
import io.crate.operation.reference.sys.cluster.ClusterSettingsExpression;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.Collections;

@Singleton
public class SysClusterTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "cluster");

    private final ClusterService clusterService;

    @Inject
    public SysClusterTableInfo(ClusterService clusterService) {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.CLUSTER)
                .register("id", DataTypes.STRING, null)
                .register("name", DataTypes.STRING, null)
                .register("master_node", DataTypes.STRING, null)
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, null)
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.STATS.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.INTEGER, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_JOBS_LOG_SIZE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_JOBS_LOG_EXPIRATION.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.INTEGER, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_OPERATIONS_LOG_SIZE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_OPERATIONS_LOG_EXPIRATION.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_ENABLED.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_SERVICE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_SERVICE.name(),
                    CrateSettings.STATS_SERVICE_INTERVAL.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_BREAKER.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_BREAKER.name(),
                    CrateSettings.STATS_BREAKER_LOG.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_BREAKER.name(),
                    CrateSettings.STATS_BREAKER_LOG.name(),
                    CrateSettings.STATS_BREAKER_LOG_JOBS.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_BREAKER.name(),
                    CrateSettings.STATS_BREAKER_LOG.name(),
                    CrateSettings.STATS_BREAKER_LOG_JOBS.name(),
                    CrateSettings.STATS_BREAKER_LOG_JOBS_LIMIT.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_BREAKER.name(),
                    CrateSettings.STATS_BREAKER_LOG.name(),
                    CrateSettings.STATS_BREAKER_LOG_OPERATIONS.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.STATS.name(),
                    CrateSettings.STATS_BREAKER.name(),
                    CrateSettings.STATS_BREAKER_LOG.name(),
                    CrateSettings.STATS_BREAKER_LOG_OPERATIONS.name(),
                    CrateSettings.STATS_BREAKER_LOG_OPERATIONS_LIMIT.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.DISCOVERY.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.DISCOVERY.name(),
                    CrateSettings.DISCOVERY_ZEN.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.INTEGER, ImmutableList.of(CrateSettings.DISCOVERY.name(),
                    CrateSettings.DISCOVERY_ZEN.name(),
                    CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.DISCOVERY.name(),
                    CrateSettings.DISCOVERY_ZEN.name(),
                    CrateSettings.DISCOVERY_ZEN_PING_TIMEOUT.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.DISCOVERY.name(),
                    CrateSettings.DISCOVERY_ZEN.name(),
                    CrateSettings.DISCOVERY_ZEN_PUBLISH_TIMEOUT.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.GRACEFUL_STOP.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.GRACEFUL_STOP.name(),
                    CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.GRACEFUL_STOP.name(),
                    CrateSettings.GRACEFUL_STOP_REALLOCATE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.GRACEFUL_STOP.name(),
                    CrateSettings.GRACEFUL_STOP_TIMEOUT.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.GRACEFUL_STOP.name(),
                    CrateSettings.GRACEFUL_STOP_FORCE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_ENABLE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_ALLOW_REBALANCE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.INTEGER, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.INTEGER, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.INTEGER, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_INCLUDE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_INCLUDE.name(),
                    CrateSettings.ROUTING_ALLOCATION_INCLUDE_IP.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_INCLUDE.name(),
                    CrateSettings.ROUTING_ALLOCATION_INCLUDE_ID.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_INCLUDE.name(),
                    CrateSettings.ROUTING_ALLOCATION_INCLUDE_HOST.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_INCLUDE.name(),
                    CrateSettings.ROUTING_ALLOCATION_INCLUDE_NAME.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_EXCLUDE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_EXCLUDE.name(),
                    CrateSettings.ROUTING_ALLOCATION_EXCLUDE_IP.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_EXCLUDE.name(),
                    CrateSettings.ROUTING_ALLOCATION_EXCLUDE_ID.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_EXCLUDE.name(),
                    CrateSettings.ROUTING_ALLOCATION_EXCLUDE_HOST.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_EXCLUDE.name(),
                    CrateSettings.ROUTING_ALLOCATION_EXCLUDE_NAME.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_REQUIRE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_REQUIRE.name(),
                    CrateSettings.ROUTING_ALLOCATION_REQUIRE_IP.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_REQUIRE.name(),
                    CrateSettings.ROUTING_ALLOCATION_REQUIRE_ID.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_REQUIRE.name(),
                    CrateSettings.ROUTING_ALLOCATION_REQUIRE_HOST.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_REQUIRE.name(),
                    CrateSettings.ROUTING_ALLOCATION_REQUIRE_NAME.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_BALANCE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.FLOAT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_BALANCE.name(),
                    CrateSettings.ROUTING_ALLOCATION_BALANCE_SHARD.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.FLOAT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_BALANCE.name(),
                    CrateSettings.ROUTING_ALLOCATION_BALANCE_INDEX.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.FLOAT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_BALANCE.name(),
                    CrateSettings.ROUTING_ALLOCATION_BALANCE_THRESHOLD.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_DISK.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_DISK.name(),
                    CrateSettings.ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_DISK.name(),
                    CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_DISK.name(),
                    CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK.name(),
                    CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_LOW.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.ROUTING.name(),
                    CrateSettings.ROUTING_ALLOCATION.name(),
                    CrateSettings.ROUTING_ALLOCATION_DISK.name(),
                    CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK.name(),
                    CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_HIGH.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_RECOVERY.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_RECOVERY.name(),
                    CrateSettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_RECOVERY.name(),
                    CrateSettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_RECOVERY.name(),
                    CrateSettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_RECOVERY.name(),
                    CrateSettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_RECOVERY.name(),
                    CrateSettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_RECOVERY.name(),
                    CrateSettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_STORE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_STORE.name(),
                    CrateSettings.INDICES_STORE_THROTTLE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_STORE.name(),
                    CrateSettings.INDICES_STORE_THROTTLE.name(),
                    CrateSettings.INDICES_STORE_THROTTLE_TYPE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_STORE.name(),
                    CrateSettings.INDICES_STORE_THROTTLE.name(),
                    CrateSettings.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_BREAKER.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_BREAKER.name(),
                    CrateSettings.INDICES_BREAKER_QUERY.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_BREAKER.name(),
                    CrateSettings.INDICES_BREAKER_QUERY.name(),
                    CrateSettings.INDICES_BREAKER_QUERY_LIMIT.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.DOUBLE, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_BREAKER.name(),
                    CrateSettings.INDICES_BREAKER_QUERY.name(),
                    CrateSettings.INDICES_BREAKER_QUERY_OVERHEAD.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_BREAKER.name(),
                    CrateSettings.INDICES_BREAKER_REQUEST.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_BREAKER.name(),
                    CrateSettings.INDICES_BREAKER_REQUEST.name(),
                    CrateSettings.INDICES_BREAKER_REQUEST_LIMIT.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.DOUBLE, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_BREAKER.name(),
                    CrateSettings.INDICES_BREAKER_REQUEST.name(),
                    CrateSettings.INDICES_BREAKER_REQUEST_OVERHEAD.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_BREAKER.name(),
                    CrateSettings.INDICES_BREAKER_FIELDDATA.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_BREAKER.name(),
                    CrateSettings.INDICES_BREAKER_FIELDDATA.name(),
                    CrateSettings.INDICES_BREAKER_FIELDDATA_LIMIT.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.DOUBLE, ImmutableList.of(CrateSettings.INDICES.name(),
                    CrateSettings.INDICES_BREAKER.name(),
                    CrateSettings.INDICES_BREAKER_FIELDDATA.name(),
                    CrateSettings.INDICES_BREAKER_FIELDDATA_OVERHEAD.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.CLUSTER_INFO.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.CLUSTER_INFO.name(),
                    CrateSettings.CLUSTER_INFO_UPDATE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.CLUSTER.name(),
                    CrateSettings.CLUSTER_INFO.name(),
                    CrateSettings.CLUSTER_INFO_UPDATE.name(),
                    CrateSettings.CLUSTER_INFO_UPDATE_INTERVAL.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.BULK.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.BULK.name(),
                    CrateSettings.BULK_REQUEST_TIMEOUT.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.GATEWAY.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.INTEGER, ImmutableList.of(CrateSettings.GATEWAY.name(),
                    CrateSettings.GATEWAY_EXPECTED_NODES.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.INTEGER, ImmutableList.of(CrateSettings.GATEWAY.name(),
                    CrateSettings.GATEWAY_RECOVERY_AFTER_NODES.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.GATEWAY.name(),
                    CrateSettings.GATEWAY_RECOVER_AFTER_TIME.name()))

                .register(ClusterSettingsExpression.NAME, new ArrayType(DataTypes.OBJECT), ImmutableList.of(
                    ClusterLoggingOverridesExpression.NAME))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(
                    ClusterLoggingOverridesExpression.NAME,
                    ClusterLoggingOverridesExpression.ClusterLoggingOverridesChildExpression.NAME))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(
                    ClusterLoggingOverridesExpression.NAME,
                    ClusterLoggingOverridesExpression.ClusterLoggingOverridesChildExpression.LEVEL))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.UDC.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.UDC.name(),
                    CrateSettings.UDC_ENABLED.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.UDC.name(),
                    CrateSettings.UDC_INITIAL_DELAY.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.UDC.name(),
                    CrateSettings.UDC_INTERVAL.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(CrateSettings.UDC.name(),
                    CrateSettings.UDC_URL.name()))

                .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, ImmutableList.of(CrateSettings.LICENSE.name()))
                .register(ClusterSettingsExpression.NAME, DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.LICENSE.name(),
                    CrateSettings.LICENSE_ENTERPRISE.name())),
            Collections.emptyList());
        this.clusterService = clusterService;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return Routing.forTableOnSingleNode(IDENT, clusterService.localNode().getId());
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.CLUSTER;
    }
}
