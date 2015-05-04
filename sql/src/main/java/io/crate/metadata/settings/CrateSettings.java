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

package io.crate.metadata.settings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.crate.analyze.SettingsApplier;
import io.crate.analyze.SettingsAppliers;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CrateSettings {

    public static final NestedSetting STATS = new NestedSetting() {
        @Override
        public String name() {
            return "stats";
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(STATS_ENABLED, STATS_JOBS_LOG_SIZE, STATS_OPERATIONS_LOG_SIZE);
        }
    };

    public static final BoolSetting STATS_ENABLED = new BoolSetting() {
        @Override
        public String name() {
            return "enabled";
        }

        @Override
        public Boolean defaultValue() {
            return false;
        }

        @Override
        public Setting parent() {
            return STATS;
        }
    };

    public static final IntSetting STATS_JOBS_LOG_SIZE = new IntSetting() {
        @Override
        public String name() {
            return "jobs_log_size";
        }

        @Override
        public Integer defaultValue() {
            return 10_000;
        }

        @Override
        public Integer minValue() {
            return 0;
        }

        @Override
        public Setting parent() {
            return STATS;
        }
    };

    public static final IntSetting STATS_OPERATIONS_LOG_SIZE = new IntSetting() {
        @Override
        public String name() {
            return "operations_log_size";
        }

        @Override
        public Integer defaultValue() {
            return 10_000;
        }

        @Override
        public Integer minValue() {
            return 0;
        }

        @Override
        public Setting parent() {
            return STATS;
        }
    };

    public static final NestedSetting CLUSTER = new NestedSetting() {
        @Override
        public String name() {
            return "cluster";
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(GRACEFUL_STOP, ROUTING, CLUSTER_INFO);
        }
    };

    public static final NestedSetting GRACEFUL_STOP = new NestedSetting() {

        @Override
        public String name() { return "graceful_stop"; }

        @Override
        public Setting parent() {
            return CLUSTER;
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    GRACEFUL_STOP_MIN_AVAILABILITY,
                    GRACEFUL_STOP_REALLOCATE,
                    GRACEFUL_STOP_TIMEOUT,
                    GRACEFUL_STOP_FORCE);
        }
    };

    public static final StringSetting GRACEFUL_STOP_MIN_AVAILABILITY = new StringSetting(
            Sets.newHashSet("full", "primaries", "none")
    ) {
        @Override
        public String name() { return "min_availability"; }

        @Override
        public String defaultValue() { return "primaries"; }

        @Override
        public Setting parent() {
            return GRACEFUL_STOP;
        }
    };

    public static final BoolSetting GRACEFUL_STOP_REALLOCATE = new BoolSetting() {
        @Override
        public String name() { return "reallocate"; }

        @Override
        public Boolean defaultValue() {
            return true;
        }

        @Override
        public Setting parent() {
            return GRACEFUL_STOP;
        }
    };

    public static final TimeSetting GRACEFUL_STOP_TIMEOUT = new TimeSetting() {
        @Override
        public String name() {
            return "timeout";
        }

        @Override
        public TimeValue defaultValue() {
            return new TimeValue(7_200_000);
        }

        @Override
        public Setting parent() {
            return GRACEFUL_STOP;
        }
    };

    public static final BoolSetting GRACEFUL_STOP_FORCE = new BoolSetting() {
        @Override
        public String name() {
            return "force";
        }

        @Override
        public Boolean defaultValue() {
            return false;
        }

        @Override
        public Setting parent() {
            return GRACEFUL_STOP;
        }
    };

    public static final NestedSetting DISCOVERY = new NestedSetting() {
        @Override
        public String name() {
            return "discovery";
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(DISCOVERY_ZEN);
        }
    };

    public static final NestedSetting DISCOVERY_ZEN = new NestedSetting() {
        @Override
        public String name() { return "zen"; }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    DISCOVERY_ZEN_MIN_MASTER_NODES,
                    DISCOVERY_ZEN_PING_TIMEOUT,
                    DISCOVERY_ZEN_PUBLISH_TIMEOUT
            );
        }

        @Override
        public Setting parent() {
            return DISCOVERY;
        }
    };

    public static final IntSetting DISCOVERY_ZEN_MIN_MASTER_NODES = new IntSetting() {
        @Override
        public String name() {
            return "minimum_master_nodes";
        }

        @Override
        public Integer defaultValue() {
            return 1;
        }

        @Override
        public Setting parent() {
            return DISCOVERY_ZEN;
        }
    };

    public static final TimeSetting DISCOVERY_ZEN_PING_TIMEOUT = new TimeSetting() {
        @Override
        public String name() {
            return "ping_timeout";
        }

        @Override
        public TimeValue defaultValue() {
            return new TimeValue(3, TimeUnit.SECONDS);
        }

        @Override
        public Setting parent() {
            return DISCOVERY_ZEN;
        }
    };

    public static final TimeSetting DISCOVERY_ZEN_PUBLISH_TIMEOUT = new TimeSetting() {
        @Override
        public String name() {
            return "publish_timeout";
        }

        @Override
        public TimeValue defaultValue() {
            return new TimeValue(30, TimeUnit.SECONDS);
        }

        @Override
        public Setting parent() {
            return DISCOVERY_ZEN;
        }
    };

    public static final NestedSetting ROUTING = new NestedSetting() {
        @Override
        public String name() { return "routing"; }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(ROUTING_ALLOCATION);
        }

        @Override
        public Setting parent() {
            return CLUSTER;
        }
    };

    public static final NestedSetting ROUTING_ALLOCATION = new NestedSetting() {
        @Override
        public String name() { return "allocation"; }

        @Override
        public Setting parent() {
            return ROUTING;
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    ROUTING_ALLOCATION_ENABLE,
                    ROUTING_ALLOCATION_ALLOW_REBALANCE,
                    ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE,
                    ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES,
                    ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES,
                    ROUTING_ALLOCATION_INCLUDE,
                    ROUTING_ALLOCATION_EXCLUDE,
                    ROUTING_ALLOCATION_REQUIRE,
                    ROUTING_ALLOCATION_BALANCE,
                    ROUTING_ALLOCATION_DISK
            );
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_ENABLE = new StringSetting(
            Sets.newHashSet("none", "primaries", "all", "new_primaries")
    ) {
        @Override
        public String name() { return "enable"; }

        @Override
        public String defaultValue() { return "all"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_ALLOW_REBALANCE = new StringSetting(
            Sets.newHashSet("always", "indices_primary_active", "indices_all_active")
    ) {
        @Override
        public String name() { return "allow_rebalance"; }

        @Override
        public String defaultValue() { return "indices_all_active"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION;
        }
    };

    public static final IntSetting ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE = new IntSetting() {
        @Override
        public String name() { return "cluster_concurrent_rebalance"; }

        @Override
        public Integer defaultValue() { return 2; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION;
        }
    };

    public static final IntSetting ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES = new IntSetting() {
        @Override
        public String name() { return "node_initial_primaries_recoveries"; }

        @Override
        public Integer defaultValue() { return 4; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION;
        }
    };

    public static final IntSetting ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES = new IntSetting() {
        @Override
        public String name() { return "node_concurrent_recoveries"; }

        @Override
        public Integer defaultValue() { return 2; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION;
        }
    };

    public static final NestedSetting ROUTING_ALLOCATION_INCLUDE = new NestedSetting() {
        @Override
        public String name() { return "include"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_INCLUDE_IP = new StringSetting() {
        @Override
        public String name() { return "_ip"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_INCLUDE;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_INCLUDE_ID = new StringSetting() {
        @Override
        public String name() { return "_id"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_INCLUDE;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_INCLUDE_HOST = new StringSetting() {
        @Override
        public String name() { return "_host"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_INCLUDE;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_INCLUDE_NAME = new StringSetting() {
        @Override
        public String name() { return "_name"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_INCLUDE;
        }
    };

    public static final NestedSetting ROUTING_ALLOCATION_EXCLUDE = new NestedSetting() {
        @Override
        public String name() { return "exclude"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_EXCLUDE_IP = new StringSetting() {
        @Override
        public String name() { return "_ip"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_EXCLUDE;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_EXCLUDE_ID = new StringSetting() {
        @Override
        public String name() { return "_id"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_EXCLUDE;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_EXCLUDE_HOST = new StringSetting() {
        @Override
        public String name() { return "_host"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_EXCLUDE;
        }
    };


    public static final StringSetting ROUTING_ALLOCATION_EXCLUDE_NAME = new StringSetting() {
        @Override
        public String name() { return "_name"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_EXCLUDE;
        }
    };

    public static final NestedSetting ROUTING_ALLOCATION_REQUIRE = new NestedSetting() {
        @Override
        public String name() { return "require"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_REQUIRE_IP = new StringSetting() {
        @Override
        public String name() { return "_ip"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_REQUIRE;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_REQUIRE_ID = new StringSetting() {
        @Override
        public String name() { return "_id"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_REQUIRE;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_REQUIRE_HOST = new StringSetting() {
        @Override
        public String name() { return "_host"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_REQUIRE;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_REQUIRE_NAME = new StringSetting() {
        @Override
        public String name() { return "_name"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_REQUIRE;
        }
    };

    public static final NestedSetting ROUTING_ALLOCATION_BALANCE = new NestedSetting() {
        @Override
        public String name() { return "balance"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION;
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    ROUTING_ALLOCATION_BALANCE_SHARD,
                    ROUTING_ALLOCATION_BALANCE_INDEX,
                    ROUTING_ALLOCATION_BALANCE_PRIMARY,
                    ROUTING_ALLOCATION_BALANCE_THRESHOLD
            );
        }
    };

    public static final FloatSetting ROUTING_ALLOCATION_BALANCE_SHARD = new FloatSetting() {
        @Override
        public String name() { return "shard"; }

        @Override
        public Float defaultValue() { return 0.45f; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_BALANCE;
        }
    };

    public static final FloatSetting ROUTING_ALLOCATION_BALANCE_INDEX = new FloatSetting() {
        @Override
        public String name() { return "index"; }

        @Override
        public Float defaultValue() { return 0.5f; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_BALANCE;
        }
    };

    public static final FloatSetting ROUTING_ALLOCATION_BALANCE_PRIMARY = new FloatSetting() {
        @Override
        public String name() { return "primary"; }

        @Override
        public Float defaultValue() { return 0.05f; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_BALANCE;
        }
    };

    public static final FloatSetting ROUTING_ALLOCATION_BALANCE_THRESHOLD = new FloatSetting() {
        @Override
        public String name() { return "threshold"; }

        @Override
        public Float defaultValue() { return 1.0f; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_BALANCE;
        }
    };

    public static final NestedSetting ROUTING_ALLOCATION_DISK = new NestedSetting() {
        @Override
        public String name() { return "disk"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION;
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED,
                    ROUTING_ALLOCATION_DISK_WATERMARK
            );
        }
    };

    public static final BoolSetting ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED = new BoolSetting() {
        @Override
        public String name() { return "threshold_enabled"; }

        @Override
        public Boolean defaultValue() { return true; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_DISK;
        }
    };

    public static final NestedSetting ROUTING_ALLOCATION_DISK_WATERMARK = new NestedSetting() {
        @Override
        public String name() { return "watermark"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_DISK;
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    ROUTING_ALLOCATION_DISK_WATERMARK_LOW,
                    ROUTING_ALLOCATION_DISK_WATERMARK_HIGH
            );
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_DISK_WATERMARK_LOW = new StringSetting() {
        @Override
        public String name() { return "low"; }

        @Override
        public String defaultValue() { return "85%"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_DISK_WATERMARK;
        }
    };

    public static final StringSetting ROUTING_ALLOCATION_DISK_WATERMARK_HIGH = new StringSetting() {
        @Override
        public String name() { return "high"; }

        @Override
        public String defaultValue() { return "90%"; }

        @Override
        public Setting parent() {
            return ROUTING_ALLOCATION_DISK_WATERMARK;
        }
    };

    public static final NestedSetting INDICES = new NestedSetting() {
        @Override
        public String name() {
            return "indices";
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(INDICES_RECOVERY, INDICES_STORE, INDICES_FIELDDATA);
        }
    };

    public static final NestedSetting INDICES_RECOVERY = new NestedSetting() {
        @Override
        public String name() { return "recovery"; }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    INDICES_RECOVERY_CONCURRENT_STREAMS,
                    INDICES_RECOVERY_FILE_CHUNK_SIZE,
                    INDICES_RECOVERY_TRANSLOG_OPS,
                    INDICES_RECOVERY_TRANSLOG_SIZE,
                    INDICES_RECOVERY_COMPRESS,
                    INDICES_RECOVERY_MAX_BYTES_PER_SEC,
                    INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC,
                    INDICES_RECOVERY_RETRY_DELAY_NETWORK,
                    INDICES_RECOVERY_ACTIVITY_TIMEOUT,
                    INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT,
                    INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT
            );
        }

        @Override
        public Setting parent() {
            return INDICES;
        }
    };

    public static final IntSetting INDICES_RECOVERY_CONCURRENT_STREAMS = new IntSetting() {
        @Override
        public String name() { return "concurrent_streams"; }

        @Override
        public Integer defaultValue() { return 3; }

        @Override
        public Setting parent() {
            return INDICES_RECOVERY;
        }
    };

    public static final ByteSizeSetting INDICES_RECOVERY_FILE_CHUNK_SIZE = new ByteSizeSetting() {
        @Override
        public String name() { return "file_chunk_size"; }

        @Override
        public ByteSizeValue defaultValue() { return new ByteSizeValue(512, ByteSizeUnit.KB); }

        @Override
        public Setting parent() {
            return INDICES_RECOVERY;
        }
    };

    public static final IntSetting INDICES_RECOVERY_TRANSLOG_OPS = new IntSetting() {
        @Override
        public String name() { return "translog_ops"; }

        @Override
        public Integer defaultValue() { return 1000; }

        @Override
        public Setting parent() {
            return INDICES_RECOVERY;
        }
    };

    public static final ByteSizeSetting INDICES_RECOVERY_TRANSLOG_SIZE = new ByteSizeSetting() {
        @Override
        public String name() { return "translog_size"; }

        @Override
        public ByteSizeValue defaultValue() { return new ByteSizeValue(512, ByteSizeUnit.KB); }

        @Override
        public Setting parent() {
            return INDICES_RECOVERY;
        }
    };

    public static final BoolSetting INDICES_RECOVERY_COMPRESS = new BoolSetting() {
        @Override
        public String name() { return "compress"; }

        @Override
        public Boolean defaultValue() { return true; }

        @Override
        public Setting parent() {
            return INDICES_RECOVERY;
        }
    };

    public static final ByteSizeSetting INDICES_RECOVERY_MAX_BYTES_PER_SEC = new ByteSizeSetting() {
        @Override
        public String name() { return "max_bytes_per_sec"; }

        @Override
        public ByteSizeValue defaultValue() { return new ByteSizeValue(40, ByteSizeUnit.MB); }

        @Override
        public Setting parent() {
            return INDICES_RECOVERY;
        }
    };

    public static final TimeSetting INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC = new TimeSetting() {
        @Override
        public String name() { return "retry_delay_state_sync"; }

        @Override
        public TimeValue defaultValue() { return TimeValue.timeValueMillis(500); }

        @Override
        public Setting parent() {
            return INDICES_RECOVERY;
        }
    };

    public static final TimeSetting INDICES_RECOVERY_RETRY_DELAY_NETWORK = new TimeSetting() {
        @Override
        public String name() { return "retry_delay_network"; }

        @Override
        public TimeValue defaultValue() { return TimeValue.timeValueSeconds(5); }

        @Override
        public Setting parent() {
            return INDICES_RECOVERY;
        }
    };

    public static final TimeSetting INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT = new TimeSetting() {
        @Override
        public String name() { return "internal_action_timeout"; }

        @Override
        public TimeValue defaultValue() { return TimeValue.timeValueMinutes(15); }

        @Override
        public Setting parent() {
            return INDICES_RECOVERY;
        }
    };

    public static final TimeSetting INDICES_RECOVERY_ACTIVITY_TIMEOUT = new TimeSetting() {
        @Override
        public String name() { return "activity_timeout"; }

        @Override
        public TimeValue defaultValue() { return INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT.defaultValue(); }

        @Override
        public Setting parent() {
            return INDICES_RECOVERY;
        }
    };

    public static final TimeSetting INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT = new TimeSetting() {
        @Override
        public String name() { return "internal_action_long_timeout"; }

        @Override
        public TimeValue defaultValue() { return new TimeValue(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT.defaultValue().millis() * 2); }

        @Override
        public Setting parent() {
            return INDICES_RECOVERY;
        }
    };

    public static final NestedSetting INDICES_STORE = new NestedSetting() {
        @Override
        public String name() { return "store"; }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(INDICES_STORE_THROTTLE);
        }

        @Override
        public Setting parent() {
            return INDICES;
        }
    };

    public static final NestedSetting INDICES_STORE_THROTTLE = new NestedSetting() {
        @Override
        public String name() { return "throttle"; }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    INDICES_STORE_THROTTLE_TYPE,
                    INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC
            );
        }

        @Override
        public Setting parent() {
            return INDICES_STORE;
        }
    };

    public static final StringSetting INDICES_STORE_THROTTLE_TYPE = new StringSetting(
            Sets.newHashSet("all", "merge", "none")
    ) {
        @Override
        public String name() { return "type"; }

        @Override
        public String defaultValue() { return "merge"; }

        @Override
        public Setting parent() {
            return INDICES_STORE_THROTTLE;
        }
    };

    public static final ByteSizeSetting INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC = new ByteSizeSetting() {
        @Override
        public String name() { return "max_bytes_per_sec"; }

        @Override
        public ByteSizeValue defaultValue() { return new ByteSizeValue(20, ByteSizeUnit.MB); }

        @Override
        public Setting parent() {
            return INDICES_STORE_THROTTLE;
        }
    };

    public static final NestedSetting INDICES_FIELDDATA = new NestedSetting() {
        @Override
        public String name() { return "fielddata"; }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(INDICES_FIELDDATA_BREAKER);
        }

        @Override
        public Setting parent() {
            return INDICES;
        }
    };

    public static final NestedSetting INDICES_FIELDDATA_BREAKER = new NestedSetting() {
        @Override
        public String name() { return "breaker"; }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    INDICES_FIELDDATA_BREAKER_LIMIT,
                    INDICES_FIELDDATA_BREAKER_OVERHEAD
            );
        }

        @Override
        public Setting parent() {
            return INDICES_FIELDDATA;
        }
    };

    public static final StringSetting INDICES_FIELDDATA_BREAKER_LIMIT = new StringSetting() {
        @Override
        public String name() { return "limit"; }

        @Override
        public String defaultValue() { return "60%"; }

        @Override
        public Setting parent() {
            return INDICES_FIELDDATA_BREAKER;
        }
    };

    public static final DoubleSetting INDICES_FIELDDATA_BREAKER_OVERHEAD = new DoubleSetting() {
        @Override
        public String name() { return "overhead"; }

        @Override
        public Double defaultValue() { return 1.03; }

        @Override
        public Setting parent() {
            return INDICES_FIELDDATA_BREAKER;
        }
    };

    public static final NestedSetting CLUSTER_INFO = new NestedSetting() {
        @Override
        public String name() { return "info"; }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(CLUSTER_INFO_UPDATE);
        }

        @Override
        public Setting parent() {
            return CLUSTER;
        }
    };

    public static final NestedSetting CLUSTER_INFO_UPDATE = new NestedSetting() {
        @Override
        public String name() { return "update"; }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(
                    CLUSTER_INFO_UPDATE_INTERVAL
            );
        }

        @Override
        public Setting parent() {
            return CLUSTER_INFO;
        }
    };

    public static final TimeSetting CLUSTER_INFO_UPDATE_INTERVAL = new TimeSetting() {
        @Override
        public String name() {
            return "interval";
        }

        @Override
        public TimeValue defaultValue() {
            return new TimeValue(30, TimeUnit.SECONDS);

        }

        @Override
        public Setting parent() {
            return CLUSTER_INFO_UPDATE;
        }
    };

    public static final NestedSetting BULK = new NestedSetting() {
        @Override
        public String name() {
            return "bulk";
        }

        @Override
        public List<Setting> children() {
            return ImmutableList.<Setting>of(BULK_PARTITION_CREATION_TIMEOUT, BULK_REQUEST_TIMEOUT);
        }
    };

    public static final TimeSetting BULK_REQUEST_TIMEOUT = new TimeSetting() {
        @Override
        public String name() {
            return "request_timeout";
        }

        @Override
        public TimeValue defaultValue() {
            return new TimeValue(1, TimeUnit.MINUTES);
        }

        @Override
        public Setting parent() {
            return BULK;
        }
    };

    public static final TimeSetting BULK_PARTITION_CREATION_TIMEOUT = new TimeSetting() {
        @Override
        public String name() {
            return "partition_creation_timeout";
        }

        @Override
        public TimeValue defaultValue() {
            return new TimeValue(0, TimeUnit.SECONDS);
        }

        @Override
        public Setting parent() {
            return BULK;
        }
    };


    public static final ImmutableList<Setting> CRATE_SETTINGS = ImmutableList.<Setting>of(STATS, CLUSTER, DISCOVERY, INDICES, BULK);

    public static final Map<String, SettingsApplier> SUPPORTED_SETTINGS = ImmutableMap.<String, SettingsApplier>builder()
            .put(CrateSettings.STATS.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.STATS))
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(),
                    new SettingsAppliers.IntSettingsApplier(CrateSettings.STATS_JOBS_LOG_SIZE))
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(),
                    new SettingsAppliers.IntSettingsApplier(CrateSettings.STATS_OPERATIONS_LOG_SIZE))
            .put(CrateSettings.STATS_ENABLED.settingName(),
                    new SettingsAppliers.BooleanSettingsApplier(CrateSettings.STATS_ENABLED))
            .put(CrateSettings.CLUSTER.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.CLUSTER))
            .put(CrateSettings.GRACEFUL_STOP.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.GRACEFUL_STOP))
            .put(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY))
            .put(CrateSettings.GRACEFUL_STOP_REALLOCATE.settingName(),
                    new SettingsAppliers.BooleanSettingsApplier(CrateSettings.GRACEFUL_STOP_REALLOCATE))
            .put(CrateSettings.GRACEFUL_STOP_FORCE.settingName(),
                    new SettingsAppliers.BooleanSettingsApplier(CrateSettings.GRACEFUL_STOP_FORCE))
            .put(CrateSettings.GRACEFUL_STOP_TIMEOUT.settingName(),
                    new SettingsAppliers.TimeSettingsApplier(CrateSettings.GRACEFUL_STOP_TIMEOUT))
            .put(CrateSettings.DISCOVERY.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.DISCOVERY))
            .put(CrateSettings.DISCOVERY_ZEN.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.DISCOVERY_ZEN))
            .put(CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES.settingName(),
                    new SettingsAppliers.IntSettingsApplier(CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES))
            .put(CrateSettings.DISCOVERY_ZEN_PING_TIMEOUT.settingName(),
                    new SettingsAppliers.TimeSettingsApplier(CrateSettings.DISCOVERY_ZEN_PING_TIMEOUT))
            .put(CrateSettings.DISCOVERY_ZEN_PUBLISH_TIMEOUT.settingName(),
                    new SettingsAppliers.TimeSettingsApplier(CrateSettings.DISCOVERY_ZEN_PUBLISH_TIMEOUT))
            .put(CrateSettings.ROUTING.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING))
            .put(CrateSettings.ROUTING_ALLOCATION.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION))
            .put(CrateSettings.ROUTING_ALLOCATION_ENABLE.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_ENABLE))
            .put(CrateSettings.ROUTING_ALLOCATION_ALLOW_REBALANCE.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_ALLOW_REBALANCE))
            .put(CrateSettings.ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE.settingName(),
                    new SettingsAppliers.IntSettingsApplier(CrateSettings.ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE))
            .put(CrateSettings.ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES.settingName(),
                    new SettingsAppliers.IntSettingsApplier(CrateSettings.ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES))
            .put(CrateSettings.ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES.settingName(),
                    new SettingsAppliers.IntSettingsApplier(CrateSettings.ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES))
            .put(CrateSettings.ROUTING_ALLOCATION_INCLUDE.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION_INCLUDE))
            .put(CrateSettings.ROUTING_ALLOCATION_INCLUDE_IP.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_INCLUDE_IP))
            .put(CrateSettings.ROUTING_ALLOCATION_INCLUDE_ID.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_INCLUDE_ID))
            .put(CrateSettings.ROUTING_ALLOCATION_INCLUDE_HOST.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_INCLUDE_HOST))
            .put(CrateSettings.ROUTING_ALLOCATION_INCLUDE_NAME.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_INCLUDE_NAME))
            .put(CrateSettings.ROUTING_ALLOCATION_EXCLUDE.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION_EXCLUDE))
            .put(CrateSettings.ROUTING_ALLOCATION_EXCLUDE_IP.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_EXCLUDE_IP))
            .put(CrateSettings.ROUTING_ALLOCATION_EXCLUDE_ID.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_EXCLUDE_ID))
            .put(CrateSettings.ROUTING_ALLOCATION_EXCLUDE_HOST.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_EXCLUDE_HOST))
            .put(CrateSettings.ROUTING_ALLOCATION_EXCLUDE_NAME.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_EXCLUDE_NAME))
            .put(CrateSettings.ROUTING_ALLOCATION_REQUIRE.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION_REQUIRE))
            .put(CrateSettings.ROUTING_ALLOCATION_REQUIRE_IP.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_REQUIRE_IP))
            .put(CrateSettings.ROUTING_ALLOCATION_REQUIRE_ID.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_REQUIRE_ID))
            .put(CrateSettings.ROUTING_ALLOCATION_REQUIRE_HOST.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_REQUIRE_HOST))
            .put(CrateSettings.ROUTING_ALLOCATION_REQUIRE_NAME.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_REQUIRE_NAME))
            .put(CrateSettings.ROUTING_ALLOCATION_BALANCE.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION_BALANCE))
            .put(CrateSettings.ROUTING_ALLOCATION_BALANCE_SHARD.settingName(),
                    new SettingsAppliers.FloatSettingsApplier(CrateSettings.ROUTING_ALLOCATION_BALANCE_SHARD))
            .put(CrateSettings.ROUTING_ALLOCATION_BALANCE_INDEX.settingName(),
                    new SettingsAppliers.FloatSettingsApplier(CrateSettings.ROUTING_ALLOCATION_BALANCE_INDEX))
            .put(CrateSettings.ROUTING_ALLOCATION_BALANCE_PRIMARY.settingName(),
                    new SettingsAppliers.FloatSettingsApplier(CrateSettings.ROUTING_ALLOCATION_BALANCE_PRIMARY))
            .put(CrateSettings.ROUTING_ALLOCATION_BALANCE_THRESHOLD.settingName(),
                    new SettingsAppliers.FloatSettingsApplier(CrateSettings.ROUTING_ALLOCATION_BALANCE_THRESHOLD))
            .put(CrateSettings.ROUTING_ALLOCATION_DISK.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION_DISK))
            .put(CrateSettings.ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED.settingName(),
                    new SettingsAppliers.BooleanSettingsApplier(CrateSettings.ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED))
            .put(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK))
            .put(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_LOW.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_LOW))
            .put(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_HIGH.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_HIGH))
            .put(CrateSettings.INDICES.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES))
            .put(CrateSettings.INDICES_RECOVERY.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES_RECOVERY))
            .put(CrateSettings.INDICES_RECOVERY_CONCURRENT_STREAMS.settingName(),
                    new SettingsAppliers.IntSettingsApplier(CrateSettings.INDICES_RECOVERY_CONCURRENT_STREAMS))
            .put(CrateSettings.INDICES_RECOVERY_FILE_CHUNK_SIZE.settingName(),
                    new SettingsAppliers.ByteSizeSettingsApplier(CrateSettings.INDICES_RECOVERY_FILE_CHUNK_SIZE))
            .put(CrateSettings.INDICES_RECOVERY_TRANSLOG_OPS.settingName(),
                    new SettingsAppliers.IntSettingsApplier(CrateSettings.INDICES_RECOVERY_TRANSLOG_OPS))
            .put(CrateSettings.INDICES_RECOVERY_TRANSLOG_SIZE.settingName(),
                    new SettingsAppliers.ByteSizeSettingsApplier(CrateSettings.INDICES_RECOVERY_TRANSLOG_SIZE))
            .put(CrateSettings.INDICES_RECOVERY_COMPRESS.settingName(),
                    new SettingsAppliers.BooleanSettingsApplier(CrateSettings.INDICES_RECOVERY_COMPRESS))
            .put(CrateSettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC.settingName(),
                    new SettingsAppliers.ByteSizeSettingsApplier(CrateSettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC))
            .put(CrateSettings.INDICES_STORE.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES_STORE))
            .put(CrateSettings.INDICES_STORE_THROTTLE.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES_STORE_THROTTLE))
            .put(CrateSettings.INDICES_STORE_THROTTLE_TYPE.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.INDICES_STORE_THROTTLE_TYPE))
            .put(CrateSettings.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC.settingName(),
                    new SettingsAppliers.ByteSizeSettingsApplier(CrateSettings.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC))
            .put(CrateSettings.INDICES_FIELDDATA.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES_FIELDDATA))
            .put(CrateSettings.INDICES_FIELDDATA_BREAKER.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES_FIELDDATA_BREAKER))
            .put(CrateSettings.INDICES_FIELDDATA_BREAKER_LIMIT.settingName(),
                    new SettingsAppliers.StringSettingsApplier(CrateSettings.INDICES_FIELDDATA_BREAKER_LIMIT))
            .put(CrateSettings.INDICES_FIELDDATA_BREAKER_OVERHEAD.settingName(),
                    new SettingsAppliers.DoubleSettingsApplier(CrateSettings.INDICES_FIELDDATA_BREAKER_OVERHEAD))
            .put(CrateSettings.CLUSTER_INFO.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.CLUSTER_INFO))
            .put(CrateSettings.CLUSTER_INFO_UPDATE.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.CLUSTER_INFO_UPDATE))
            .put(CrateSettings.CLUSTER_INFO_UPDATE_INTERVAL.settingName(),
                    new SettingsAppliers.TimeSettingsApplier(CrateSettings.CLUSTER_INFO_UPDATE_INTERVAL))
            .put(CrateSettings.BULK.settingName(),
                    new SettingsAppliers.ObjectSettingsApplier(CrateSettings.BULK))
            .put(CrateSettings.BULK_REQUEST_TIMEOUT.settingName(),
                    new SettingsAppliers.TimeSettingsApplier(CrateSettings.BULK_REQUEST_TIMEOUT))
            .put(CrateSettings.BULK_PARTITION_CREATION_TIMEOUT.settingName(),
                    new SettingsAppliers.TimeSettingsApplier(CrateSettings.BULK_PARTITION_CREATION_TIMEOUT))
            .build();

    @Nullable
    public static SettingsApplier getSetting(String name) {
        return SUPPORTED_SETTINGS.get(name);
    }

    public static Set<String> settingNamesByPrefix(String prefix) {
        Set<String> settingNames = Sets.newHashSet();
        SettingsApplier settingsApplier = SUPPORTED_SETTINGS.get(prefix);
        if (settingsApplier != null
                && !(settingsApplier instanceof SettingsAppliers.ObjectSettingsApplier)) {
            settingNames.add(prefix);
        } else {
            prefix += ".";
            for (String name : SUPPORTED_SETTINGS.keySet()) {
                if (name.startsWith(prefix)) {
                    settingNames.add(name);
                }
            }
        }
        return settingNames;
    }
}
