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

package io.crate.analyze;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.metadata.settings.CrateTableSettings;
import io.crate.metadata.table.ColumnPolicy;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.PrimaryShardAllocator;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

@Immutable
@ThreadSafe
public class TableParameterInfo {

    public static final TableParameterInfo INSTANCE = new TableParameterInfo();

    // all available table settings
    public static final String NUMBER_OF_REPLICAS = IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
    public static final String AUTO_EXPAND_REPLICAS = IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;
    public static final String REFRESH_INTERVAL = IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey();
    public static final String NUMBER_OF_SHARDS = IndexMetaData.SETTING_NUMBER_OF_SHARDS;
    public static final String READ_ONLY = IndexMetaData.SETTING_READ_ONLY;
    public static final String BLOCKS_READ = IndexMetaData.SETTING_BLOCKS_READ;
    public static final String BLOCKS_WRITE = IndexMetaData.SETTING_BLOCKS_WRITE;
    public static final String BLOCKS_METADATA = IndexMetaData.SETTING_BLOCKS_METADATA;
    public static final String SETTING_WAIT_FOR_ACTIVE_SHARDS = IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey();
    public static final String BLOBS_PATH = BlobIndicesService.SETTING_INDEX_BLOBS_PATH.getKey();
    public static final String FLUSH_THRESHOLD_SIZE = IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey();
    public static final String TRANSLOG_DURABILITY = IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey();
    public static final String TRANSLOG_SYNC_INTERVAL = IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey();
    public static final String ROUTING_ALLOCATION_ENABLE = EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey();
    public static final String TOTAL_SHARDS_PER_NODE = ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey();
    public static final String MAPPING_TOTAL_FIELDS_LIMIT = MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey();
    public static final String ALLOCATION_MAX_RETRIES = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey();

    @Deprecated
    public static final String RECOVERY_INITIAL_SHARDS = PrimaryShardAllocator.INDEX_RECOVERY_INITIAL_SHARDS_SETTING.getKey();
    public static final String WARMER_ENABLED = IndexSettings.INDEX_WARMER_ENABLED_SETTING.getKey();
    public static final String UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT = UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey();

    // all available table mapping keys
    public static final String COLUMN_POLICY = ColumnPolicy.ES_MAPPING_NAME;

    private static final ImmutableList<String> SUPPORTED_SETTINGS =
        ImmutableList.<String>builder()
            .add(NUMBER_OF_REPLICAS)
            .add(REFRESH_INTERVAL)
            .add(READ_ONLY)
            .add(BLOCKS_READ)
            .add(BLOCKS_WRITE)
            .add(BLOCKS_METADATA)
            .add(FLUSH_THRESHOLD_SIZE)
            .add(ROUTING_ALLOCATION_ENABLE)
            .add(TRANSLOG_SYNC_INTERVAL)
            .add(TRANSLOG_DURABILITY)
            .add(TOTAL_SHARDS_PER_NODE)
            .add(MAPPING_TOTAL_FIELDS_LIMIT)
            .add(RECOVERY_INITIAL_SHARDS)
            .add(WARMER_ENABLED)
            .add(UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT)
            .add(SETTING_WAIT_FOR_ACTIVE_SHARDS)
            .add(ALLOCATION_MAX_RETRIES)
            .build();

    private static final ImmutableList<String> SUPPORTED_INTERNAL_SETTINGS =
        ImmutableList.<String>builder()
            .addAll(SUPPORTED_SETTINGS)
            .add(AUTO_EXPAND_REPLICAS)
            .build();

    private static final ImmutableList<String> SUPPORTED_MAPPINGS =
        ImmutableList.<String>builder()
            .add(COLUMN_POLICY)
            .build();

    /**
     * Returns list of public settings names supported by this table
     */
    public ImmutableList<String> supportedSettings() {
        return SUPPORTED_SETTINGS;
    }

    /**
     * Returns list of internal settings names supported by this table
     */
    public ImmutableList<String> supportedInternalSettings() {
        return SUPPORTED_INTERNAL_SETTINGS;
    }

    /**
     * Returns a list of mapping names supported by this table
     */
    public ImmutableList<String> supportedMappings() {
        return SUPPORTED_MAPPINGS;
    }

    public static ImmutableMap<String, Object> tableParametersFromIndexMetaData(IndexMetaData metaData) {
        Settings settings = metaData.getSettings();
        return ImmutableMap.<String, Object>builder()
            .put(TableParameterInfo.READ_ONLY, CrateTableSettings.READ_ONLY.extract(settings))
            .put(TableParameterInfo.BLOCKS_READ, CrateTableSettings.BLOCKS_READ.extract(settings))
            .put(TableParameterInfo.BLOCKS_WRITE, CrateTableSettings.BLOCKS_WRITE.extract(settings))
            .put(TableParameterInfo.BLOCKS_METADATA, CrateTableSettings.BLOCKS_METADATA.extract(settings))
            .put(TableParameterInfo.FLUSH_THRESHOLD_SIZE, CrateTableSettings.FLUSH_THRESHOLD_SIZE.extractBytes(settings))
            .put(TableParameterInfo.ROUTING_ALLOCATION_ENABLE, CrateTableSettings.ROUTING_ALLOCATION_ENABLE.extract(settings))
            .put(TableParameterInfo.TOTAL_SHARDS_PER_NODE, CrateTableSettings.TOTAL_SHARDS_PER_NODE.extract(settings))
            .put(TableParameterInfo.MAPPING_TOTAL_FIELDS_LIMIT, CrateTableSettings.TOTAL_FIELDS_LIMIT.extract(settings))
            .put(TableParameterInfo.RECOVERY_INITIAL_SHARDS, CrateTableSettings.RECOVERY_INITIAL_SHARDS.extract(settings))
            .put(TableParameterInfo.WARMER_ENABLED, CrateTableSettings.WARMER_ENABLED.extract(settings))
            .put(TableParameterInfo.TRANSLOG_SYNC_INTERVAL, CrateTableSettings.TRANSLOG_SYNC_INTERVAL.extractMillis(settings))
            .put(TableParameterInfo.TRANSLOG_DURABILITY, CrateTableSettings.TRANSLOG_DURABILITY.extract(settings))
            .put(TableParameterInfo.REFRESH_INTERVAL, CrateTableSettings.REFRESH_INTERVAL.extractMillis(settings))
            .put(TableParameterInfo.SETTING_WAIT_FOR_ACTIVE_SHARDS, CrateTableSettings.SETTING_WAIT_FOR_ACTIVE_SHARDS.extract(settings))
            .put(TableParameterInfo.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, CrateTableSettings.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT.extractMillis(settings))
            .put(TableParameterInfo.ALLOCATION_MAX_RETRIES, CrateTableSettings.ALLOCATION_MAX_RETRIES.extract(settings))
            .build();
    }

    protected TableParameterInfo() {
    }
}
