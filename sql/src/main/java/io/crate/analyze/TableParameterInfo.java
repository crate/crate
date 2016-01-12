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
import io.crate.blob.v2.BlobIndices;
import io.crate.metadata.settings.CrateTableSettings;
import io.crate.metadata.table.ColumnPolicy;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.PrimaryShardAllocator;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.TranslogService;
import org.elasticsearch.indices.IndicesWarmer;

public class TableParameterInfo {

    // all available table settings
    public static final String NUMBER_OF_REPLICAS = IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
    public static final String AUTO_EXPAND_REPLICAS = IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;
    public static final String REFRESH_INTERVAL = IndexShard.INDEX_REFRESH_INTERVAL;
    public static final String NUMBER_OF_SHARDS = IndexMetaData.SETTING_NUMBER_OF_SHARDS;
    public static final String READ_ONLY = IndexMetaData.SETTING_READ_ONLY;
    public static final String BLOCKS_READ = IndexMetaData.SETTING_BLOCKS_READ;
    public static final String BLOCKS_WRITE = IndexMetaData.SETTING_BLOCKS_WRITE;
    public static final String BLOCKS_METADATA = IndexMetaData.SETTING_BLOCKS_METADATA;
    public static final String BLOBS_PATH = BlobIndices.SETTING_INDEX_BLOBS_PATH;
    public static final String FLUSH_THRESHOLD_OPS = TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_OPS;
    public static final String FLUSH_THRESHOLD_SIZE = TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE;
    public static final String FLUSH_THRESHOLD_PERIOD = TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_PERIOD;
    public static final String FLUSH_DISABLE = TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH;
    public static final String TRANSLOG_INTERVAL = TranslogService.INDEX_TRANSLOG_FLUSH_INTERVAL;
    public static final String GATEWAY_LOCAL_SYNC = "index.gateway.local.sync";
    public static final String ROUTING_ALLOCATION_ENABLE = EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE;
    public static final String TOTAL_SHARDS_PER_NODE = ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE;
    public static final String RECOVERY_INITIAL_SHARDS = PrimaryShardAllocator.INDEX_RECOVERY_INITIAL_SHARDS;
    public static final String WARMER_ENABLED = IndicesWarmer.INDEX_WARMER_ENABLED;
    public static final String UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT = UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;

    // all available table mapping keys
    public static final String COLUMN_POLICY = ColumnPolicy.ES_MAPPING_NAME;

    protected static final ImmutableList<String> SUPPORTED_SETTINGS =
            ImmutableList.<String>builder()
                    .add(NUMBER_OF_REPLICAS)
                    .add(REFRESH_INTERVAL)
                    .add(READ_ONLY)
                    .add(BLOCKS_READ)
                    .add(BLOCKS_WRITE)
                    .add(BLOCKS_METADATA)
                    .add(FLUSH_THRESHOLD_OPS)
                    .add(FLUSH_THRESHOLD_SIZE)
                    .add(FLUSH_THRESHOLD_PERIOD)
                    .add(FLUSH_DISABLE)
                    .add(TRANSLOG_INTERVAL)
                    .add(ROUTING_ALLOCATION_ENABLE)
                    .add(GATEWAY_LOCAL_SYNC)
                    .add(TOTAL_SHARDS_PER_NODE)
                    .add(RECOVERY_INITIAL_SHARDS)
                    .add(WARMER_ENABLED)
                    .add(UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT)
                    .build();

    protected static final ImmutableList<String> SUPPORTED_INTERNAL_SETTINGS =
            ImmutableList.<String>builder()
                    .addAll(SUPPORTED_SETTINGS)
                    .add(AUTO_EXPAND_REPLICAS)
                    .build();

    protected static final ImmutableList<String> SUPPORTED_MAPPINGS =
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

    public static ImmutableMap<String,Object> tableParametersFromIndexMetaData(IndexMetaData metaData) {
        Settings settings = metaData.getSettings();
        return ImmutableMap.<String,Object>builder()
                .put(TableParameterInfo.READ_ONLY, CrateTableSettings.READ_ONLY.extract(settings))
                .put(TableParameterInfo.BLOCKS_READ, CrateTableSettings.BLOCKS_READ.extract(settings))
                .put(TableParameterInfo.BLOCKS_WRITE, CrateTableSettings.BLOCKS_WRITE.extract(settings))
                .put(TableParameterInfo.BLOCKS_METADATA, CrateTableSettings.BLOCKS_METADATA.extract(settings))
                .put(TableParameterInfo.FLUSH_THRESHOLD_OPS, CrateTableSettings.FLUSH_THRESHOLD_OPS.extract(settings))
                .put(TableParameterInfo.FLUSH_THRESHOLD_PERIOD, CrateTableSettings.FLUSH_THRESHOLD_PERIOD.extractMillis(settings))
                .put(TableParameterInfo.FLUSH_THRESHOLD_SIZE, CrateTableSettings.FLUSH_THRESHOLD_SIZE.extractBytes(settings))
                .put(TableParameterInfo.FLUSH_DISABLE, CrateTableSettings.FLUSH_DISABLE.extract(settings))
                .put(TableParameterInfo.TRANSLOG_INTERVAL, CrateTableSettings.TRANSLOG_INTERVAL.extractMillis(settings))
                .put(TableParameterInfo.ROUTING_ALLOCATION_ENABLE, CrateTableSettings.ROUTING_ALLOCATION_ENABLE.extract(settings))
                .put(TableParameterInfo.TOTAL_SHARDS_PER_NODE, CrateTableSettings.TOTAL_SHARDS_PER_NODE.extract(settings))
                .put(TableParameterInfo.RECOVERY_INITIAL_SHARDS, CrateTableSettings.RECOVERY_INITIAL_SHARDS.extract(settings))
                .put(TableParameterInfo.WARMER_ENABLED, CrateTableSettings.WARMER_ENABLED.extract(settings))
                .put(TableParameterInfo.GATEWAY_LOCAL_SYNC, CrateTableSettings.GATEWAY_LOCAL_SYNC.extractMillis(settings))
                .put(TableParameterInfo.REFRESH_INTERVAL, CrateTableSettings.REFRESH_INTERVAL.extractMillis(settings))
                .put(TableParameterInfo.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, CrateTableSettings.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT.extractMillis(settings))
                .build();
    }
}
