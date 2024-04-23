/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.replication.logical;

import java.util.Set;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.engine.EngineConfig;

import io.crate.common.unit.TimeValue;

public class LogicalReplicationSettings {

    public static final Setting<Integer> REPLICATION_CHANGE_BATCH_SIZE = Setting.intSetting(
        "replication.logical.ops_batch_size", 50000, 16,
        Property.Dynamic,
        Property.NodeScope,
        Property.Exposed
    );

    public static final Setting<TimeValue> REPLICATION_READ_POLL_DURATION = Setting.timeSetting(
        "replication.logical.reads_poll_duration",
        TimeValue.timeValueMillis(50),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueSeconds(1),
        Property.Dynamic,
        Property.NodeScope,
        Property.Exposed
    );

    public static final Setting<ByteSizeValue> REPLICATION_RECOVERY_CHUNK_SIZE =
        Setting.byteSizeSetting("replication.logical.recovery.chunk_size",
            new ByteSizeValue(1, ByteSizeUnit.MB),
            new ByteSizeValue(1, ByteSizeUnit.KB),
            new ByteSizeValue(1, ByteSizeUnit.GB),
            Property.Dynamic,
            Property.NodeScope,
            Property.Exposed
        );

    /**
     * Controls the maximum number of file chunk requests that can be sent concurrently between clusters.
     */
    public static final Setting<Integer> REPLICATION_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS =
        Setting.intSetting("replication.logical.recovery.max_concurrent_file_chunks",
            2, 1, 5,
            Property.Dynamic,
            Property.NodeScope,
            Property.Exposed
        );

    /**
     * Internal index setting marking an index as subscribed/replicated and stores to what subscription
     * this index belongs to.
     */
    public static final Setting<String> REPLICATION_SUBSCRIPTION_NAME = Setting.simpleString(
        "index.replication.logical.subscription_name",
        Setting.Property.InternalIndex,
        Setting.Property.IndexScope
    );

    /**
     * Internal index setting to store the original index UUID of the publisher cluster.
     * The index UUID on the subscriber cluster will be re-generated thus it differs from the original one.
     */
    public static final Setting<String> PUBLISHER_INDEX_UUID = Setting.simpleString(
        "index.replication.logical.publisher_index_uuid",
        Setting.Property.InternalIndex,
        Setting.Property.IndexScope
    );

    /**
     * These settings are not suitable to replicate between subscribed/replicated indices
     */
    public static final Set<Setting<?>> NON_REPLICATED_SETTINGS = Set.of(
        IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING,
        IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING,
        IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING,
        IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING,
        IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
        IndexMetadata.INDEX_READ_ONLY_SETTING,
        IndexMetadata.INDEX_BLOCKS_READ_SETTING,
        IndexMetadata.INDEX_BLOCKS_WRITE_SETTING,
        IndexMetadata.INDEX_BLOCKS_METADATA_SETTING,
        IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING,
        IndexMetadata.INDEX_PRIORITY_SETTING,
        IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS,
        EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING,
        EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING,
        ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING,
        MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY,
        UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING,
        IndexSettings.INDEX_REFRESH_INTERVAL_SETTING,
        IndexSettings.INDEX_SEARCH_IDLE_AFTER,
        IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING,
        IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING,
        IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING,
        IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING,
        IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING,
        IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING,
        IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING,
        IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING,
        IndexSettings.INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING,
        IndexSettings.INDEX_GC_DELETES_SETTING,
        IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD,
        MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING,
        MergeSchedulerConfig.AUTO_THROTTLE_SETTING,
        MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING,
        MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING,
        EngineConfig.INDEX_CODEC_SETTING
    );

    private int batchSize;
    private int maxConcurrentFileChunks;
    private TimeValue pollDelay;
    private ByteSizeValue recoveryChunkSize;

    public LogicalReplicationSettings(Settings settings, ClusterService clusterService) {
        batchSize = REPLICATION_CHANGE_BATCH_SIZE.get(settings);
        pollDelay = REPLICATION_READ_POLL_DURATION.get(settings);
        recoveryChunkSize = REPLICATION_RECOVERY_CHUNK_SIZE.get(settings);
        maxConcurrentFileChunks = REPLICATION_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS.get(settings);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(REPLICATION_CHANGE_BATCH_SIZE, this::batchSize);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REPLICATION_READ_POLL_DURATION, this::pollDelay);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REPLICATION_RECOVERY_CHUNK_SIZE, this::recoveryChunkSize);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REPLICATION_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS, this::maxConcurrentFileChunks);
    }

    public int batchSize() {
        return batchSize;
    }

    private void batchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public TimeValue pollDelay() {
        return pollDelay;
    }

    private void pollDelay(TimeValue pollDelay) {
        this.pollDelay = pollDelay;
    }

    public ByteSizeValue recoveryChunkSize() {
        return recoveryChunkSize;
    }

    private void recoveryChunkSize(final ByteSizeValue recoveryChunkSize) {
        this.recoveryChunkSize = recoveryChunkSize;
    }

    public int maxConcurrentFileChunks() {
        return maxConcurrentFileChunks;
    }

    private void maxConcurrentFileChunks(int maxConcurrentFileChunks) {
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
    }
}
