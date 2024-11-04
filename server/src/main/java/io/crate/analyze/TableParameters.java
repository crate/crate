/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.store.Store;

import io.crate.blob.v2.BlobIndicesService;
import io.crate.common.annotations.Immutable;
import io.crate.common.annotations.ThreadSafe;
import io.crate.common.collections.Lists;
import io.crate.common.collections.MapBuilder;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.NumberOfReplicas;
import io.crate.metadata.settings.Validators;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;

/**
 * Container for the supported settings that can be used in the `WITH` clause of `CREATE TABLE` statements
 * or which can be updated via `ALTER TABLE SET` statements.
 *
 */
@Immutable
@ThreadSafe
public class TableParameters {

    // all available table settings
    public static final Setting<ColumnPolicy> COLUMN_POLICY = new Setting<>(
        new Setting.SimpleKey("column_policy"),
        _ -> ColumnPolicy.STRICT.lowerCaseName(),
        s -> ColumnPolicy.of(s),
        o -> {
            if (ColumnPolicy.IGNORED.equals(o)) {
                throw new IllegalArgumentException("Invalid value for argument 'column_policy'");
            }
        },
        DataTypes.STRING,
        Setting.Property.IndexScope
    );


    /**
     * Settings that are only applicable on a table level and can't be changed for a
     * single partition.
     *
     * These are typically schema/mapping related.
     **/
    public static final List<Setting<?>> TABLE_ONLY_SETTINGS = List.of(
        COLUMN_POLICY
    );

    /**
     * These settings are applied on index/partition level but their default value
     * Is inherited: Cluster -> Table -> Partition
     *
     * These settings can be changed either:
     *  - on table level:       ALTER TABLE
     *  - on partition level:   ALTER TABLE <name> PARTITION (...)
     **/
    private static final List<Setting<?>> PARTITION_SETTINGS =
        List.of(
            NumberOfReplicas.SETTING,
            IndexSettings.INDEX_REFRESH_INTERVAL_SETTING,
            IndexMetadata.INDEX_READ_ONLY_SETTING,
            INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING,
            IndexMetadata.INDEX_BLOCKS_READ_SETTING,
            IndexMetadata.INDEX_BLOCKS_WRITE_SETTING,
            IndexMetadata.INDEX_BLOCKS_METADATA_SETTING,
            IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING,
            EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING,
            IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING,
            IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING,
            ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING,
            DocTableInfo.TOTAL_COLUMNS_LIMIT,
            UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING,
            IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS,
            MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY,
            IndexSettings.MAX_NGRAM_DIFF_SETTING,
            IndexSettings.MAX_SHINGLE_DIFF_SETTING,
            IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING,
            IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
            IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING,
            IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING,
            EngineConfig.INDEX_CODEC_SETTING,
            IndexModule.INDEX_STORE_TYPE_SETTING,
            MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING,
            IndexSettings.INDEX_SOFT_DELETES_SETTING,
            IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING,

            // this setting is needed for tests and is not documented. see ClusterDisruptionIT for usages.
            IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING,
            // this setting is needed for tests and is not documented. see RetentionLeaseBackgroundSyncIT for usages.
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING,

            // this setting is needed for tests and is not documented. see ReplicaShardAllocatorIT for usages.
            IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING,

            // this setting is needed for tests and is not documented. see IndexRecoveryIT for usages.
            Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING
        );

    private static final Map<String, Setting<?>> SUPPORTED_SETTINGS_DEFAULT
        = Lists.concat(PARTITION_SETTINGS, TABLE_ONLY_SETTINGS)
        .stream()
        .collect(Collectors.toMap((s) -> stripDotSuffix(stripIndexPrefix(s.getKey())), s -> s));

    private static final Map<String, Setting<?>> SUPPORTED_NON_FINAL_SETTINGS_DEFAULT
        = Lists.concat(PARTITION_SETTINGS, TABLE_ONLY_SETTINGS)
            .stream()
            .filter(s -> s.isFinal() == false)
            .collect(Collectors.toMap((s) -> stripDotSuffix(stripIndexPrefix(s.getKey())), s -> s));

    private static final Map<String, Setting<?>> SUPPORTED_SETTINGS_INCL_SHARDS
        = MapBuilder.newMapBuilder(SUPPORTED_NON_FINAL_SETTINGS_DEFAULT)
            .put(
                stripIndexPrefix(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey()),
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING
            ).immutableMap();

    private static final Map<String, Setting<?>> SUPPORTED_SETTINGS_FOR_REPLICATED_TABLES = PARTITION_SETTINGS
        .stream()
        .filter(Setting::isReplicatedIndexScope)
        .filter(s -> s.isFinal() == false)
        .collect(Collectors.toMap(s -> stripDotSuffix(stripIndexPrefix(s.getKey())), s -> s));

    public static final TableParameters TABLE_CREATE_PARAMETER_INFO
        = new TableParameters(SUPPORTED_SETTINGS_DEFAULT);

    public static final TableParameters REPLICATED_TABLE_ALTER_PARAMETER_INFO
        = new TableParameters(SUPPORTED_SETTINGS_FOR_REPLICATED_TABLES);

    public static final TableParameters TABLE_ALTER_PARAMETER_INFO
        = new TableParameters(SUPPORTED_SETTINGS_INCL_SHARDS);

    public static final TableParameters PARTITIONED_TABLE_PARAMETER_INFO_FOR_TEMPLATE_UPDATE
        = new TableParameters(SUPPORTED_NON_FINAL_SETTINGS_DEFAULT);

    public static final TableParameters PARTITION_PARAMETER_INFO = new TableParameters(SUPPORTED_SETTINGS_INCL_SHARDS);

    public static final TableParameters CREATE_BLOB_TABLE_PARAMETERS = new TableParameters(
        Map.of(
            stripIndexPrefix(NumberOfReplicas.SETTING.getKey()), NumberOfReplicas.SETTING,
            "blobs_path", Setting.simpleString(
                BlobIndicesService.SETTING_INDEX_BLOBS_PATH.getKey(), Validators.stringValidator("blobs_path"))
        )
    );

    public static final TableParameters ALTER_BLOB_TABLE_PARAMETERS = new TableParameters(
        Map.of(stripIndexPrefix(NumberOfReplicas.SETTING.getKey()),
               NumberOfReplicas.SETTING,
               stripDotSuffix(stripIndexPrefix(SETTING_READ_ONLY_ALLOW_DELETE)),
               INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING
        ));

    private final Map<String, Setting<?>> supportedSettings;

    protected TableParameters(Map<String, Setting<?>> supportedSettings) {
        this.supportedSettings = supportedSettings;
    }

    /**
     * Returns list of public settings names supported by this table
     */
    public Map<String, Setting<?>> supportedSettings() {
        return supportedSettings;
    }

    public static String stripIndexPrefix(String key) {
        if (key.startsWith(IndexMetadata.INDEX_SETTING_PREFIX)) {
            return key.substring(IndexMetadata.INDEX_SETTING_PREFIX.length());
        }
        return key;
    }

    private static String stripDotSuffix(String key) {
        if (key.endsWith(".")) {
            return key.substring(0, key.length() - 1);
        }
        return key;
    }
}
