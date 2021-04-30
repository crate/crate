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

import io.crate.blob.v2.BlobIndicesService;
import io.crate.common.collections.MapBuilder;
import io.crate.metadata.settings.NumberOfReplicasSetting;
import io.crate.metadata.settings.Validators;
import io.crate.metadata.table.ColumnPolicies;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.mapper.MapperService;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Container for the supported settings that can be used in the `WITH` clause of `CREATE TABLE` statements
 * or which can be updated via `ALTER TABLE SET` statements.
 *
 */
@Immutable
@ThreadSafe
public class TableParameters {

    // all available table settings
    static final NumberOfReplicasSetting NUMBER_OF_REPLICAS = new NumberOfReplicasSetting();
    static final Setting<String> COLUMN_POLICY = new Setting<>(
        new Setting.SimpleKey(ColumnPolicies.ES_MAPPING_NAME),
        s -> ColumnPolicy.STRICT.lowerCaseName(),
        s -> ColumnPolicies.encodeMappingValue(ColumnPolicy.of(s)),
        o -> {
            if (ColumnPolicies.encodeMappingValue(ColumnPolicy.IGNORED).equals(o)) {
                throw new IllegalArgumentException("Invalid value for argument '" + ColumnPolicies.CRATE_NAME + "'");
            }
        },
        DataTypes.STRING,
        Setting.Property.IndexScope
    );

    // all available table mapping keys

    private static final List<Setting<?>> SUPPORTED_SETTINGS =
        List.of(
            NUMBER_OF_REPLICAS,
            IndexSettings.INDEX_REFRESH_INTERVAL_SETTING,
            IndexMetadata.INDEX_READ_ONLY_SETTING,
            IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING,
            IndexMetadata.INDEX_BLOCKS_READ_SETTING,
            IndexMetadata.INDEX_BLOCKS_WRITE_SETTING,
            IndexMetadata.INDEX_BLOCKS_METADATA_SETTING,
            IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING,
            EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING,
            IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING,
            IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING,
            ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING,
            MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING,
            IndexSettings.INDEX_WARMER_ENABLED_SETTING,
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
            IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING
        );

    /**
     * Settings which are not included in table default settings
     */
    static final Set<Setting> SETTINGS_NOT_INCLUDED_IN_DEFAULT = Set.of(
        IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING,
        IndexSettings.INDEX_WARMER_ENABLED_SETTING,
        IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING,
        MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING,
        IndexSettings.INDEX_SOFT_DELETES_SETTING,
        IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING,
        IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING,

        // We want IndexSettings#isExplicitRefresh and it's usages to work
        IndexSettings.INDEX_REFRESH_INTERVAL_SETTING,

        IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING
    );

    private static final Map<String, Setting<?>> SUPPORTED_SETTINGS_DEFAULT
        = SUPPORTED_SETTINGS
            .stream()
            .collect(Collectors.toMap((s) -> stripDotSuffix(stripIndexPrefix(s.getKey())), s -> s));

    private static final Set<Setting<?>> EXCLUDED_SETTING_FOR_METADATA_IMPORT = Set.of(NUMBER_OF_REPLICAS);

    private static final Map<String, Setting<?>> SUPPORTED_SETTINGS_INCL_SHARDS
        = MapBuilder.newMapBuilder(SUPPORTED_SETTINGS_DEFAULT)
            .put(
                stripIndexPrefix(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey()),
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING
            ).immutableMap();

    private static final Map<String, Setting<?>> SUPPORTED_MAPPINGS_DEFAULT = Map.of("column_policy", COLUMN_POLICY);

    public static final TableParameters TABLE_CREATE_PARAMETER_INFO
        = new TableParameters(SUPPORTED_SETTINGS_DEFAULT, SUPPORTED_MAPPINGS_DEFAULT);

    public static final TableParameters TABLE_ALTER_PARAMETER_INFO
        = new TableParameters(SUPPORTED_SETTINGS_INCL_SHARDS, SUPPORTED_MAPPINGS_DEFAULT);

    public static final TableParameters PARTITIONED_TABLE_PARAMETER_INFO_FOR_TEMPLATE_UPDATE
        = new TableParameters(SUPPORTED_SETTINGS_DEFAULT, Map.of());

    public static final TableParameters PARTITION_PARAMETER_INFO
        = new TableParameters(SUPPORTED_SETTINGS_INCL_SHARDS, Map.of());

    public static final TableParameters CREATE_BLOB_TABLE_PARAMETERS = new TableParameters(
        Map.of(
            NUMBER_OF_REPLICAS.getKey(), NUMBER_OF_REPLICAS,
            "blobs_path", Setting.simpleString(
                BlobIndicesService.SETTING_INDEX_BLOBS_PATH.getKey(), Validators.stringValidator("blobs_path"))
        ),
        Map.of()
    );

    public static final TableParameters ALTER_BLOB_TABLE_PARAMETERS = new TableParameters(
        Map.of(NUMBER_OF_REPLICAS.getKey(), NUMBER_OF_REPLICAS),
        Map.of()
    );

    private final Map<String, Setting<?>> supportedSettings;
    private final Map<String, Setting<?>> supportedMappings;

    protected TableParameters(Map<String, Setting<?>> supportedSettings, Map<String, Setting<?>> supportedMappings) {
        this.supportedSettings = supportedSettings;
        this.supportedMappings = supportedMappings;
    }

    /**
     * Returns list of public settings names supported by this table
     */
    public Map<String, Setting<?>> supportedSettings() {
        return supportedSettings;
    }

    /**
     * Returns a list of mapping names supported by this table
     */
    public Map<String, Setting<?>> supportedMappings() {
        return supportedMappings;
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

    public static Map<String, Object> tableParametersFromIndexMetadata(Settings settings) {
        MapBuilder<String, Object> builder = MapBuilder.newMapBuilder();
        for (Setting<?> setting : SUPPORTED_SETTINGS) {
            boolean shouldBeExcluded = EXCLUDED_SETTING_FOR_METADATA_IMPORT.contains(setting);
            if (shouldBeExcluded == false) {
                if (setting instanceof Setting.AffixSetting) {
                    flattenAffixSetting(builder, settings, (Setting.AffixSetting<?>) setting);
                } else if (settings.hasValue(setting.getKey())) {
                    builder.put(setting.getKey(), convertEsSettingType(setting.get(settings)));
                }
            }
        }
        return builder.immutableMap();
    }

    private static void flattenAffixSetting(MapBuilder<String, Object> builder,
                                            Settings settings,
                                            Setting.AffixSetting<?> setting) {
        String prefix = setting.getKey();
        setting.getNamespaces(settings)
            .forEach(s -> builder.put(prefix + s, setting.getConcreteSetting(prefix + s).get(settings)));
    }

    private static Object convertEsSettingType(Object value) {
        if (value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof ByteSizeValue) {
            // return bytes as long so it can be compared correctly
            return ((ByteSizeValue) value).getBytes();
        }
        if (value instanceof TimeValue) {
            // return time as long (epoch) in MS so it can be compared correctly
            return ((TimeValue) value).getMillis();
        }
        return value.toString();
    }
}
