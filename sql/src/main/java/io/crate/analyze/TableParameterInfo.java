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
import io.crate.metadata.settings.NumberOfReplicasSetting;
import io.crate.metadata.settings.Validators;
import io.crate.metadata.table.ColumnPolicy;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.translog.Translog;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

@Immutable
@ThreadSafe
public class TableParameterInfo {

    // all available table settings
    static final NumberOfReplicasSetting NUMBER_OF_REPLICAS = new NumberOfReplicasSetting();
    static final Setting<Integer> NUMBER_OF_SHARDS = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING;
    public static final Setting<Boolean> READ_ONLY = IndexMetaData.INDEX_READ_ONLY_SETTING;
    static final Setting<Boolean> READ_ONLY_ALLOW_DELETE = IndexMetaData.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING;
    public static final Setting<Boolean> BLOCKS_READ = IndexMetaData.INDEX_BLOCKS_READ_SETTING;
    public static final Setting<Boolean> BLOCKS_WRITE = IndexMetaData.INDEX_BLOCKS_WRITE_SETTING;
    public static final Setting<Boolean> BLOCKS_METADATA = IndexMetaData.INDEX_BLOCKS_METADATA_SETTING;
    public static final Setting<Integer> TOTAL_SHARDS_PER_NODE = ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING;
    public static final Setting<Long> MAPPING_TOTAL_FIELDS_LIMIT = MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;
    public static final Setting<EnableAllocationDecider.Allocation> ROUTING_ALLOCATION_ENABLE =
        EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING;
    public static final Setting<ActiveShardCount> SETTING_WAIT_FOR_ACTIVE_SHARDS = IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS;
    public static final Setting<ByteSizeValue> FLUSH_THRESHOLD_SIZE = IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING;
    public static final Setting<Boolean> WARMER_ENABLED = IndexSettings.INDEX_WARMER_ENABLED_SETTING;
    public static final Setting<TimeValue> TRANSLOG_SYNC_INTERVAL = IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING;
    public static final Setting<Translog.Durability> TRANSLOG_DURABILITY = IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING;
    public static final Setting<TimeValue> REFRESH_INTERVAL = IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
    public static final Setting<TimeValue> UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT = UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
    static final Setting<Integer> ALLOCATION_MAX_RETRIES = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
    static final Setting<Integer> MAX_NGRAM_DIFF = IndexSettings.MAX_NGRAM_DIFF_SETTING;
    static final Setting<Integer> MAX_SHINGLE_DIFF = IndexSettings.MAX_SHINGLE_DIFF_SETTING;
    static final Setting<Object> COLUMN_POLICY =
        new Setting<>(
            new Setting.SimpleKey(ColumnPolicy.ES_MAPPING_NAME),
            (s) -> ColumnPolicy.DYNAMIC.value(),
            (s) -> ColumnPolicy.byName(s).mappingValue(),
            (o, m) -> {
                if (ColumnPolicy.IGNORED.mappingValue().equals(o)) {
                    throw new IllegalArgumentException("Invalid value for argument '" + ColumnPolicy.CRATE_NAME + "'");
                }
            },
            Setting.Property.IndexScope);

    // all available table mapping keys

    private static final ImmutableList<Setting> SUPPORTED_SETTINGS =
        ImmutableList.<Setting>builder()
            .add(NUMBER_OF_REPLICAS)
            .add(REFRESH_INTERVAL)
            .add(READ_ONLY)
            .add(READ_ONLY_ALLOW_DELETE)
            .add(BLOCKS_READ)
            .add(BLOCKS_WRITE)
            .add(BLOCKS_METADATA)
            .add(FLUSH_THRESHOLD_SIZE)
            .add(ROUTING_ALLOCATION_ENABLE)
            .add(TRANSLOG_SYNC_INTERVAL)
            .add(TRANSLOG_DURABILITY)
            .add(TOTAL_SHARDS_PER_NODE)
            .add(MAPPING_TOTAL_FIELDS_LIMIT)
            .add(WARMER_ENABLED)
            .add(UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT)
            .add(SETTING_WAIT_FOR_ACTIVE_SHARDS)
            .add(ALLOCATION_MAX_RETRIES)
            .add(MAX_NGRAM_DIFF)
            .add(MAX_SHINGLE_DIFF)
            .build();

    private static final ImmutableMap<String, Setting> SUPPORTED_SETTINGS_DEFAULT
        = SUPPORTED_SETTINGS
            .stream()
            .collect(ImmutableMap.toImmutableMap((s) -> stripIndexPrefix(s.getKey()), s -> s));

    private static final ImmutableList<Setting> EXCLUDED_SETTING_FOR_METADATA_IMPORT =
        ImmutableList.<Setting>builder()
            .add(NUMBER_OF_REPLICAS)
            .build();

    private static final ImmutableMap<String, Setting> SUPPORTED_SETTINGS_INCL_SHARDS
        = ImmutableMap.<String, Setting>builder()
            .putAll(SUPPORTED_SETTINGS_DEFAULT)
            .put(stripIndexPrefix(NUMBER_OF_SHARDS.getKey()), NUMBER_OF_SHARDS)
            .build();

    private static final ImmutableMap<String, Setting> SUPPORTED_SETTINGS_FOR_BLOB_CREATION
        = ImmutableMap.<String, Setting>builder()
            .put(NUMBER_OF_REPLICAS.getKey(), NUMBER_OF_REPLICAS)
            .put("blobs_path",
                Setting.simpleString(BlobIndicesService.SETTING_INDEX_BLOBS_PATH.getKey(),
                Validators.stringValidator("blobs_path")))
            .build();

    private static final ImmutableMap<String, Setting> SUPPORTED_SETTINGS_FOR_BLOB_ALTERING
        = ImmutableMap.<String, Setting>builder()
            .put(NUMBER_OF_REPLICAS.getKey(), NUMBER_OF_REPLICAS)
            .build();

    private static final ImmutableMap<String, Setting> SUPPORTED_MAPPINGS_DEFAULT
        = ImmutableMap.<String, Setting>builder()
            .put("column_policy", COLUMN_POLICY)
            .build();

    private static final ImmutableMap<String, Setting> EMPTY_MAP = ImmutableMap.of();

    static final TableParameterInfo TABLE_CREATE_PARAMETER_INFO
        = new TableParameterInfo(SUPPORTED_SETTINGS_DEFAULT, SUPPORTED_MAPPINGS_DEFAULT);
    static final TableParameterInfo TABLE_ALTER_PARAMETER_INFO
        = new TableParameterInfo(SUPPORTED_SETTINGS_INCL_SHARDS, SUPPORTED_MAPPINGS_DEFAULT);
    public static final TableParameterInfo PARTITIONED_TABLE_PARAMETER_INFO_FOR_TEMPLATE_UPDATE
        = new TableParameterInfo(SUPPORTED_SETTINGS_DEFAULT, EMPTY_MAP);
    static final TableParameterInfo PARTITION_PARAMETER_INFO
        = new TableParameterInfo(SUPPORTED_SETTINGS_INCL_SHARDS, EMPTY_MAP);
    static final TableParameterInfo BLOB_TABLE_CREATE_PARAMETER_INFO
        = new TableParameterInfo(SUPPORTED_SETTINGS_FOR_BLOB_CREATION, EMPTY_MAP);
    public static final TableParameterInfo BLOB_TABLE_ALTER_PARAMETER_INFO
        = new TableParameterInfo(SUPPORTED_SETTINGS_FOR_BLOB_ALTERING, EMPTY_MAP);


    private final ImmutableMap<String, Setting> supportedSettings;
    private final ImmutableMap<String, Setting> supportedMappings;

    protected TableParameterInfo(final ImmutableMap<String, Setting> supportedSettings,
                                 final ImmutableMap<String, Setting> supportedMappings) {
        this.supportedSettings = supportedSettings;
        this.supportedMappings = supportedMappings;
    }

    /**
     * Returns list of public settings names supported by this table
     */
    public ImmutableMap<String, Setting> supportedSettings() {
        return supportedSettings;
    }

    /**
     * Returns a list of mapping names supported by this table
     */
    public ImmutableMap<String, Setting> supportedMappings() {
        return supportedMappings;
    }

    public static String stripIndexPrefix(String key) {
        if (key.startsWith(IndexMetaData.INDEX_SETTING_PREFIX)) {
            return key.substring(IndexMetaData.INDEX_SETTING_PREFIX.length());
        }
        return key;
    }

    public static ImmutableMap<String, Object> tableParametersFromIndexMetaData(IndexMetaData metaData) {
        Settings settings = metaData.getSettings();
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        for (Setting setting : SUPPORTED_SETTINGS) {
            if (EXCLUDED_SETTING_FOR_METADATA_IMPORT.contains(setting) == false) {
                builder.put(setting.getKey(), convertEsSettingType(setting.get(settings)));
            }
        }
        return builder.build();
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
