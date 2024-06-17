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

package io.crate.metadata.information;

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.STRING;
import static org.elasticsearch.index.MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING;
import static org.elasticsearch.index.MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING;
import static org.elasticsearch.index.engine.EngineConfig.INDEX_CODEC_SETTING;

import java.util.Map;
import java.util.function.Function;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.translog.Translog;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.PartitionInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.doc.DocTableInfo;

public class InformationPartitionsTableInfo {

    public static final String NAME = "table_partitions";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static SystemTable<PartitionInfo> INSTANCE = SystemTable.<PartitionInfo>builder(IDENT)
        .add("table_schema", STRING, r -> r.name().relationName().schema())
        .add("table_name", STRING, r -> r.name().relationName().name())
        .add("partition_ident", STRING, r -> r.name().ident())
        .addDynamicObject("values", STRING, PartitionInfo::values)
        .add("number_of_shards", INTEGER, PartitionInfo::numberOfShards)
        .add("number_of_replicas", STRING, PartitionInfo::numberOfReplicas)
        .add("routing_hash_function", STRING, r -> IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME)
        .add("closed", BOOLEAN, PartitionInfo::isClosed)
        .startObject("version")
            .add(Version.Property.CREATED.toString(), STRING, r -> r.versionCreated().externalNumber())
            .add(Version.Property.UPGRADED.toString(), STRING, r -> r.versionUpgraded().externalNumber())
        .endObject()
        .startObject("settings")
            .startObject("blocks")
                .add("read_only", BOOLEAN, fromSetting(IndexMetadata.INDEX_READ_ONLY_SETTING))
                .add("read", BOOLEAN, fromSetting(IndexMetadata.INDEX_BLOCKS_READ_SETTING))
                .add("write", BOOLEAN, fromSetting(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING))
                .add("metadata", BOOLEAN, fromSetting(IndexMetadata.INDEX_BLOCKS_METADATA_SETTING))
                .add("read_only_allow_delete", BOOLEAN, fromSetting(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING))
            .endObject()

            .add("codec", STRING, fromSetting(INDEX_CODEC_SETTING))

            .startObject("store")
                .add("type", STRING, fromSetting(IndexModule.INDEX_STORE_TYPE_SETTING))
            .endObject()

            .startObject("translog")
                .add("flush_threshold_size", LONG, fromByteSize(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING))
                .add("sync_interval", LONG, fromTimeValue(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING))
                .add("durability", STRING, fromSetting(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING, Translog.Durability::name))
            .endObject()

            .startObject("routing")
                .startObject("allocation")
                    .add("enable", STRING, fromSetting(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING, EnableAllocationDecider.Allocation::toString))
                    .add("total_shards_per_node", INTEGER, fromSetting(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING))
                    .addDynamicObject("require", STRING, fromSetting(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING))
                    .addDynamicObject("include", STRING, fromSetting(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING))
                    .addDynamicObject("exclude", STRING, fromSetting(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING))
                .endObject()
            .endObject()

            .startObject("unassigned")
                .startObject("node_left")
                    .add("delayed_timeout", LONG, fromTimeValue(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING))
                .endObject()
            .endObject()

            .startObject("mapping")
                .startObject("total_fields")
                    .add("limit", INTEGER, fromSetting(DocTableInfo.TOTAL_COLUMNS_LIMIT, INTEGER::sanitizeValue))
                .endObject()
            .endObject()

            .startObject("merge")
                .startObject("scheduler")
                    .add("max_thread_count", INTEGER, fromSetting(MAX_THREAD_COUNT_SETTING))
                    .add("max_merge_count", INTEGER, fromSetting(MAX_MERGE_COUNT_SETTING))
                .endObject()
            .endObject()

            .startObject("write")
                .add("wait_for_active_shards", STRING, fromSetting(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS, ActiveShardCount::toString))
            .endObject()

        .endObject()
        .setPrimaryKeys(
            new ColumnIdent("table_schema"),
            new ColumnIdent("table_name"),
            new ColumnIdent("partition_ident")
        )
        .build();

    private static Function<PartitionInfo, Long> fromByteSize(Setting<ByteSizeValue> byteSizeSetting) {
        return rel -> byteSizeSetting.get(rel.tableParameters()).getBytes();
    }

    @SuppressWarnings("unchecked")
    private static <T> Function<PartitionInfo, Map<String, Object>> fromSetting(Setting.AffixSetting<T> setting) {
        return rel -> {
            return (Map<String, Object>) setting.getAsMap(rel.tableParameters());
        };
    }

    private static <T> Function<PartitionInfo, T> fromSetting(Setting<T> setting) {
        return rel -> setting.get(rel.tableParameters());
    }

    private static <T, U> Function<PartitionInfo, U> fromSetting(Setting<T> setting, Function<T, U> andThen) {
        return rel -> andThen.apply(setting.get(rel.tableParameters()));
    }

    private static Function<PartitionInfo, Long> fromTimeValue(Setting<TimeValue> timeValueSetting) {
        return rel -> timeValueSetting.get(rel.tableParameters()).millis();
    }
}
