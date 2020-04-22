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

package io.crate.metadata.information;

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.STRING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.index.MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING;
import static org.elasticsearch.index.MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING;
import static org.elasticsearch.index.engine.EngineConfig.INDEX_CODEC_SETTING;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;

import io.crate.common.StringUtils;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.PartitionInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public class InformationPartitionsTableInfo {

    public static final String NAME = "table_partitions";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);


    public static SystemTable<PartitionInfo> create() {
        return SystemTable.<PartitionInfo>builder(IDENT)
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
                    .add("read_only", BOOLEAN, r -> (Boolean) r.tableParameters().get(IndexMetaData.INDEX_READ_ONLY_SETTING.getKey()))
                    .add("read", BOOLEAN, r -> (Boolean) r.tableParameters().get(IndexMetaData.INDEX_BLOCKS_READ_SETTING.getKey()))
                    .add("write", BOOLEAN, r -> (Boolean) r.tableParameters().get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()))
                    .add("metadata", BOOLEAN, r -> (Boolean) r.tableParameters().get(IndexMetaData.INDEX_BLOCKS_METADATA_SETTING.getKey()))
                .endObject()

                .add("codec", STRING, r -> (String) r.tableParameters().getOrDefault(INDEX_CODEC_SETTING.getKey(), INDEX_CODEC_SETTING.getDefault(Settings.EMPTY)))

                .startObject("store")
                    .add("type", STRING, r -> StringUtils.nullOrString(r.tableParameters().get(INDEX_STORE_TYPE_SETTING.getKey())))
                .endObject()

                .startObject("translog")
                    .add("flush_threshold_size", LONG, r -> (Long) r.tableParameters().get(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey()))
                    .add("sync_interval", LONG, r -> (Long) r.tableParameters().get(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey()))
                    .add("durability", STRING, r -> StringUtils.nullOrString(r.tableParameters().get(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey())))
                .endObject()

                .startObject("routing")
                    .startObject("allocation")
                        .add("enable", STRING, r ->
                            StringUtils.nullOrString(r.tableParameters().get(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey())))
                        .add("total_shards_per_node", INTEGER, r ->
                            (Integer) r.tableParameters().get(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()))
                    .endObject()
                .endObject()

                .startObject("warmer")
                    .add("enabled", BOOLEAN, r -> (Boolean) r.tableParameters().get(IndexSettings.INDEX_WARMER_ENABLED_SETTING.getKey()))
                .endObject()

                .startObject("unassigned")
                    .startObject("node_left")
                        .add("delayed_timeout", LONG, r -> (Long) r.tableParameters().get(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey()))
                    .endObject()
                .endObject()

                .startObject("mapping")
                    .startObject("total_fields")
                        .add("limit", INTEGER, r -> (Integer) r.tableParameters().get(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()))
                    .endObject()
                .endObject()

                .startObject("merge")
                    .startObject("scheduler")
                        .add("max_thread_count", INTEGER, r -> (Integer) r.tableParameters().get(MAX_THREAD_COUNT_SETTING.getKey()))
                        .add("max_merge_count", INTEGER, r -> (Integer) r.tableParameters().get(MAX_MERGE_COUNT_SETTING.getKey()))
                    .endObject()
                .endObject()

                .startObject("write")
                    .add("wait_for_active_shards", STRING, r -> (String) r.tableParameters().get(IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey()))
                .endObject()

            .endObject()
            .setPrimaryKeys(
                new ColumnIdent("table_schema"),
                new ColumnIdent("table_name"),
                new ColumnIdent("partition_ident")
            )
            .build();
    }
}
