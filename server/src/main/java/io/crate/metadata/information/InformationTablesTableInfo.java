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
import static io.crate.types.DataTypes.STRING_ARRAY;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.index.MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING;
import static org.elasticsearch.index.MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING;
import static org.elasticsearch.index.engine.EngineConfig.INDEX_CODEC_SETTING;

import java.util.List;
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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.translog.Translog;

import io.crate.Constants;
import io.crate.common.collections.Lists;
import io.crate.common.unit.TimeValue;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ShardedTable;
import io.crate.metadata.table.StoredTable;
import io.crate.sql.tree.ColumnPolicy;

public class InformationTablesTableInfo {

    public static final String NAME = "tables";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static final String SELF_REFERENCING_COLUMN_NAME = "_id";
    private static final String REFERENCE_GENERATION = "SYSTEM GENERATED";

    public static SystemTable<RelationInfo> INSTANCE = SystemTable.<RelationInfo>builder(IDENT)
        .add("table_schema", STRING, r -> r.ident().schema())
        .add("table_name", STRING, r -> r.ident().name())
        .add("table_catalog", STRING, r -> Constants.DB_NAME)
        .add("table_type", STRING, r -> r.relationType().pretty())
        .add("number_of_shards", INTEGER, row -> {
            if (row instanceof ShardedTable) {
                return ((ShardedTable) row).numberOfShards();
            }
            return null;
        })
        .add("number_of_replicas", STRING,
            row -> {
                if (row instanceof ShardedTable) {
                    return ((ShardedTable) row).numberOfReplicas();
                }
                return null;
            })
        .add("clustered_by", STRING,
            row -> {
                if (row instanceof ShardedTable) {
                    ColumnIdent clusteredBy = ((ShardedTable) row).clusteredBy();
                    if (clusteredBy == null) {
                        return null;
                    }
                    return clusteredBy.fqn();
                }
                return null;
            })
        .add("partitioned_by", STRING_ARRAY,
            row -> {
                if (row instanceof DocTableInfo) {
                    List<ColumnIdent> partitionedBy = ((DocTableInfo) row).partitionedBy();
                    if (partitionedBy == null || partitionedBy.isEmpty()) {
                        return null;
                    }
                    return Lists.map(partitionedBy, ColumnIdent::sqlFqn);
                }
                return null;
            })
        .add("blobs_path", STRING,
            row -> {
                if (row instanceof BlobTableInfo) {
                    return ((BlobTableInfo) row).blobsPath();
                }
                return null;
            })
        .add("column_policy", STRING,
            row -> {
                if (row instanceof DocTableInfo) {
                    return ((DocTableInfo) row).columnPolicy().lowerCaseName();
                }
                return ColumnPolicy.STRICT.lowerCaseName();
            })
        .add("routing_hash_function", STRING,
            row -> {
                if (row instanceof ShardedTable) {
                    return IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME;
                }
                return null;
            })
        .startObject("version", x -> !(x instanceof ShardedTable))
            .add(
                Version.Property.CREATED.toString(),
                STRING,
                x -> {
                    if (x instanceof StoredTable) {
                        Version version = ((StoredTable) x).versionCreated();
                        return version == null ? null : version.externalNumber();
                    }
                    return null;
                }
            )
            .add(
                Version.Property.UPGRADED.toString(),
                STRING,
                x -> {
                    if (x instanceof StoredTable) {
                        Version version = ((StoredTable) x).versionUpgraded();
                        return version == null ? null : version.externalNumber();
                    }
                    return null;
                }
            )
        .endObject()
        .add("closed", BOOLEAN, row -> {
            if (row instanceof ShardedTable) {
                return ((ShardedTable) row).isClosed();
            }
            return null;
        })
        .add("reference_generation", STRING, r -> REFERENCE_GENERATION)
        .add("self_referencing_column_name", STRING,row -> {
            if (row instanceof ShardedTable) {
                return SELF_REFERENCING_COLUMN_NAME;
            }
            return null;
        })
        .startObject("settings", x -> !(x instanceof ShardedTable))
            .add("refresh_interval", LONG, fromTimeValue(INDEX_REFRESH_INTERVAL_SETTING))

            .startObject("blocks")
                .add("read_only", BOOLEAN, fromSetting(IndexMetadata.INDEX_READ_ONLY_SETTING))
                .add("read", BOOLEAN, fromSetting(IndexMetadata.INDEX_BLOCKS_READ_SETTING))
                .add("write", BOOLEAN, fromSetting(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING))
                .add("metadata", BOOLEAN, fromSetting(IndexMetadata.INDEX_BLOCKS_METADATA_SETTING))
                .add("read_only_allow_delete", BOOLEAN, fromSetting(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING))
            .endObject()
            .add("codec", STRING, fromSetting(INDEX_CODEC_SETTING))
            .startObject("store")
                .add("type", STRING, fromSetting(INDEX_STORE_TYPE_SETTING))
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
            ColumnIdent.of("table_catalog"),
            ColumnIdent.of("table_schema"),
            ColumnIdent.of("table_name")
        )
        .build();

    private static Function<RelationInfo, Long> fromByteSize(Setting<ByteSizeValue> byteSizeSetting) {
        return rel -> {
            if (rel instanceof StoredTable) {
                return byteSizeSetting.get(rel.parameters()).getBytes();
            }
            return null;
        };
    }

    private static <T> Function<RelationInfo, T> fromSetting(Setting<T> setting) {
        return rel -> {
            if (rel instanceof StoredTable) {
                return setting.get(rel.parameters());
            }
            return null;
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> Function<RelationInfo, Map<String, Object>> fromSetting(Setting.AffixSetting<T> setting) {
        return rel -> {
            if (rel instanceof StoredTable) {
                return (Map<String, Object>) setting.getAsMap(rel.parameters());
            }
            return null;
        };
    }

    private static <T, U> Function<RelationInfo, U> fromSetting(Setting<T> setting, Function<T, U> andThen) {
        return rel -> {
            if (rel instanceof StoredTable) {
                return andThen.apply(setting.get(rel.parameters()));
            }
            return null;
        };
    }

    private static Function<RelationInfo, Long> fromTimeValue(Setting<TimeValue> timeValueSetting) {
        return rel -> {
            if (rel instanceof StoredTable) {
                return timeValueSetting.get(rel.parameters()).millis();
            }
            return null;
        };
    }
}
