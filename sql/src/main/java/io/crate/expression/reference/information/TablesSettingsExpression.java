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

package io.crate.expression.reference.information;

import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.ObjectCollectExpression;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.doc.DocTableInfo;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;

import java.util.HashMap;
import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.index.engine.EngineConfig.INDEX_CODEC_SETTING;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;

public class TablesSettingsExpression extends AbstractTablesSettingsExpression {

    private static final String REFRESH_INTERVAL = "refresh_interval";

    public TablesSettingsExpression() {
        addChildImplementations();
    }

    private void addChildImplementations() {

        childImplementations.put(REFRESH_INTERVAL, new TableParameterExpression(INDEX_REFRESH_INTERVAL_SETTING.getKey()));
        childImplementations.put(TablesSettingsBlocksExpression.NAME, new TablesSettingsBlocksExpression());
        childImplementations.put(
            "codec", forFunction(
                (RelationInfo row) -> row.parameters().getOrDefault(INDEX_CODEC_SETTING.getKey(), INDEX_CODEC_SETTING.getDefault(Settings.EMPTY))
            )
        );
        childImplementations.put("store", new ObjectCollectExpression<RelationInfo>(
            Map.of(
                "type",
                forFunction((RelationInfo row) -> row.parameters().get(INDEX_STORE_TYPE_SETTING.getKey()))))
        );
        childImplementations.put(TablesSettingsMappingExpression.NAME, new TablesSettingsMappingExpression());
        childImplementations.put(TablesSettingsRoutingExpression.NAME, new TablesSettingsRoutingExpression());
        childImplementations.put(TablesSettingsWarmerExpression.NAME, new TablesSettingsWarmerExpression());
        childImplementations.put(TablesSettingsTranslogExpression.NAME, new TablesSettingsTranslogExpression());
        childImplementations.put(TablesSettingsWriteExpression.NAME, new TablesSettingsWriteExpression());
        childImplementations.put(TablesSettingsUnassignedExpression.NAME, new TablesSettingsUnassignedExpression());
    }

    static class TableParameterExpression implements NestableCollectExpression<RelationInfo, Object> {

        private final String paramName;
        private Object value;

        TableParameterExpression(String paramName) {
            this.paramName = paramName;
        }

        @Override
        public void setNextRow(RelationInfo row) {
            value = null;
            if (row instanceof DocTableInfo) {
                value = row.parameters().get(paramName);
            }
        }

        @Override
        public Object value() {
            return value;
        }
    }

    static class StringTableParameterExpression implements NestableCollectExpression<RelationInfo, String> {

        private final String paramName;
        private String value;

        StringTableParameterExpression(String paramName) {
            this.paramName = paramName;
        }

        @Override
        public void setNextRow(RelationInfo row) {
            Object o = row.parameters().get(paramName);
            if (o == null) {
                value = null;
            } else {
                value = o.toString();
            }
        }

        @Override
        public String value() {
            return value;
        }
    }

    static class TablesSettingsMappingExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "mapping";

        TablesSettingsMappingExpression() {
            childImplementations.put(TablesSettingsMappingTotalFieldsExpression.NAME, new TablesSettingsMappingTotalFieldsExpression());
        }
    }

    static class TablesSettingsMappingTotalFieldsExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "total_fields";
        public static final String LIMIT = "limit";

        TablesSettingsMappingTotalFieldsExpression() {
            childImplementations.put(LIMIT, new TableParameterExpression(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()));
        }
    }

    static class TablesSettingsBlocksExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "blocks";

        TablesSettingsBlocksExpression() {
            addChildImplementations();
        }

        public static final String READ_ONLY = "read_only";
        public static final String READ = "read";
        static final String WRITE = "write";
        static final String METADATA = "metadata";

        private void addChildImplementations() {
            childImplementations.put(READ_ONLY, new TableParameterExpression(IndexMetaData.INDEX_READ_ONLY_SETTING.getKey()));
            childImplementations.put(READ, new TableParameterExpression(IndexMetaData.INDEX_BLOCKS_READ_SETTING.getKey()));
            childImplementations.put(WRITE, new TableParameterExpression(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()));
            childImplementations.put(METADATA, new TableParameterExpression(IndexMetaData.INDEX_BLOCKS_METADATA_SETTING.getKey()));
        }
    }

    static class TablesSettingsRoutingExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "routing";

        TablesSettingsRoutingExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(TablesSettingsRoutingAllocationExpression.NAME, new TablesSettingsRoutingAllocationExpression());
        }
    }

    static class TablesSettingsRoutingAllocationExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "allocation";

        TablesSettingsRoutingAllocationExpression() {
            addChildImplementations();
        }

        static final String ENABLE = "enable";
        static final String TOTAL_SHARDS_PER_NODE = "total_shards_per_node";
        static final String REQUIRE = "require";
        static final String INCLUDE = "include";
        static final String EXCLUDE = "exclude";

        private void addChildImplementations() {
            childImplementations.put(ENABLE, new StringTableParameterExpression(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()));
            childImplementations.put(TOTAL_SHARDS_PER_NODE, new TableParameterExpression(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()));
            childImplementations.put(REQUIRE, NestableCollectExpression.<RelationInfo, Map<String, Object>>forFunction(
                r -> mapOfDynamicGroupSetting(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_PREFIX, r)));
            childImplementations.put(INCLUDE, NestableCollectExpression.<RelationInfo, Map<String, Object>>forFunction(
                r -> mapOfDynamicGroupSetting(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_PREFIX, r)));
            childImplementations.put(EXCLUDE, NestableCollectExpression.<RelationInfo, Map<String, Object>>forFunction(
                r -> mapOfDynamicGroupSetting(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX, r)));
        }
    }

    private static Map<String, Object> mapOfDynamicGroupSetting(String groupName, RelationInfo row) {
        if (row instanceof DocTableInfo) {
            Map<String, Object> valueMap = new HashMap<>();
            for (Map.Entry<String, Object> entry : row.parameters().entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(groupName)) {
                    valueMap.put(key.substring(groupName.length() + 1), entry.getValue());
                }
            }
            if (valueMap.size() > 0) {
                return valueMap;
            }
        }
        return null;
    }

    static class TablesSettingsWarmerExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "warmer";

        TablesSettingsWarmerExpression() {
            addChildImplementations();
        }

        static final String ENABLED = "enabled";

        private void addChildImplementations() {
            childImplementations.put(ENABLED, new TableParameterExpression(IndexSettings.INDEX_WARMER_ENABLED_SETTING.getKey()));
        }
    }

    static class TablesSettingsTranslogExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "translog";

        TablesSettingsTranslogExpression() {
            addChildImplementations();
        }

        static final String FLUSH_THRESHOLD_SIZE = "flush_threshold_size";
        static final String SYNC_INTERVAL = "sync_interval";
        static final String DURABILITY = "durability";

        private void addChildImplementations() {
            childImplementations.put(FLUSH_THRESHOLD_SIZE, new TableParameterExpression(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey()));
            childImplementations.put(SYNC_INTERVAL, new TableParameterExpression(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey()));
            childImplementations.put(DURABILITY, new TableParameterExpression(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey()));
        }
    }

    static class TablesSettingsWriteExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "write";

        TablesSettingsWriteExpression() {
            addChildImplementations();
        }

        static final String WAIT_FOR_ACTIVE_SHARDS = "wait_for_active_shards";

        private void addChildImplementations() {
            childImplementations.put(
                WAIT_FOR_ACTIVE_SHARDS,
                new StringTableParameterExpression(IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey()));
        }
    }

    static class TablesSettingsUnassignedExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "unassigned";

        TablesSettingsUnassignedExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(TablesSettingsNodeLeftExpression.NAME,
                new TablesSettingsNodeLeftExpression());
        }
    }

    static class TablesSettingsNodeLeftExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "node_left";

        TablesSettingsNodeLeftExpression() {
            addChildImplementations();
        }

        static final String DELAYED_TIMEOUT = "delayed_timeout";

        private void addChildImplementations() {
            childImplementations.put(DELAYED_TIMEOUT, new TableParameterExpression(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey()));
        }
    }
}
