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

package io.crate.expression.reference.partitioned;

import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.ObjectCollectExpression;
import io.crate.metadata.PartitionInfo;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.index.engine.EngineConfig.INDEX_CODEC_SETTING;

public class PartitionsSettingsExpression extends ObjectCollectExpression<PartitionInfo> {

    public PartitionsSettingsExpression() {
        super(Map.ofEntries(
            Map.entry(PartitionsSettingsBlocksExpression.NAME, new PartitionsSettingsBlocksExpression()),
            Map.entry(
                "codec",
                forFunction((PartitionInfo row) -> row.tableParameters()
                    .getOrDefault(INDEX_CODEC_SETTING.getKey(), INDEX_CODEC_SETTING.getDefault(Settings.EMPTY)))
            ),
            Map.entry("store", new ObjectCollectExpression<PartitionInfo>(
                Map.of(
                    "type",
                    forFunction((PartitionInfo row) -> row.tableParameters().get(INDEX_STORE_TYPE_SETTING.getKey()))))
            ),
            Map.entry(PartitionSettingsMappingExpression.NAME, new PartitionSettingsMappingExpression()),
            Map.entry(PartitionsSettingsRoutingExpression.NAME, new PartitionsSettingsRoutingExpression()),
            Map.entry(PartitionsSettingsWarmerExpression.NAME, new PartitionsSettingsWarmerExpression()),
            Map.entry(PartitionsSettingsTranslogExpression.NAME, new PartitionsSettingsTranslogExpression()),
            Map.entry(PartitionsSettingsUnassignedExpression.NAME, new PartitionsSettingsUnassignedExpression()),
            Map.entry("write", new ObjectCollectExpression<PartitionInfo>(
                Map.of(
                    "wait_for_active_shards",
                    forFunction((PartitionInfo pi) ->
                        pi.tableParameters().get(IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey())))
            )
        )));
    }

    static class PartitionTableParameterExpression implements NestableCollectExpression<PartitionInfo, Object> {

        private final String paramName;
        private Object value;

        PartitionTableParameterExpression(String paramName) {
            this.paramName = paramName;
        }

        @Override
        public void setNextRow(PartitionInfo row) {
            value = row.tableParameters().get(paramName);
        }

        @Override
        public Object value() {
            return value;
        }
    }

    static class StringPartitionTableParameterExpression implements NestableCollectExpression<PartitionInfo, String> {

        private final String paramName;
        private String value;

        StringPartitionTableParameterExpression(String paramName) {
            this.paramName = paramName;
        }

        @Override
        public void setNextRow(PartitionInfo row) {
            value = row.tableParameters().get(paramName).toString();
        }

        @Override
        public String value() {
            return value;
        }
    }

    static class PartitionSettingsMappingExpression extends ObjectCollectExpression<PartitionInfo> {

        public static final String NAME = "mapping";

        PartitionSettingsMappingExpression() {
            childImplementations.put(PartitionSettingsMappingTotalFieldsExpression.NAME, new PartitionSettingsMappingTotalFieldsExpression());
        }
    }

    static class PartitionSettingsMappingTotalFieldsExpression extends ObjectCollectExpression<PartitionInfo> {

        public static final String NAME = "total_fields";
        public static final String LIMIT = "limit";

        PartitionSettingsMappingTotalFieldsExpression() {
            childImplementations.put(LIMIT, new PartitionTableParameterExpression(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()));
        }
    }

    static class PartitionsSettingsBlocksExpression extends ObjectCollectExpression<PartitionInfo> {

        public static final String NAME = "blocks";

        PartitionsSettingsBlocksExpression() {
            addChildImplementations();
        }

        public static final String READ_ONLY = "read_only";
        public static final String READ = "read";
        static final String WRITE = "write";
        static final String METADATA = "metadata";

        private void addChildImplementations() {
            childImplementations.put(READ_ONLY, new PartitionTableParameterExpression(IndexMetaData.INDEX_READ_ONLY_SETTING.getKey()));
            childImplementations.put(READ, new PartitionTableParameterExpression(IndexMetaData.INDEX_BLOCKS_READ_SETTING.getKey()));
            childImplementations.put(WRITE, new PartitionTableParameterExpression(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()));
            childImplementations.put(METADATA, new PartitionTableParameterExpression(IndexMetaData.INDEX_BLOCKS_METADATA_SETTING.getKey()));
        }
    }

    static class PartitionsSettingsRoutingExpression extends ObjectCollectExpression<PartitionInfo> {

        public static final String NAME = "routing";

        PartitionsSettingsRoutingExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(PartitionsSettingsRoutingAllocationExpression.NAME, new PartitionsSettingsRoutingAllocationExpression());
        }
    }

    static class PartitionsSettingsRoutingAllocationExpression extends ObjectCollectExpression<PartitionInfo> {

        public static final String NAME = "allocation";

        PartitionsSettingsRoutingAllocationExpression() {
            addChildImplementations();
        }

        static final String ENABLE = "enable";
        static final String TOTAL_SHARDS_PER_NODE = "total_shards_per_node";

        private void addChildImplementations() {
            childImplementations.put(ENABLE, new StringPartitionTableParameterExpression(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()));
            childImplementations.put(TOTAL_SHARDS_PER_NODE, new PartitionTableParameterExpression(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()));
        }
    }

    static class PartitionsSettingsWarmerExpression extends ObjectCollectExpression<PartitionInfo> {

        public static final String NAME = "warmer";

        PartitionsSettingsWarmerExpression() {
            addChildImplementations();
        }

        static final String ENABLED = "enabled";

        private void addChildImplementations() {
            childImplementations.put(ENABLED, new PartitionTableParameterExpression(IndexSettings.INDEX_WARMER_ENABLED_SETTING.getKey()));
        }
    }

    static class PartitionsSettingsTranslogExpression extends ObjectCollectExpression<PartitionInfo> {

        public static final String NAME = "translog";

        PartitionsSettingsTranslogExpression() {
            addChildImplementations();
        }

        static final String FLUSH_THRESHOLD_SIZE = "flush_threshold_size";
        static final String SYNC_INTERVAL = "sync_interval";
        static final String DURABILITY = "durability";

        private void addChildImplementations() {
            childImplementations.put(FLUSH_THRESHOLD_SIZE, new PartitionTableParameterExpression(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey()));
            childImplementations.put(SYNC_INTERVAL, new PartitionTableParameterExpression(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey()));
            childImplementations.put(DURABILITY, new PartitionTableParameterExpression(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey()));
        }
    }

    static class PartitionsSettingsUnassignedExpression extends ObjectCollectExpression<PartitionInfo> {

        public static final String NAME = "unassigned";

        PartitionsSettingsUnassignedExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(PartitionsSettingsNodeLeftExpression.NAME,
                new PartitionsSettingsNodeLeftExpression());
        }
    }

    static class PartitionsSettingsNodeLeftExpression extends ObjectCollectExpression<PartitionInfo> {

        public static final String NAME = "node_left";

        PartitionsSettingsNodeLeftExpression() {
            addChildImplementations();
        }

        static final String DELAYED_TIMEOUT = "delayed_timeout";

        private void addChildImplementations() {
            childImplementations.put(DELAYED_TIMEOUT, new PartitionTableParameterExpression(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey()));
        }
    }
}
