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

import io.crate.analyze.TableParameterInfo;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.ObjectCollectExpression;
import io.crate.metadata.PartitionInfo;

public class PartitionsSettingsExpression extends ObjectCollectExpression<PartitionInfo> {

    public PartitionsSettingsExpression() {
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(PartitionsSettingsBlocksExpression.NAME, new PartitionsSettingsBlocksExpression());
        childImplementations.put(PartitionSettingsMappingExpression.NAME, new PartitionSettingsMappingExpression());
        childImplementations.put(PartitionsSettingsRoutingExpression.NAME, new PartitionsSettingsRoutingExpression());
        childImplementations.put(PartitionsSettingsWarmerExpression.NAME, new PartitionsSettingsWarmerExpression());
        childImplementations.put(PartitionsSettingsTranslogExpression.NAME, new PartitionsSettingsTranslogExpression());
        childImplementations.put(PartitionsSettingsUnassignedExpression.NAME, new PartitionsSettingsUnassignedExpression());
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
            childImplementations.put(LIMIT, new PartitionTableParameterExpression(TableParameterInfo.MAPPING_TOTAL_FIELDS_LIMIT.getKey()));
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
            childImplementations.put(READ_ONLY, new PartitionTableParameterExpression(TableParameterInfo.READ_ONLY.getKey()));
            childImplementations.put(READ, new PartitionTableParameterExpression(TableParameterInfo.BLOCKS_READ.getKey()));
            childImplementations.put(WRITE, new PartitionTableParameterExpression(TableParameterInfo.BLOCKS_WRITE.getKey()));
            childImplementations.put(METADATA, new PartitionTableParameterExpression(TableParameterInfo.BLOCKS_METADATA.getKey()));
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
            childImplementations.put(ENABLE, new StringPartitionTableParameterExpression(TableParameterInfo.ROUTING_ALLOCATION_ENABLE.getKey()));
            childImplementations.put(TOTAL_SHARDS_PER_NODE, new PartitionTableParameterExpression(TableParameterInfo.TOTAL_SHARDS_PER_NODE.getKey()));
        }
    }

    static class PartitionsSettingsWarmerExpression extends ObjectCollectExpression<PartitionInfo> {

        public static final String NAME = "warmer";

        PartitionsSettingsWarmerExpression() {
            addChildImplementations();
        }

        static final String ENABLED = "enabled";

        private void addChildImplementations() {
            childImplementations.put(ENABLED, new PartitionTableParameterExpression(TableParameterInfo.WARMER_ENABLED.getKey()));
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
            childImplementations.put(FLUSH_THRESHOLD_SIZE, new PartitionTableParameterExpression(TableParameterInfo.FLUSH_THRESHOLD_SIZE.getKey()));
            childImplementations.put(SYNC_INTERVAL, new PartitionTableParameterExpression(TableParameterInfo.TRANSLOG_SYNC_INTERVAL.getKey()));
            childImplementations.put(DURABILITY, new PartitionTableParameterExpression(TableParameterInfo.TRANSLOG_DURABILITY.getKey()));
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
            childImplementations.put(DELAYED_TIMEOUT, new PartitionTableParameterExpression(TableParameterInfo.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT.getKey()));
        }
    }
}
