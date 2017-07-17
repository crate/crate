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

package io.crate.operation.reference.partitioned;

import io.crate.analyze.TableParameterInfo;
import io.crate.metadata.PartitionInfo;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.operation.reference.RowCollectNestedObjectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

public class PartitionsSettingsExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

    public PartitionsSettingsExpression() {
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(PartitionsSettingsBlocksExpression.NAME, new PartitionsSettingsBlocksExpression());
        childImplementations.put(PartitionSettingsMappingExpression.NAME, new PartitionSettingsMappingExpression());
        childImplementations.put(PartitionsSettingsRoutingExpression.NAME, new PartitionsSettingsRoutingExpression());
        childImplementations.put(PartitionsSettingsRecoveryExpression.NAME, new PartitionsSettingsRecoveryExpression());
        childImplementations.put(PartitionsSettingsWarmerExpression.NAME, new PartitionsSettingsWarmerExpression());
        childImplementations.put(PartitionsSettingsTranslogExpression.NAME, new PartitionsSettingsTranslogExpression());
        childImplementations.put(PartitionsSettingsUnassignedExpression.NAME, new PartitionsSettingsUnassignedExpression());
    }

    static class PartitionTableParameterExpression extends RowContextCollectorExpression<PartitionInfo, Object> {

        private final String paramName;

        public PartitionTableParameterExpression(String paramName) {
            this.paramName = paramName;
        }

        @Override
        public Object value() {
            return row.tableParameters().get(paramName);
        }
    }

    static class BytesRefPartitionTableParameterExpression extends RowContextCollectorExpression<PartitionInfo, BytesRef> {

        private final String paramName;

        public BytesRefPartitionTableParameterExpression(String paramName) {
            this.paramName = paramName;
        }

        @Override
        public BytesRef value() {
            return BytesRefs.toBytesRef(row.tableParameters().get(paramName));
        }
    }

    static class PartitionSettingsMappingExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

        public static final String NAME = "mapping";

        public PartitionSettingsMappingExpression() {
            childImplementations.put(PartitionSettingsMappingTotalFieldsExpression.NAME, new PartitionSettingsMappingTotalFieldsExpression());
        }
    }

    static class PartitionSettingsMappingTotalFieldsExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

        public static final String NAME = "total_fields";
        public static final String LIMIT = "limit";

        public PartitionSettingsMappingTotalFieldsExpression() {
            childImplementations.put(LIMIT, new PartitionTableParameterExpression(TableParameterInfo.MAPPING_TOTAL_FIELDS_LIMIT));
        }
    }

    static class PartitionsSettingsBlocksExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

        public static final String NAME = "blocks";

        public PartitionsSettingsBlocksExpression() {
            addChildImplementations();
        }

        public static final String READ_ONLY = "read_only";
        public static final String READ = "read";
        public static final String WRITE = "write";
        public static final String METADATA = "metadata";

        private void addChildImplementations() {
            childImplementations.put(READ_ONLY, new PartitionTableParameterExpression(TableParameterInfo.READ_ONLY));
            childImplementations.put(READ, new PartitionTableParameterExpression(TableParameterInfo.BLOCKS_READ));
            childImplementations.put(WRITE, new PartitionTableParameterExpression(TableParameterInfo.BLOCKS_WRITE));
            childImplementations.put(METADATA, new PartitionTableParameterExpression(TableParameterInfo.BLOCKS_METADATA));
        }
    }

    static class PartitionsSettingsRoutingExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

        public static final String NAME = "routing";

        public PartitionsSettingsRoutingExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(PartitionsSettingsRoutingAllocationExpression.NAME, new PartitionsSettingsRoutingAllocationExpression());
        }
    }

    static class PartitionsSettingsRoutingAllocationExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

        public static final String NAME = "allocation";

        public PartitionsSettingsRoutingAllocationExpression() {
            addChildImplementations();
        }

        public static final String ENABLE = "enable";
        public static final String TOTAL_SHARDS_PER_NODE = "total_shards_per_node";

        private void addChildImplementations() {
            childImplementations.put(ENABLE, new BytesRefPartitionTableParameterExpression(TableParameterInfo.ROUTING_ALLOCATION_ENABLE));
            childImplementations.put(TOTAL_SHARDS_PER_NODE, new PartitionTableParameterExpression(TableParameterInfo.TOTAL_SHARDS_PER_NODE));
        }
    }

    static class PartitionsSettingsRecoveryExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

        public static final String NAME = "recovery";

        public PartitionsSettingsRecoveryExpression() {
            addChildImplementations();
        }

        public static final String INITIAL_SHARDS = "initial_shards";

        private void addChildImplementations() {
            childImplementations.put(INITIAL_SHARDS, new PartitionTableParameterExpression(TableParameterInfo.RECOVERY_INITIAL_SHARDS));
        }
    }

    static class PartitionsSettingsWarmerExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

        public static final String NAME = "warmer";

        public PartitionsSettingsWarmerExpression() {
            addChildImplementations();
        }

        public static final String ENABLED = "enabled";

        private void addChildImplementations() {
            childImplementations.put(ENABLED, new PartitionTableParameterExpression(TableParameterInfo.WARMER_ENABLED));
        }
    }

    static class PartitionsSettingsTranslogExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

        public static final String NAME = "translog";

        public PartitionsSettingsTranslogExpression() {
            addChildImplementations();
        }

        public static final String FLUSH_THRESHOLD_SIZE = "flush_threshold_size";
        public static final String SYNC_INTERVAL = "sync_interval";
        public static final String DURABILITY = "durability";

        private void addChildImplementations() {
            childImplementations.put(FLUSH_THRESHOLD_SIZE, new PartitionTableParameterExpression(TableParameterInfo.FLUSH_THRESHOLD_SIZE));
            childImplementations.put(SYNC_INTERVAL, new PartitionTableParameterExpression(TableParameterInfo.TRANSLOG_SYNC_INTERVAL));
            childImplementations.put(DURABILITY, new PartitionTableParameterExpression(TableParameterInfo.TRANSLOG_DURABILITY));
        }
    }

    static class PartitionsSettingsUnassignedExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

        public static final String NAME = "unassigned";

        public PartitionsSettingsUnassignedExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(PartitionsSettingsNodeLeftExpression.NAME,
                new PartitionsSettingsNodeLeftExpression());
        }
    }

    static class PartitionsSettingsNodeLeftExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

        public static final String NAME = "node_left";

        public PartitionsSettingsNodeLeftExpression() {
            addChildImplementations();
        }

        public static final String DELAYED_TIMEOUT = "delayed_timeout";

        private void addChildImplementations() {
            childImplementations.put(DELAYED_TIMEOUT, new PartitionTableParameterExpression(TableParameterInfo.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT));
        }
    }

}
