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
import io.crate.metadata.information.InformationPartitionsTableInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

public class PartitionsSettingsExpression extends AbstractPartitionsSettingsExpression {

    private static final BytesRef EMPTY_BYTES_REF = new BytesRef();
    public static final String NAME = "settings";

    public PartitionsSettingsExpression() {
        super(InformationPartitionsTableInfo.ReferenceInfos.TABLE_SETTINGS);
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(PartitionsSettingsBlocksExpression.NAME, new PartitionsSettingsBlocksExpression());
        childImplementations.put(PartitionsSettingsRoutingExpression.NAME, new PartitionsSettingsRoutingExpression());
        childImplementations.put(PartitionsSettingsRecoveryExpression.NAME, new PartitionsSettingsRecoveryExpression());
        childImplementations.put(PartitionsSettingsWarmerExpression.NAME, new PartitionsSettingsWarmerExpression());
        childImplementations.put(PartitionsSettingsTranslogExpression.NAME, new PartitionsSettingsTranslogExpression());
        childImplementations.put(PartitionsSettingsGatewayExpression.NAME, new PartitionsSettingsGatewayExpression());
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


    static class PartitionsSettingsBlocksExpression extends AbstractPartitionsSettingsExpression {

        public static final String NAME = "blocks";

        public PartitionsSettingsBlocksExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.TABLE_SETTINGS_BLOCKS);
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

    static class PartitionsSettingsRoutingExpression extends AbstractPartitionsSettingsExpression {

        public static final String NAME = "routing";

        public PartitionsSettingsRoutingExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.TABLE_SETTINGS_ROUTING);
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(PartitionsSettingsRoutingAllocationExpression.NAME, new PartitionsSettingsRoutingAllocationExpression());
        }
    }

    static class PartitionsSettingsRoutingAllocationExpression extends AbstractPartitionsSettingsExpression {

        public static final String NAME = "allocation";

        public PartitionsSettingsRoutingAllocationExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.TABLE_SETTINGS_ROUTING_ALLOCATION);
            addChildImplementations();
        }

        public static final String ENABLE = "enable";
        public static final String TOTAL_SHARDS_PER_NODE="total_shards_per_node";

        private void addChildImplementations() {
            childImplementations.put(ENABLE, new BytesRefPartitionTableParameterExpression(TableParameterInfo.ROUTING_ALLOCATION_ENABLE));
            childImplementations.put(TOTAL_SHARDS_PER_NODE, new PartitionTableParameterExpression(TableParameterInfo.TOTAL_SHARDS_PER_NODE));
        }
    }

    static class PartitionsSettingsRecoveryExpression extends AbstractPartitionsSettingsExpression {

        public static final String NAME = "recovery";

        public PartitionsSettingsRecoveryExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.TABLE_SETTINGS_RECOVERY);
            addChildImplementations();
        }

        public static final String INITIAL_SHARDS = "initial_shards";

        private void addChildImplementations() {
            childImplementations.put(INITIAL_SHARDS, new PartitionTableParameterExpression(TableParameterInfo.RECOVERY_INITIAL_SHARDS));
        }
    }

    static class PartitionsSettingsWarmerExpression extends AbstractPartitionsSettingsExpression {

        public static final String NAME = "warmer";

        public PartitionsSettingsWarmerExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.TABLE_SETTINGS_WARMER);
            addChildImplementations();
        }

        public static final String ENABLED = "enabled";

        private void addChildImplementations() {
            childImplementations.put(ENABLED, new PartitionTableParameterExpression(TableParameterInfo.WARMER_ENABLED));
        }
    }

    static class PartitionsSettingsGatewayExpression extends AbstractPartitionsSettingsExpression {

        public static final String NAME = "gateway";

        public PartitionsSettingsGatewayExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.TABLE_SETTINGS_GATEWAY);
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(PartitionsSettingsGatewayLocalExpression.NAME, new PartitionsSettingsGatewayLocalExpression());
        }

    }

    static class PartitionsSettingsGatewayLocalExpression extends AbstractPartitionsSettingsExpression {

        public static final String NAME = "local";

        public PartitionsSettingsGatewayLocalExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.TABLE_SETTINGS_GATEWAY_LOCAL);
            addChildImplementations();
        }

        public static final String SYNC = "sync";

        private void addChildImplementations() {
            childImplementations.put(SYNC, new PartitionTableParameterExpression(TableParameterInfo.GATEWAY_LOCAL_SYNC));
        }
    }
    
    static class PartitionsSettingsTranslogExpression extends AbstractPartitionsSettingsExpression {

        public static final String NAME = "translog";

        public PartitionsSettingsTranslogExpression() {
            super(InformationPartitionsTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG);
            addChildImplementations();
        }

        public static final String FLUSH_THRESHOLD_OPS = "flush_threshold_ops";
        public static final String FLUSH_THRESHOLD_SIZE = "flush_threshold_size";
        public static final String FLUSH_THRESHOLD_PERIOD = "flush_threshold_period";
        public static final String DISABLE_FLUSH = "disable_flush";
        public static final String INTERVAL = "interval";

        private void addChildImplementations() {
            childImplementations.put(FLUSH_THRESHOLD_OPS, new PartitionTableParameterExpression(TableParameterInfo.FLUSH_THRESHOLD_OPS));
            childImplementations.put(FLUSH_THRESHOLD_SIZE, new PartitionTableParameterExpression(TableParameterInfo.FLUSH_THRESHOLD_SIZE));
            childImplementations.put(FLUSH_THRESHOLD_PERIOD, new PartitionTableParameterExpression(TableParameterInfo.FLUSH_THRESHOLD_PERIOD));
            childImplementations.put(DISABLE_FLUSH, new PartitionTableParameterExpression(TableParameterInfo.FLUSH_DISABLE));
            childImplementations.put(INTERVAL, new PartitionTableParameterExpression(TableParameterInfo.TRANSLOG_INTERVAL));
        }
    }
}
