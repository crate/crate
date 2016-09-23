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

package io.crate.operation.reference.information;

import io.crate.analyze.TableParameterInfo;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.table.TableInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

public class TablesSettingsExpression extends AbstractTablesSettingsExpression {

    public static final String REFRESH_INTERVAL = "refresh_interval";

    public TablesSettingsExpression() {
        addChildImplementations();
    }

    private void addChildImplementations() {

        childImplementations.put(REFRESH_INTERVAL, new TableParameterExpression(TableParameterInfo.REFRESH_INTERVAL));
        childImplementations.put(TablesSettingsBlocksExpression.NAME, new TablesSettingsBlocksExpression());
        childImplementations.put(TablesSettingsRoutingExpression.NAME, new TablesSettingsRoutingExpression());
        childImplementations.put(TablesSettingsRecoveryExpression.NAME, new TablesSettingsRecoveryExpression());
        childImplementations.put(TablesSettingsWarmerExpression.NAME, new TablesSettingsWarmerExpression());
        childImplementations.put(TablesSettingsTranslogExpression.NAME, new TablesSettingsTranslogExpression());
        childImplementations.put(TablesSettingsUnassignedExpression.NAME, new TablesSettingsUnassignedExpression());
    }

    static class TableParameterExpression extends RowContextCollectorExpression<TableInfo, Object> {

        private final String paramName;

        public TableParameterExpression(String paramName) {
            this.paramName = paramName;
        }

        @Override
        public Object value() {
            return row.tableParameters().get(paramName);
        }
    }

    static class BytesRefTableParameterExpression extends RowContextCollectorExpression<TableInfo, BytesRef> {

        private final String paramName;

        public BytesRefTableParameterExpression(String paramName) {
            this.paramName = paramName;
        }

        @Override
        public BytesRef value() {
            return BytesRefs.toBytesRef(row.tableParameters().get(paramName));
        }
    }

    static class TablesSettingsBlocksExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "blocks";
        public static final String READ_ONLY = "read_only";
        public static final String READ = "read";
        public static final String WRITE = "write";
        public static final String METADATA = "metadata";
        public TablesSettingsBlocksExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(READ_ONLY, new TableParameterExpression(TableParameterInfo.READ_ONLY));
            childImplementations.put(READ, new TableParameterExpression(TableParameterInfo.BLOCKS_READ));
            childImplementations.put(WRITE, new TableParameterExpression(TableParameterInfo.BLOCKS_WRITE));
            childImplementations.put(METADATA, new TableParameterExpression(TableParameterInfo.BLOCKS_METADATA));
        }
    }

    static class TablesSettingsRoutingExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "routing";

        public TablesSettingsRoutingExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(TablesSettingsRoutingAllocationExpression.NAME, new TablesSettingsRoutingAllocationExpression());
        }
    }

    static class TablesSettingsRoutingAllocationExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "allocation";
        public static final String ENABLE = "enable";
        public static final String TOTAL_SHARDS_PER_NODE = "total_shards_per_node";
        public TablesSettingsRoutingAllocationExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(ENABLE, new BytesRefTableParameterExpression(TableParameterInfo.ROUTING_ALLOCATION_ENABLE));
            childImplementations.put(TOTAL_SHARDS_PER_NODE, new TableParameterExpression(TableParameterInfo.TOTAL_SHARDS_PER_NODE));
        }
    }

    static class TablesSettingsRecoveryExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "recovery";
        public static final String INITIAL_SHARDS = "initial_shards";

        public TablesSettingsRecoveryExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(INITIAL_SHARDS, new BytesRefTableParameterExpression(TableParameterInfo.RECOVERY_INITIAL_SHARDS));
        }
    }

    static class TablesSettingsWarmerExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "warmer";
        public static final String ENABLED = "enabled";

        public TablesSettingsWarmerExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(ENABLED, new TableParameterExpression(TableParameterInfo.WARMER_ENABLED));
        }
    }

    static class TablesSettingsTranslogExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "translog";
        public static final String FLUSH_THRESHOLD_OPS = "flush_threshold_ops";
        public static final String FLUSH_THRESHOLD_SIZE = "flush_threshold_size";
        public static final String FLUSH_THRESHOLD_PERIOD = "flush_threshold_period";
        public static final String DISABLE_FLUSH = "disable_flush";
        public static final String INTERVAL = "interval";
        public static final String SYNC_INTERVAL = "sync_interval";
        public TablesSettingsTranslogExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(FLUSH_THRESHOLD_OPS, new TableParameterExpression(TableParameterInfo.FLUSH_THRESHOLD_OPS));
            childImplementations.put(FLUSH_THRESHOLD_SIZE, new TableParameterExpression(TableParameterInfo.FLUSH_THRESHOLD_SIZE));
            childImplementations.put(FLUSH_THRESHOLD_PERIOD, new TableParameterExpression(TableParameterInfo.FLUSH_THRESHOLD_PERIOD));
            childImplementations.put(DISABLE_FLUSH, new TableParameterExpression(TableParameterInfo.FLUSH_DISABLE));
            childImplementations.put(INTERVAL, new TableParameterExpression(TableParameterInfo.TRANSLOG_INTERVAL));
            childImplementations.put(SYNC_INTERVAL, new TableParameterExpression(TableParameterInfo.TRANSLOG_SYNC_INTERVAL));
        }
    }

    static class TablesSettingsUnassignedExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "unassigned";

        public TablesSettingsUnassignedExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(TablesSettingsNodeLeftExpression.NAME,
                new TablesSettingsNodeLeftExpression());
        }
    }

    static class TablesSettingsNodeLeftExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "node_left";
        public static final String DELAYED_TIMEOUT = "delayed_timeout";

        public TablesSettingsNodeLeftExpression() {
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(DELAYED_TIMEOUT, new TableParameterExpression(TableParameterInfo.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT));
        }
    }
}

