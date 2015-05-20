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
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.information.InformationTablesTableInfo;

public class TablesSettingsExpression extends AbstractTablesSettingsExpression {

    public static final String NAME = "settings";

    public TablesSettingsExpression() {
        super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS);
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(TablesSettingsBlocksExpression.NAME, new TablesSettingsBlocksExpression());
        childImplementations.put(TablesSettingsRoutingExpression.NAME, new TablesSettingsRoutingExpression());
        childImplementations.put(TablesSettingsRecoveryExpression.NAME, new TablesSettingsRecoveryExpression());
        childImplementations.put(TablesSettingsWarmerExpression.NAME, new TablesSettingsWarmerExpression());
        childImplementations.put(TablesSettingsTranslogExpression.NAME, new TablesSettingsTranslogExpression());
        childImplementations.put(TablesSettingsGatewayExpression.NAME, new TablesSettingsGatewayExpression());
    }


    static class TablesSettingsBlocksExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "blocks";

        public TablesSettingsBlocksExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_BLOCKS);
            addChildImplementations();
        }

        public static final String READ_ONLY = "read_only";
        public static final String READ = "read";
        public static final String WRITE = "write";
        public static final String METADATA = "metadata";

        private void addChildImplementations() {
            childImplementations.put(READ_ONLY,
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_BLOCKS_READ_ONLY) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.READ_ONLY);
                        }
                    });
            childImplementations.put(READ,
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_BLOCKS_READ) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.BLOCKS_READ);
                        }
                    });
            childImplementations.put(WRITE,
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_BLOCKS_WRITE) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.BLOCKS_WRITE);
                        }
                    });
            childImplementations.put(METADATA,
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_BLOCKS_METADATA) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.BLOCKS_METADATA);
                        }
                    });
        }
    }

    static class TablesSettingsRoutingExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "routing";

        public TablesSettingsRoutingExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_ROUTING);
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(TablesSettingsRoutingAllocationExpression.NAME, new TablesSettingsRoutingAllocationExpression());
        }
    }

    static class TablesSettingsRoutingAllocationExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "allocation";

        public TablesSettingsRoutingAllocationExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_ROUTING_ALLOCATION);
            addChildImplementations();
        }

        public static final String ENABLE = "enable";
        public static final String TOTAL_SHARDS_PER_NODE="total_shards_per_node";

        private void addChildImplementations() {
            childImplementations.put(ENABLE,
                    new InformationTablesExpression<String>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE) {
                        @Override
                        public String value() {
                            return (String) this.row.tableParameters().get(TableParameterInfo.ROUTING_ALLOCATION_ENABLE);
                        }
                    });
            childImplementations.put(TOTAL_SHARDS_PER_NODE,
                    new InformationTablesExpression<Integer>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE) {
                        @Override
                        public Integer value() {
                            return (Integer) this.row.tableParameters().get(TableParameterInfo.TOTAL_SHARDS_PER_NODE);
                        }
                    });
        }
    }

    static class TablesSettingsRecoveryExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "recovery";

        public TablesSettingsRecoveryExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_RECOVERY);
            addChildImplementations();
        }

        public static final String INITIAL_SHARDS = "initial_shards";

        private void addChildImplementations() {
            childImplementations.put(INITIAL_SHARDS,
                    new InformationTablesExpression<String>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_RECOVERY_INITIAL_SHARDS) {
                        @Override
                        public String value() {
                            return (String) this.row.tableParameters().get(TableParameterInfo.RECOVERY_INITIAL_SHARDS);
                        }
                    });
        }
    }

    static class TablesSettingsWarmerExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "warmer";

        public TablesSettingsWarmerExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_WARMER);
            addChildImplementations();
        }

        public static final String ENABLED = "enabled";

        private void addChildImplementations() {
            childImplementations.put(ENABLED,
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_WARMER_ENABLED) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.WARMER_ENABLED);
                        }
                    });
        }
    }

    static class TablesSettingsGatewayExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "gateway";

        public TablesSettingsGatewayExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_GATEWAY);
            addChildImplementations();
        }


        private void addChildImplementations() {
            childImplementations.put(TablesSettingsGatewayLocalExpression.NAME, new TablesSettingsGatewayLocalExpression());
        }

    }

    static class TablesSettingsGatewayLocalExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "local";

        public TablesSettingsGatewayLocalExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_GATEWAY_LOCAL);
            addChildImplementations();
        }

        public static final String SYNC = "sync";

        private void addChildImplementations() {
            childImplementations.put(SYNC,
                    new InformationTablesExpression<String>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_GATEWAY_LOCAL_SYNC) {
                        @Override
                        public String value() {
                            return (String) this.row.tableParameters().get(TableParameterInfo.GATEWAY_LOCAL_SYNC);
                        }
                    });
        }
    }

    static class TablesSettingsTranslogExpression extends AbstractTablesSettingsExpression {

        public static final String NAME = "translog";

        public TablesSettingsTranslogExpression() {
            super(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG);
            addChildImplementations();
        }

        public static final String FLUSH_THRESHOLD_OPS = "flush_threshold_ops";
        public static final String FLUSH_THRESHOLD_SIZE = "flush_threshold_size";
        public static final String FLUSH_THRESHOLD_PERIOD = "flush_threshold_period";
        public static final String DISABLE_FLUSH = "disable_flush";
        public static final String INTERVAL = "interval";

        private void addChildImplementations() {
            childImplementations.put(FLUSH_THRESHOLD_OPS,
                    new InformationTablesExpression<Integer>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_OPS) {
                        @Override
                        public Integer value() {
                            return (Integer) this.row.tableParameters().get(TableParameterInfo.FLUSH_THRESHOLD_OPS);
                        }
                    });
            childImplementations.put(FLUSH_THRESHOLD_SIZE,
                    new InformationTablesExpression<String>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE) {
                        @Override
                        public String value() {
                            return (String) this.row.tableParameters().get(TableParameterInfo.FLUSH_THRESHOLD_SIZE);
                        }
                    });
            childImplementations.put(FLUSH_THRESHOLD_PERIOD,
                    new InformationTablesExpression<String>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_PERIOD) {
                        @Override
                        public String value() {
                            return (String) this.row.tableParameters().get(TableParameterInfo.FLUSH_THRESHOLD_PERIOD);
                        }
                    });
            childImplementations.put(DISABLE_FLUSH,
                    new InformationTablesExpression<Boolean>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG_DISABLE_FLUSH) {
                        @Override
                        public Boolean value() {
                            return (Boolean) this.row.tableParameters().get(TableParameterInfo.FLUSH_DISABLE);
                        }
                    });
            childImplementations.put(INTERVAL,
                    new InformationTablesExpression<String>(InformationTablesTableInfo.ReferenceInfos.TABLE_SETTINGS_TRANSLOG_INTERVAL) {
                        @Override
                        public String value() {
                            return (String) this.row.tableParameters().get(TableParameterInfo.TRANSLOG_INTERVAL);
                        }
                    });
        }
    }
}
