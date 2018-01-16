/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.expression.reference.sys.node;

import io.crate.monitor.ExtendedOsStats;

class NodeOsCpuStatsExpression extends NestedNodeStatsExpression {

    private static final String SYS = "system";
    private static final String USER = "user";
    private static final String IDLE = "idle";
    private static final String USAGE = "used";
    private static final String STOLEN = "stolen";

    NodeOsCpuStatsExpression() {
        childImplementations.put(SYS, new SimpleNodeStatsExpression<Short>() {
            @Override
            public Short innerValue() {
                ExtendedOsStats.Cpu cpu = this.row.extendedOsStats().cpu();
                if (cpu != null) {
                    return cpu.sys();
                } else {
                    return -1;
                }
            }
        });
        childImplementations.put(USER, new SimpleNodeStatsExpression<Short>() {
            @Override
            public Short innerValue() {
                ExtendedOsStats.Cpu cpu = this.row.extendedOsStats().cpu();
                if (cpu != null) {
                    return cpu.user();
                } else {
                    return -1;
                }
            }
        });
        childImplementations.put(IDLE, new SimpleNodeStatsExpression<Short>() {
            @Override
            public Short innerValue() {
                ExtendedOsStats.Cpu cpu = this.row.extendedOsStats().cpu();
                if (cpu != null) {
                    return cpu.idle();
                } else {
                    return -1;
                }
            }
        });
        childImplementations.put(USAGE, new SimpleNodeStatsExpression<Short>() {
            @Override
            public Short innerValue() {
                ExtendedOsStats.Cpu cpu = this.row.extendedOsStats().cpu();
                if (cpu != null) {
                    return cpu.percent();
                } else {
                    return -1;
                }
            }
        });
        childImplementations.put(STOLEN, new SimpleNodeStatsExpression<Short>() {
            @Override
            public Short innerValue() {
                ExtendedOsStats.Cpu cpu = this.row.extendedOsStats().cpu();
                if (cpu != null) {
                    return cpu.stolen();
                } else {
                    return -1;
                }
            }
        });
    }
}
