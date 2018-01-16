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

import org.elasticsearch.monitor.process.ProcessStats;

class NodeProcessCpuStatsExpression extends NestedNodeStatsExpression {

    private static final String PERCENT = "percent";
    private static final String USER = "user";
    private static final String SYSTEM = "system";

    NodeProcessCpuStatsExpression() {
        childImplementations.put(PERCENT, new SimpleNodeStatsExpression<Short>() {
            @Override
            public Short innerValue() {
                ProcessStats.Cpu cpuStats = this.row.processStats().getCpu();
                if (cpuStats != null) {
                    return cpuStats.getPercent();
                } else {
                    return -1;
                }
            }
        });
        childImplementations.put(USER, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                return -1L;
            }
        });
        childImplementations.put(SYSTEM, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                return -1L;
            }
        });
    }
}
