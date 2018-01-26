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

package io.crate.expression.reference.sys.node;

public class NodeOsStatsExpression extends NestedNodeStatsExpression {

    private static final String UPTIME = "uptime";
    private static final String TIMESTAMP = "timestamp";
    private static final String PROBE_TIMESTAMP = "probe_timestamp";
    private static final String CPU = "cpu";
    private static final String CGROUP = "cgroup";

    public NodeOsStatsExpression() {
        childImplementations.put(UPTIME, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                long uptime = this.row.extendedOsStats().uptime().millis();
                return uptime > 0 ? uptime : -1;
            }
        });
        childImplementations.put(TIMESTAMP, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                assert this.row.timestamp() > 0 : "os timestamp must always be greater than 0.";
                return this.row.timestamp();
            }
        });
        childImplementations.put(PROBE_TIMESTAMP, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                return this.row.extendedOsStats().timestamp();
            }
        });
        childImplementations.put(CPU, new NodeOsCpuStatsExpression());
        childImplementations.put(CGROUP, new NodeOsCgroupStatsExpression());
    }
}
