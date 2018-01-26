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

import org.elasticsearch.monitor.jvm.JvmStats;

public class NodeHeapStatsExpression extends NestedNodeStatsExpression {

    private static final String MAX = "max";
    private static final String FREE = "free";
    private static final String USED = "used";
    private static final String PROBE_TIMESTAMP = "probe_timestamp";

    public NodeHeapStatsExpression() {
        childImplementations.put(FREE, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                JvmStats stats = this.row.jvmStats();
                JvmStats.Mem mem = stats.getMem();
                return mem.getHeapMax().getBytes() - mem.getHeapUsed().getBytes();
            }
        });
        childImplementations.put(USED, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                JvmStats stats = this.row.jvmStats();
                return stats.getMem().getHeapUsed().getBytes();
            }
        });
        childImplementations.put(MAX, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                JvmStats stats = this.row.jvmStats();
                return stats.getMem().getHeapMax().getBytes();
            }
        });
        childImplementations.put(PROBE_TIMESTAMP, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                JvmStats stats = this.row.jvmStats();
                return stats.getTimestamp();
            }
        });
    }
}
