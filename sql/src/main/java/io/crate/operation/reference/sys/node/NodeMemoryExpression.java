/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.sys.node;

import org.elasticsearch.monitor.os.OsStats;

public class NodeMemoryExpression extends NestedDiscoveryNodeExpression {

    abstract class MemoryExpression extends SimpleDiscoveryNodeExpression<Object> {
    }

    private static final String FREE = "free";
    private static final String USED = "used";
    private static final String FREE_PERCENT = "free_percent";
    private static final String USED_PERCENT = "used_percent";
    private static final String PROBE_TIMESTAMP = "probe_timestamp";

    public NodeMemoryExpression() {
        childImplementations.put(FREE, new MemoryExpression() {
            @Override
            public Long value() {
                OsStats.Mem mem = this.row.osStats.getMem();
                if (mem != null) {
                    return mem.getFree().bytes();
                }
                return -1L;
            }
        });
        childImplementations.put(USED, new MemoryExpression() {
            @Override
            public Long value() {
                OsStats.Mem mem = this.row.osStats.getMem();
                if (mem != null) {
                    return mem.getUsed().bytes();
                }
                return -1L;
            }
        });
        childImplementations.put(FREE_PERCENT, new MemoryExpression() {
            @Override
            public Short value() {
                OsStats.Mem mem = this.row.osStats.getMem();
                if (mem != null) {
                    return mem.getFreePercent();
                }
                return -1;
            }
        });
        childImplementations.put(USED_PERCENT, new MemoryExpression() {
            @Override
            public Short value() {
                OsStats.Mem mem = this.row.osStats.getMem();
                if (mem != null) {
                    return mem.getUsedPercent();
                }
                return -1;
            }
        });
        childImplementations.put(PROBE_TIMESTAMP, new SimpleDiscoveryNodeExpression<Long>() {
            @Override
            public Long value() {
                return this.row.osStats.getTimestamp();
            }
        });
    }

}
