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

import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.elasticsearch.monitor.os.OsStats;

public class NodeMemoryExpression extends SysNodeObjectReference {

    abstract class MemoryExpression extends SysNodeExpression<Object> {
    }

    public static final String NAME = "mem";

    public static final String FREE = "free";
    public static final String USED = "used";
    public static final String FREE_PERCENT = "free_percent";
    public static final String USED_PERCENT = "used_percent";
    public static final String PROBE_TIMESTAMP = "probe_timestamp";

    public NodeMemoryExpression(final OsStats stats) {
        OsStats.Mem mem = stats.mem();
        if (mem == null) {
            childImplementations.put(FREE, UNKNOWN_VALUE_EXPRESSION);
            childImplementations.put(USED, UNKNOWN_VALUE_EXPRESSION);
            childImplementations.put(FREE_PERCENT, UNKNOWN_VALUE_EXPRESSION);
            childImplementations.put(USED_PERCENT, UNKNOWN_VALUE_EXPRESSION);
        } else {
            addChildImplementations(mem);
        }
        childImplementations.put(PROBE_TIMESTAMP, new SysNodeExpression<Long>() {
            @Override
            public Long value() {
                return stats.timestamp();
            }
        });
    }

    private void addChildImplementations(final OsStats.Mem mem) {
        childImplementations.put(FREE, new MemoryExpression() {
            @Override
            public Long value() {
                return mem.actualFree().bytes();
            }
        });
        childImplementations.put(USED, new MemoryExpression() {
            @Override
            public Long value() {
                return mem.actualUsed().bytes();
            }
        });
        childImplementations.put(FREE_PERCENT, new MemoryExpression() {
            @Override
            public Short value() {
                return mem.freePercent();
            }
        });
        childImplementations.put(USED_PERCENT, new MemoryExpression() {
            @Override
            public Short value() {
                return mem.usedPercent();
            }
        });
    }

}
