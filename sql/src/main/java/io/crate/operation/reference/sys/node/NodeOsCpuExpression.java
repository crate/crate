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

package io.crate.operation.reference.sys.node;

import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.monitor.os.OsStats;

public class NodeOsCpuExpression extends SysNodeObjectReference {

    public static final String NAME = "cpu";

    abstract class CpuExpression extends SysNodeExpression<Object> {
    }

    public static final String SYS = "system";
    public static final String USER = "user";
    public static final String IDLE = "idle";
    public static final String USAGE = "used";
    public static final String STOLEN = "stolen";

    @Inject
    public NodeOsCpuExpression(OsStats stats) {
        OsStats.Cpu cpu = stats.cpu();
        if (cpu == null) {
            childImplementations.put(SYS, UNKNOWN_VALUE_EXPRESSION);
            childImplementations.put(USER, UNKNOWN_VALUE_EXPRESSION);
            childImplementations.put(IDLE, UNKNOWN_VALUE_EXPRESSION);
            childImplementations.put(USAGE, UNKNOWN_VALUE_EXPRESSION);
            childImplementations.put(STOLEN, UNKNOWN_VALUE_EXPRESSION);
        } else {
            addChildImplementations(cpu);
        }
    }

    private void addChildImplementations(final OsStats.Cpu cpu) {
        childImplementations.put(SYS, new CpuExpression() {
            @Override
            public Short value() {
                return cpu.sys();
            }
        });
        childImplementations.put(USER, new CpuExpression() {
            @Override
            public Short value() {
                return cpu.user();
            }
        });
        childImplementations.put(IDLE, new CpuExpression() {
            @Override
            public Short value() {
                return cpu.idle();
            }
        });
        childImplementations.put(USAGE, new CpuExpression() {
            @Override
            public Short value() {
                return (short) (cpu.sys() + cpu.user());
            }
        });
        childImplementations.put(STOLEN, new CpuExpression() {
            @Override
            public Short value() {
                return cpu.stolen();
            }
        });
    }

}
