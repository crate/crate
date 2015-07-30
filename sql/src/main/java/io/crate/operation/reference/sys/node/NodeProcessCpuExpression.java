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
import org.elasticsearch.monitor.process.ProcessStats;

public class NodeProcessCpuExpression extends SysNodeObjectReference {

    public static final String NAME = "cpu";
    public static final String PERCENT = "percent";
    public static final String USER = "user";
    public static final String SYSTEM = "system";

    public NodeProcessCpuExpression(ProcessStats stats) {
        addChildImplementations(stats.cpu());
    }

    private void addChildImplementations(final ProcessStats.Cpu cpu) {
        childImplementations.put(PERCENT, new SysNodeExpression<Short>() {
            @Override
            public Short value() {
                if (cpu != null) {
                    return cpu.getPercent();
                } else {
                    return -1;
                }
            }
        });
        childImplementations.put(USER, new SysNodeExpression<Long>() {
            @Override
            public Long value() {
                if (cpu != null) {
                    return cpu.getUser().millis();
                } else {
                    return -1L;
                }
            }
        });
        childImplementations.put(SYSTEM, new SysNodeExpression<Long>() {
            @Override
            public Long value() {
                if (cpu != null) {
                    return cpu.getSys().millis();
                } else {
                    return -1L;
                }
            }
        });
    }
}
