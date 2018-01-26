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

package io.crate.expression.reference.sys.node.local;

import io.crate.metadata.ReferenceImplementation;
import io.crate.expression.reference.NestedObjectExpression;
import org.elasticsearch.monitor.process.ProcessStats;

class NodeProcessCpuExpression extends NestedObjectExpression {

    private static final String PERCENT = "percent";
    private static final String USER = "user";
    private static final String SYSTEM = "system";

    NodeProcessCpuExpression(final ProcessStats.Cpu cpuStats) {
        addChildImplementations(cpuStats);
    }

    private void addChildImplementations(final ProcessStats.Cpu cpuStats) {
        childImplementations.put(PERCENT, (ReferenceImplementation<Short>) () -> {
            if (cpuStats != null) {
                return cpuStats.getPercent();
            } else {
                return (short) -1;
            }
        });
        childImplementations.put(USER, (ReferenceImplementation<Long>) () -> -1L);
        childImplementations.put(SYSTEM, (ReferenceImplementation<Long>) () -> -1L);
    }
}
