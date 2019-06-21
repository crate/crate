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

import io.crate.execution.engine.collect.NestableCollectExpression;
import org.elasticsearch.monitor.os.OsStats;

import java.util.function.Function;

final class CgroupExpression<R> implements NestableCollectExpression<NodeStatsContext, R> {

    private final Function<OsStats.Cgroup, R> getter;
    private R value;

    private CgroupExpression(Function<OsStats.Cgroup, R> getter) {
        this.getter = getter;
    }

    @Override
    public void setNextRow(NodeStatsContext nodeStatsContext) {
        value = null;
        if (nodeStatsContext.isComplete()) {
            OsStats.Cgroup cgroup = nodeStatsContext.extendedOsStats().osStats().getCgroup();
            if (cgroup != null) {
                value = getter.apply(cgroup);
            }
        }
    }

    @Override
    public R value() {
        return value;
    }

    static <R> CgroupExpression<R> forAttribute(Function<OsStats.Cgroup, R> attribute) {
        return new CgroupExpression<>(attribute);
    }
}
