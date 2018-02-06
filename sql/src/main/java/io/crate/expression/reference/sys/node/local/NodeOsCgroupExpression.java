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

import io.crate.expression.NestableInput;
import io.crate.monitor.ExtendedOsStats;
import io.crate.expression.reference.NestedObjectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.monitor.os.OsStats;

public class NodeOsCgroupExpression extends NestedObjectExpression {

    private static final String CPUACCT = "cpuacct";
    private static final String CPU = "cpu";
    private static final String MEM = "mem";

    NodeOsCgroupExpression(ExtendedOsStats extendedOsStats) {
        OsStats.Cgroup cgroup = extendedOsStats.osStats().getCgroup();
        childImplementations.put(CPUACCT, new NodeOsCgroupCpuAcctExpression(cgroup));
        childImplementations.put(CPU, new NodeOsCgroupCpuExpression(cgroup));
        childImplementations.put(MEM, new NodeOsCgroupMemExpression(cgroup));
    }

    private class NodeOsCgroupCpuAcctExpression extends NestedObjectExpression {

        private static final String CONTROL_GROUP = "control_group";
        private static final String USAGE_NANOS = "usage_nanos";

        NodeOsCgroupCpuAcctExpression(OsStats.Cgroup cgroup) {
            childImplementations.put(CONTROL_GROUP, (NestableInput<BytesRef>) () -> {
                if (cgroup != null) {
                    return BytesRefs.toBytesRef(cgroup.getCpuAcctControlGroup());
                }
                return null;
            });
            childImplementations.put(USAGE_NANOS, (NestableInput<Long>) () -> {
                if (cgroup != null) {
                    return cgroup.getCpuAcctUsageNanos();
                }
                return null;
            });
        }
    }

    private class NodeOsCgroupCpuExpression extends NestedObjectExpression {

        private static final String CONTROL_GROUP = "control_group";
        private static final String CFS_PERIOD_MICROS = "cfs_period_micros";
        private static final String CFS_QUOTA_MICROS = "cfs_quota_micros";
        private static final String NUM_ELAPSED_PERIODS = "num_elapsed_periods";
        private static final String NUM_TIMES_THROTTLED = "num_times_throttled";
        private static final String TIME_THROTTLED_NANOS = "time_throttled_nanos";

        NodeOsCgroupCpuExpression(OsStats.Cgroup cgroup) {
            childImplementations.put(CONTROL_GROUP, (NestableInput<BytesRef>) () -> {
                if (cgroup != null) {
                    return BytesRefs.toBytesRef(cgroup.getCpuControlGroup());
                }
                return null;
            });
            childImplementations.put(CFS_PERIOD_MICROS, (NestableInput<Long>) () -> {
                if (cgroup != null) {
                    return cgroup.getCpuCfsPeriodMicros();
                }
                return null;
            });
            childImplementations.put(CFS_QUOTA_MICROS, (NestableInput<Long>) () -> {
                if (cgroup != null) {
                    return cgroup.getCpuCfsQuotaMicros();
                }
                return null;
            });
            childImplementations.put(NUM_ELAPSED_PERIODS, (NestableInput<Long>) () -> {
                if (cgroup != null) {
                    return cgroup.getCpuStat().getNumberOfElapsedPeriods();
                }
                return null;
            });
            childImplementations.put(NUM_TIMES_THROTTLED, (NestableInput<Long>) () -> {
                if (cgroup != null) {
                    return cgroup.getCpuStat().getNumberOfTimesThrottled();
                }
                return null;
            });
            childImplementations.put(TIME_THROTTLED_NANOS, (NestableInput<Long>) () -> {
                if (cgroup != null) {
                    return cgroup.getCpuStat().getTimeThrottledNanos();
                }
                return null;
            });
        }
    }

    private class NodeOsCgroupMemExpression extends NestedObjectExpression {

        private static final String CONTROL_GROUP = "control_group";
        private static final String LIMIT_BYTES = "limit_bytes";
        private static final String USAGE_BYTES = "usage_bytes";

        public NodeOsCgroupMemExpression(OsStats.Cgroup cgroup) {
            childImplementations.put(CONTROL_GROUP, (NestableInput<BytesRef>) () -> {
                if (cgroup != null) {
                    return BytesRefs.toBytesRef(cgroup.getMemoryControlGroup());
                }
                return null;
            });
            childImplementations.put(LIMIT_BYTES, (NestableInput<BytesRef>) () -> {
                if (cgroup != null) {
                    return BytesRefs.toBytesRef(cgroup.getMemoryLimitInBytes());
                }
                return null;
            });
            childImplementations.put(USAGE_BYTES, (NestableInput<BytesRef>) () -> {
                if (cgroup != null) {
                    return BytesRefs.toBytesRef(cgroup.getMemoryUsageInBytes());
                }
                return null;
            });
        }
    }
}
