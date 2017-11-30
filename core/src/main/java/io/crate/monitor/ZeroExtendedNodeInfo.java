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

package io.crate.monitor;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.os.OsStats;

import java.util.Collections;

public class ZeroExtendedNodeInfo implements ExtendedNodeInfo {

    private static final TimeValue PROBE_CACHE_TIME = TimeValue.timeValueMillis(500L);
    private final ExtendedOsStatsCache osStatsCache;

    private static final ExtendedNetworkStats NETWORK_STATS = new ExtendedNetworkStats(new ExtendedNetworkStats.Tcp());
    private static final ExtendedNetworkInfo NETWORK_INFO = new ExtendedNetworkInfo(ExtendedNetworkInfo.NA_INTERFACE);
    private static final ExtendedFsStats FS_STATS = new ExtendedFsStats(new ExtendedFsStats.Info());
    private static final ExtendedOsInfo OS_INFO = new ExtendedOsInfo(Collections.<String, Object>emptyMap());
    private static final ExtendedProcessCpuStats PROCESS_CPU_STATS = new ExtendedProcessCpuStats();

    public ZeroExtendedNodeInfo() {
        this.osStatsCache = new ExtendedOsStatsCache(PROBE_CACHE_TIME, osStatsProbe());
    }

    @Override
    public ExtendedNetworkStats networkStats() {
        return NETWORK_STATS;
    }

    @Override
    public ExtendedNetworkInfo networkInfo() {
        return NETWORK_INFO;
    }

    @Override
    public ExtendedFsStats fsStats() {
        return FS_STATS;
    }

    @Override
    public ExtendedOsStats osStats() {
        return osStatsCache.getOrRefresh();
    }

    private ExtendedOsStats osStatsProbe() {
        ExtendedOsStats.Cpu cpu = new ExtendedOsStats.Cpu();
        OsStats osStats = OsProbe.getInstance().osStats();
        ExtendedOsStats stats = new ExtendedOsStats(cpu, osStats);
        stats.timestamp(System.currentTimeMillis());
        return stats;
    }

    @Override
    public ExtendedOsInfo osInfo() {
        return OS_INFO;
    }

    @Override
    public ExtendedProcessCpuStats processCpuStats() {
        return PROCESS_CPU_STATS;
    }

    /**
     * Cache for osStats()
     */
    private class ExtendedOsStatsCache extends SingleObjectCache<ExtendedOsStats> {

        ExtendedOsStatsCache(TimeValue interval, ExtendedOsStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected ExtendedOsStats refresh() {
            return osStatsProbe();
        }
    }
}
