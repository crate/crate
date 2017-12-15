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

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.os.OsStats;


/**
 * This class provides monitoring probes for OS stats/info and network stats/info.
 * These probes are used in SysNodesExpressions and are exposed in the sys.nodes table and the _node column.
 *
 * The name ExtendedNodeInfo is evolved from the idea that CrateDB extends the node information that is already provided
 * from Elasticsearch. When ES removed the Sigar dependency CrateDB still continued to use it and provided the
 * information that was previously available.
 * With version 2.3 CrateDB also removed Sigar and "extended" node information is reduced to a minimum. The class could
 * probably be removed completely in future versions.
 */
public class ExtendedNodeInfo {

    private static final ExtendedNetworkStats NETWORK_STATS = new ExtendedNetworkStats(new ExtendedNetworkStats.Tcp());
    private static final ExtendedNetworkInfo NETWORK_INFO = new ExtendedNetworkInfo(ExtendedNetworkInfo.iface());
    private static final ExtendedOsInfo OS_INFO = new ExtendedOsInfo(SysInfo.gather());
    private static final double[] NA_LOAD = new double[]{ -1, -1, -1 };

    private final ExtendedOsStatsCache osStatsCache;

    private static final TimeValue PROBE_CACHE_TIME = TimeValue.timeValueMillis(500L);

    @Inject
    public ExtendedNodeInfo() {
        this.osStatsCache = new ExtendedOsStatsCache(PROBE_CACHE_TIME, osStatsProbe());
    }

    @Deprecated
    public ExtendedNetworkStats networkStats() {
        return NETWORK_STATS;
    }

    public ExtendedNetworkInfo networkInfo() {
        return NETWORK_INFO;
    }

    public ExtendedOsStats osStats() {
        return osStatsCache.getOrRefresh();
    }

    private ExtendedOsStats osStatsProbe() {
        OsStats osStats = OsProbe.getInstance().osStats();
        OsStats.Cpu cpuProbe = osStats.getCpu();
        ExtendedOsStats.Cpu cpu = new ExtendedOsStats.Cpu(cpuProbe.getPercent());
        ExtendedOsStats.CgroupMem cgroupMem = Constants.LINUX ? CgroupMemoryProbe.getCgroup() : null;
        return new ExtendedOsStats(System.currentTimeMillis(),
            cpu,
            cpuProbe.getLoadAverage() == null ? NA_LOAD : cpuProbe.getLoadAverage(),
            SysInfo.getSystemUptime(),
            osStats,
            cgroupMem);
    }

    public ExtendedOsInfo osInfo() {
        return OS_INFO;
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
