/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.monitor;

import java.util.Map;
import java.util.function.Supplier;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.os.OsStats;

import io.crate.common.Suppliers;
import io.crate.common.unit.TimeValue;


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

    private static final ExtendedNetworkInfo NETWORK_INFO = new ExtendedNetworkInfo(ExtendedNetworkInfo.iface());
    private static final double[] NA_LOAD = new double[]{ -1, -1, -1 };

    private final Supplier<ExtendedOsStats> osStatsCache;
    private final Map<String, String> kernelData;

    private static final TimeValue PROBE_CACHE_TIME = TimeValue.timeValueMillis(500L);

    @Inject
    public ExtendedNodeInfo() {
        this.osStatsCache = Suppliers.memoizeWithExpiration(PROBE_CACHE_TIME, () -> osStatsProbe());
        var sysInfo = SysInfo.gather();
        this.kernelData = Map.ofEntries(
            Map.entry("Arch", sysInfo.arch()),
            Map.entry("Description", sysInfo.description()),
            Map.entry("Machine", sysInfo.machine()),
            Map.entry("Name", sysInfo.name()),
            Map.entry("PatchLevel", sysInfo.patchLevel()),
            Map.entry("Vendor", sysInfo.vendor()),
            Map.entry("VendorCodeName", sysInfo.vendorCodeName()),
            Map.entry("VendorName", sysInfo.vendorName()),
            Map.entry("VendorVersion", sysInfo.vendorVersion()),
            Map.entry("Version", sysInfo.version())
        );
    }

    public ExtendedNetworkInfo networkInfo() {
        return NETWORK_INFO;
    }

    public ExtendedOsStats osStats() {
        return osStatsCache.get();
    }

    private ExtendedOsStats osStatsProbe() {
        OsStats osStats = OsProbe.getInstance().osStats();
        OsStats.Cpu cpuProbe = osStats.getCpu();
        ExtendedOsStats.Cpu cpu = new ExtendedOsStats.Cpu(cpuProbe.getPercent());
        return new ExtendedOsStats(
            System.currentTimeMillis(),
            cpu,
            cpuProbe.getLoadAverage() == null ? NA_LOAD : cpuProbe.getLoadAverage(),
            SysInfo.getSystemUptime(),
            osStats);
    }

    public Map<String, String> kernelData() {
        return kernelData;
    }
}
