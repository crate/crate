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

import com.google.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.NodeEnvironment;
import org.hyperic.sigar.*;

import java.io.File;
import java.util.Map;

public class SigarExtendedNodeInfo implements ExtendedNodeInfo {

    private final SigarService sigarService;
    private final Map<File, FileSystem> fileSystems = Maps.newHashMap();

    @Inject
    public SigarExtendedNodeInfo(SigarService sigarService) {
        this.sigarService = sigarService;
    }

    @Override
    public ExtendedNetworkStats networkStats() {
        Sigar sigar = sigarService.sigar();
        ExtendedNetworkStats.Tcp tcp;
        try {
            Tcp sigarTcp = sigar.getTcp();
            tcp = new ExtendedNetworkStats.Tcp(
                    sigarTcp.getActiveOpens(),
                    sigarTcp.getPassiveOpens(),
                    sigarTcp.getAttemptFails(),
                    sigarTcp.getEstabResets(),
                    sigarTcp.getCurrEstab(),
                    sigarTcp.getInSegs(),
                    sigarTcp.getOutSegs(),
                    sigarTcp.getRetransSegs(),
                    sigarTcp.getInErrs(),
                    sigarTcp.getOutRsts()
            );
        } catch (SigarException e) {
            // ignore
            tcp = new ExtendedNetworkStats.Tcp();
        }
        ExtendedNetworkStats stats = new ExtendedNetworkStats(tcp);
        stats.timestamp(System.currentTimeMillis());
        return stats;
    }

    @Override
    public ExtendedNetworkInfo networkInfo() {
        Sigar sigar = sigarService.sigar();
        ExtendedNetworkInfo.Interface iface;
        try {
            NetInterfaceConfig netInterfaceConfig = sigar.getNetInterfaceConfig(null);
            iface = new ExtendedNetworkInfo.Interface(netInterfaceConfig.getName(), netInterfaceConfig.getAddress(), netInterfaceConfig.getHwaddr());
        } catch (SigarException e) {
            // ignore
            iface = ExtendedNetworkInfo.NA_INTERFACE;
        }

        return new ExtendedNetworkInfo(iface);
    }

    @Override
    public ExtendedFsStats fsStats(NodeEnvironment nodeEnvironment) {
        if (!nodeEnvironment.hasNodeFile()) {
            return new ExtendedFsStats(new ExtendedFsStats.Info[0]);
        }

        NodeEnvironment.NodePath[] nodePaths = nodeEnvironment.nodePaths();
        ExtendedFsStats.Info[] infos = new ExtendedFsStats.Info[nodePaths.length];

        Sigar sigar = sigarService.sigar();
        for (int i = 0; i < nodePaths.length; i++) {
            NodeEnvironment.NodePath nodePath = nodePaths[i];
            File dataLocation = nodePath.path.toFile();

            ExtendedFsStats.Info info = new ExtendedFsStats.Info();
            info.path(dataLocation.getAbsolutePath());

            try {
                FileSystem fileSystem = fileSystems.get(dataLocation);
                if (fileSystem == null) {
                    FileSystemMap fileSystemMap = sigar.getFileSystemMap();
                    if (fileSystemMap != null) {
                        fileSystem = fileSystemMap.getMountPoint(dataLocation.getPath());
                        fileSystems.put(dataLocation, fileSystem);
                    }
                }
                if (fileSystem != null) {
                    info.dev(fileSystem.getDevName());

                    FileSystemUsage fileSystemUsage = sigar.getFileSystemUsage(fileSystem.getDirName());
                    if (fileSystemUsage != null) {
                        // total/free/available seem to be reported in kilobytes
                        // so convert it into bytes
                        info.total(fileSystemUsage.getTotal() * 1024);
                        info.free(fileSystemUsage.getFree() * 1024);
                        info.used(fileSystemUsage.getUsed() * 1024);
                        info.available(fileSystemUsage.getAvail() * 1024);
                        info.diskReads(fileSystemUsage.getDiskReads());
                        info.diskWrites(fileSystemUsage.getDiskWrites());
                        info.diskReadSizeInBytes(fileSystemUsage.getDiskReadBytes());
                        info.diskWriteSizeInBytes(fileSystemUsage.getDiskWriteBytes());
                    }
                }
            } catch (SigarException e) {
                // failed...
            }

            infos[i] = info;
        }

        return new ExtendedFsStats(infos);
    }

    @Override
    public ExtendedOsStats osStats() {
        Sigar sigar = sigarService.sigar();

        ExtendedOsStats.Cpu cpu;
        try {
            CpuPerc cpuPerc = sigar.getCpuPerc();
            cpu = new ExtendedOsStats.Cpu(
                    (short) Math.round(cpuPerc.getSys() * 100),
                    (short) Math.round(cpuPerc.getUser() * 100),
                    (short) Math.round(cpuPerc.getIdle() * 100),
                    (short) Math.round(cpuPerc.getStolen() * 100)
            );
        } catch (SigarException e) {
            // ignore
            cpu = new ExtendedOsStats.Cpu();
        }

        ExtendedOsStats stats = new ExtendedOsStats(cpu);
        stats.timestamp(System.currentTimeMillis());

        try {
            stats.loadAverage(sigar.getLoadAverage());
        } catch (SigarException e) {
            // ignore
        }

        try {
            stats.uptime((long) sigar.getUptime().getUptime());
        } catch (SigarException e) {
            // ignore
        }

        return stats;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ExtendedOsInfo osInfo() {
        return new ExtendedOsInfo(OperatingSystem.getInstance().toMap());
    }

    @Override
    public ExtendedProcessCpuStats processCpuStats() {
        Sigar sigar = sigarService.sigar();
        try {
            ProcCpu cpu = sigar.getProcCpu(sigar.getPid());
            return new ExtendedProcessCpuStats(
                    (short) Math.round(cpu.getPercent() * 100),
                    cpu.getSys(),
                    cpu.getUser(),
                    cpu.getTotal()
            );
        } catch (SigarException e) {
            // ignore
            return new ExtendedProcessCpuStats();
        }
    }
}
