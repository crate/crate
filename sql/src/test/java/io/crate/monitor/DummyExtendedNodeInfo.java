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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.env.NodeEnvironment;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;

public class DummyExtendedNodeInfo implements ExtendedNodeInfo {

    private final NodeEnvironment nodeEnvironment;

    public DummyExtendedNodeInfo(NodeEnvironment nodeEnvironment) {
        this.nodeEnvironment = nodeEnvironment;
    }

    static List<Tuple<String, String>> FILE_SYSTEMS = Arrays.asList(
        Tuple.tuple("/dev/sda1", "/foo"),
        Tuple.tuple("/dev/sda2", "/bar")
    );

    @Override
    public ExtendedNetworkStats networkStats() {
        ExtendedNetworkStats.Tcp tcpStats = mock(ExtendedNetworkStats.Tcp.class, new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                return 42L;
            }
        });
        return new ExtendedNetworkStats(tcpStats);
    }

    @Override
    public ExtendedNetworkInfo networkInfo() {
        return new ExtendedNetworkInfo(ExtendedNetworkInfo.NA_INTERFACE);
    }

    public ExtendedFsStats fsStats() {
        List<ExtendedFsStats.Info> infos = new ArrayList<>(FILE_SYSTEMS.size());
        if (nodeEnvironment.hasNodeFile()) {
            for (Tuple<String, String> fileSystem : FILE_SYSTEMS) {
                ExtendedFsStats.Info info = new ExtendedFsStats.Info(
                    fileSystem.v2(), fileSystem.v1(),
                    42L, 42L, 42L,
                    42L, 42L, 42L, 42L, 42L
                );
                infos.add(info);
            }
        }
        return new ExtendedFsStats(infos.toArray(new ExtendedFsStats.Info[infos.size()]));
    }

    @Override
    public ExtendedOsStats osStats() {
        ExtendedOsStats.Cpu cpuStats = new ExtendedOsStats.Cpu((short) 0, (short) 4, (short) 94, (short) 10);
        ExtendedOsStats osStats = new ExtendedOsStats(cpuStats);
        osStats.uptime(3600L);
        osStats.loadAverage(new double[]{1, 5, 15});
        return osStats;
    }

    @Override
    public ExtendedOsInfo osInfo() {
        return new ExtendedOsInfo(Collections.<String, Object>emptyMap());
    }

    @Override
    public ExtendedProcessCpuStats processCpuStats() {
        return new ExtendedProcessCpuStats((short) 50, 1000L, 500L, 1500L);
    }
}
