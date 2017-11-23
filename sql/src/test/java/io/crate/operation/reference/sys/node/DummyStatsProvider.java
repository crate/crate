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

package io.crate.operation.reference.sys.node;

import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.process.ProcessStats;

import static org.elasticsearch.test.ESTestCase.randomBoolean;

public class DummyStatsProvider {

    private final long probeTimestamp;
    private final boolean hasIoStats;

    public DummyStatsProvider() {
        probeTimestamp = System.currentTimeMillis();
        hasIoStats = randomBoolean();
    }

    public FsInfo fsInfo() {
        FsInfo.Path[] paths = new FsInfo.Path[]{
            new FsInfo.Path("/dev/sda1", null, 1024L, 512L, 512L),
            new FsInfo.Path("/dev/sdb1", null, 1024L, 512L, 512L)
        };
        FsInfo.IoStats ioStats = null;
        if (hasIoStats) {
            ioStats = new FsInfo.IoStats(new FsInfo.DeviceStats[]{
                new FsInfo.DeviceStats(0, 0, "/dev/sda1", 1L, 1L, 1L, 1L, new FsInfo.DeviceStats(0, 0, "/dev/sda1", 0L, 0L, 0L, 0L, null)),
                new FsInfo.DeviceStats(0, 0, "/dev/sdb1", 1L, 1L, 1L, 1L, new FsInfo.DeviceStats(0, 0, "/dev/sdb1", 0L, 0L, 0L, 0L, null))
            });
        }
        return new FsInfo(probeTimestamp, ioStats, paths);
    }

    public ProcessStats processStats() {
        return new ProcessStats(probeTimestamp,
            1L, 2L,
            new ProcessStats.Cpu((short) 50, 1000L),
            new ProcessStats.Mem(1024L));
    }

    public long probeTimestamp() {
        return probeTimestamp;
    }

    public boolean hasIoStats() {
        return hasIoStats;
    }
}
