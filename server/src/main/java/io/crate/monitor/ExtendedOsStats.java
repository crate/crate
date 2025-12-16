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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.monitor.os.OsStats;

import io.crate.common.unit.TimeValue;

public class ExtendedOsStats implements Writeable {

    private final Cpu cpu;
    private final OsStats osStats;
    private final long timestamp;
    private final long uptime;
    private final double[] loadAverage;

    public ExtendedOsStats(long timestamp, Cpu cpu, double[] load, long uptime, OsStats osStats) {
        this.timestamp = timestamp;
        this.cpu = cpu;
        this.loadAverage = load;
        this.uptime = uptime;
        this.osStats = osStats;
    }

    public long timestamp() {
        return timestamp;
    }

    public TimeValue uptime() {
        return new TimeValue(uptime, TimeUnit.MILLISECONDS);
    }

    public double[] loadAverage() {
        return loadAverage;
    }

    public Cpu cpu() {
        return cpu;
    }

    public OsStats osStats() {
        return osStats;
    }

    public ExtendedOsStats(StreamInput in) throws IOException {
        timestamp = in.readLong();
        uptime = in.readLong();
        loadAverage = in.readDoubleArray();
        cpu = in.readOptionalWriteable(Cpu::new);
        osStats = in.readOptionalWriteable(OsStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeLong(uptime);
        out.writeDoubleArray(loadAverage);
        out.writeOptionalWriteable(cpu);
        out.writeOptionalWriteable(osStats);
    }

    public static class Cpu implements Writeable {

        private final short percent;

        Cpu(short percent) {
            this.percent = percent;
        }

        public short percent() {
            return percent;
        }

        public Cpu(StreamInput in) throws IOException {
            percent = in.readShort();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeShort(percent);
        }
    }
}
