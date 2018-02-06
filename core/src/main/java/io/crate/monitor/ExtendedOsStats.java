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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.monitor.os.OsStats;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ExtendedOsStats implements Streamable {

    private Cpu cpu;
    private OsStats osStats;
    private long timestamp;
    private long uptime = -1;
    private double[] loadAverage = new double[0];

    public static ExtendedOsStats readExtendedOsStat(StreamInput in) throws IOException {
        ExtendedOsStats stat = new ExtendedOsStats();
        stat.readFrom(in);
        return stat;
    }

    public ExtendedOsStats() {
    }

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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        timestamp = in.readLong();
        uptime = in.readLong();
        loadAverage = in.readDoubleArray();
        cpu = in.readOptionalStreamable(Cpu::new);
        osStats = in.readOptionalWriteable(OsStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeLong(uptime);
        out.writeDoubleArray(loadAverage);
        out.writeOptionalStreamable(cpu);
        out.writeOptionalWriteable(osStats);
    }

    public static class Cpu implements Streamable {

        private short sys;
        private short user;
        private short idle;
        private short stolen;
        private short percent;

        Cpu() {
            this((short) -1);
        }

        Cpu(short percent) {
            this((short) -1, (short) -1, (short) -1, (short) -1, percent);
        }

        Cpu(short sys, short user, short idle, short stolen, short percent) {
            this.sys = sys;
            this.user = user;
            this.idle = idle;
            this.stolen = stolen;
            this.percent = percent;
        }

        public short sys() {
            return sys;
        }

        public short user() {
            return user;
        }

        public short idle() {
            return idle;
        }

        public short stolen() {
            return stolen;
        }

        public short percent() {
            return percent;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            sys = in.readShort();
            user = in.readShort();
            idle = in.readShort();
            stolen = in.readShort();
            percent = in.readShort();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeShort(sys);
            out.writeShort(user);
            out.writeShort(idle);
            out.writeShort(stolen);
            out.writeShort(percent);
        }
    }
}
