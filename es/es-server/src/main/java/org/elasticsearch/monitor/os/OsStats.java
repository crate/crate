/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.os;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class OsStats implements Writeable, ToXContentFragment {

    private final long timestamp;
    private final Cpu cpu;
    private final Mem mem;
    private final Swap swap;
    private final Cgroup cgroup;

    public OsStats(final long timestamp, final Cpu cpu, final Mem mem, final Swap swap, final Cgroup cgroup) {
        this.timestamp = timestamp;
        this.cpu = Objects.requireNonNull(cpu);
        this.mem = Objects.requireNonNull(mem);
        this.swap = Objects.requireNonNull(swap);
        this.cgroup = cgroup;
    }

    public OsStats(StreamInput in) throws IOException {
        this.timestamp = in.readVLong();
        this.cpu = new Cpu(in);
        this.mem = new Mem(in);
        this.swap = new Swap(in);
        if (in.getVersion().onOrAfter(Version.V_5_1_1)) {
            this.cgroup = in.readOptionalWriteable(Cgroup::new);
        } else {
            this.cgroup = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        cpu.writeTo(out);
        mem.writeTo(out);
        swap.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_5_1_1)) {
            out.writeOptionalWriteable(cgroup);
        }
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Cpu getCpu() { return cpu; }

    public Mem getMem() {
        return mem;
    }

    public Swap getSwap() {
        return swap;
    }

    public Cgroup getCgroup() {
        return cgroup;
    }

    static final class Fields {
        static final String OS = "os";
        static final String TIMESTAMP = "timestamp";
        static final String CPU = "cpu";
        static final String PERCENT = "percent";
        static final String LOAD_AVERAGE = "load_average";
        static final String LOAD_AVERAGE_1M = "1m";
        static final String LOAD_AVERAGE_5M = "5m";
        static final String LOAD_AVERAGE_15M = "15m";

        static final String MEM = "mem";
        static final String SWAP = "swap";
        static final String FREE = "free";
        static final String FREE_IN_BYTES = "free_in_bytes";
        static final String USED = "used";
        static final String USED_IN_BYTES = "used_in_bytes";
        static final String TOTAL = "total";
        static final String TOTAL_IN_BYTES = "total_in_bytes";

        static final String FREE_PERCENT = "free_percent";
        static final String USED_PERCENT = "used_percent";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.OS);
        builder.field(Fields.TIMESTAMP, getTimestamp());
        cpu.toXContent(builder, params);
        mem.toXContent(builder, params);
        swap.toXContent(builder, params);
        if (cgroup != null) {
            cgroup.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public static class Cpu implements Writeable, ToXContentFragment {

        private final short percent;
        private final double[] loadAverage;

        public Cpu(short systemCpuPercent, double[] systemLoadAverage) {
            this.percent = systemCpuPercent;
            this.loadAverage = systemLoadAverage;
        }

        public Cpu(StreamInput in) throws IOException {
            this.percent = in.readShort();
            if (in.readBoolean()) {
                this.loadAverage = in.readDoubleArray();
            } else {
                this.loadAverage = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeShort(percent);
            if (loadAverage == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeDoubleArray(loadAverage);
            }
        }

        public short getPercent() {
            return percent;
        }

        public double[] getLoadAverage() {
            return loadAverage;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.CPU);
            builder.field(Fields.PERCENT, getPercent());
            if (getLoadAverage() != null && Arrays.stream(getLoadAverage()).anyMatch(load -> load != -1)) {
                builder.startObject(Fields.LOAD_AVERAGE);
                if (getLoadAverage()[0] != -1) {
                    builder.field(Fields.LOAD_AVERAGE_1M, getLoadAverage()[0]);
                }
                if (getLoadAverage()[1] != -1) {
                    builder.field(Fields.LOAD_AVERAGE_5M, getLoadAverage()[1]);
                }
                if (getLoadAverage()[2] != -1) {
                    builder.field(Fields.LOAD_AVERAGE_15M, getLoadAverage()[2]);
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }
    }

    public static class Swap implements Writeable, ToXContentFragment {

        private final long total;
        private final long free;

        public Swap(long total, long free) {
            this.total = total;
            this.free = free;
        }

        public Swap(StreamInput in) throws IOException {
            this.total = in.readLong();
            this.free = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(total);
            out.writeLong(free);
        }

        public ByteSizeValue getFree() {
            return new ByteSizeValue(free);
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(total - free);
        }

        public ByteSizeValue getTotal() {
            return new ByteSizeValue(total);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.SWAP);
            builder.humanReadableField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, getTotal());
            builder.humanReadableField(Fields.FREE_IN_BYTES, Fields.FREE, getFree());
            builder.humanReadableField(Fields.USED_IN_BYTES, Fields.USED, getUsed());
            builder.endObject();
            return builder;
        }
    }

    public static class Mem implements Writeable, ToXContentFragment {

        private final long total;
        private final long free;

        public Mem(long total, long free) {
            this.total = total;
            this.free = free;
        }

        public Mem(StreamInput in) throws IOException {
            this.total = in.readLong();
            this.free = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(total);
            out.writeLong(free);
        }

        public ByteSizeValue getTotal() {
            return new ByteSizeValue(total);
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(total - free);
        }

        public short getUsedPercent() {
            return calculatePercentage(getUsed().getBytes(), total);
        }

        public ByteSizeValue getFree() {
            return new ByteSizeValue(free);
        }

        public short getFreePercent() {
            return calculatePercentage(free, total);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.MEM);
            builder.humanReadableField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, getTotal());
            builder.humanReadableField(Fields.FREE_IN_BYTES, Fields.FREE, getFree());
            builder.humanReadableField(Fields.USED_IN_BYTES, Fields.USED, getUsed());
            builder.field(Fields.FREE_PERCENT, getFreePercent());
            builder.field(Fields.USED_PERCENT, getUsedPercent());
            builder.endObject();
            return builder;
        }
    }

    /**
     * Encapsulates basic cgroup statistics.
     */
    public static class Cgroup implements Writeable, ToXContentFragment {

        private final String cpuAcctControlGroup;
        private final long cpuAcctUsageNanos;
        private final String cpuControlGroup;
        private final long cpuCfsPeriodMicros;
        private final long cpuCfsQuotaMicros;
        private final CpuStat cpuStat;
        // These will be null for nodes running versions prior to 6.1.0
        private final String memoryControlGroup;
        private final String memoryLimitInBytes;
        private final String memoryUsageInBytes;

        /**
         * The control group for the {@code cpuacct} subsystem.
         *
         * @return the control group
         */
        public String getCpuAcctControlGroup() {
            return cpuAcctControlGroup;
        }

        /**
         * The total CPU time consumed by all tasks in the
         * {@code cpuacct} control group from
         * {@link Cgroup#cpuAcctControlGroup}.
         *
         * @return the total CPU time in nanoseconds
         */
        public long getCpuAcctUsageNanos() {
            return cpuAcctUsageNanos;
        }

        /**
         * The control group for the {@code cpu} subsystem.
         *
         * @return the control group
         */
        public String getCpuControlGroup() {
            return cpuControlGroup;
        }

        /**
         * The period of time for how frequently the control group from
         * {@link Cgroup#cpuControlGroup} has its access to CPU
         * resources reallocated.
         *
         * @return the period of time in microseconds
         */
        public long getCpuCfsPeriodMicros() {
            return cpuCfsPeriodMicros;
        }

        /**
         * The total amount of time for which all tasks in the control
         * group from {@link Cgroup#cpuControlGroup} can run in one
         * period as represented by {@link Cgroup#cpuCfsPeriodMicros}.
         *
         * @return the total amount of time in microseconds
         */
        public long getCpuCfsQuotaMicros() {
            return cpuCfsQuotaMicros;
        }

        /**
         * The CPU time statistics. See {@link CpuStat}.
         *
         * @return the CPU time statistics.
         */
        public CpuStat getCpuStat() {
            return cpuStat;
        }

        /**
         * The control group for the {@code memory} subsystem.
         *
         * @return the control group
         */
        public String getMemoryControlGroup() {
            return memoryControlGroup;
        }

        /**
         * The maximum amount of user memory (including file cache).
         * This is stored as a <code>String</code> because the value can be too big to fit in a
         * <code>long</code>.  (The alternative would have been <code>BigInteger</code> but then
         * it would not be possible to index the OS stats document into Elasticsearch without
         * losing information, as <code>BigInteger</code> is not a supported Elasticsearch type.)
         *
         * @return the maximum amount of user memory (including file cache).
         */
        public String getMemoryLimitInBytes() {
            return memoryLimitInBytes;
        }

        /**
         * The total current memory usage by processes in the cgroup (in bytes).
         * This is stored as a <code>String</code> for consistency with <code>memoryLimitInBytes</code>.
         *
         * @return the total current memory usage by processes in the cgroup (in bytes).
         */
        public String getMemoryUsageInBytes() {
            return memoryUsageInBytes;
        }

        public Cgroup(
            final String cpuAcctControlGroup,
            final long cpuAcctUsageNanos,
            final String cpuControlGroup,
            final long cpuCfsPeriodMicros,
            final long cpuCfsQuotaMicros,
            final CpuStat cpuStat,
            final String memoryControlGroup,
            final String memoryLimitInBytes,
            final String memoryUsageInBytes) {
            this.cpuAcctControlGroup = Objects.requireNonNull(cpuAcctControlGroup);
            this.cpuAcctUsageNanos = cpuAcctUsageNanos;
            this.cpuControlGroup = Objects.requireNonNull(cpuControlGroup);
            this.cpuCfsPeriodMicros = cpuCfsPeriodMicros;
            this.cpuCfsQuotaMicros = cpuCfsQuotaMicros;
            this.cpuStat = Objects.requireNonNull(cpuStat);
            this.memoryControlGroup = memoryControlGroup;
            this.memoryLimitInBytes = memoryLimitInBytes;
            this.memoryUsageInBytes = memoryUsageInBytes;
        }

        Cgroup(final StreamInput in) throws IOException {
            cpuAcctControlGroup = in.readString();
            cpuAcctUsageNanos = in.readLong();
            cpuControlGroup = in.readString();
            cpuCfsPeriodMicros = in.readLong();
            cpuCfsQuotaMicros = in.readLong();
            cpuStat = new CpuStat(in);
            if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
                memoryControlGroup = in.readOptionalString();
                memoryLimitInBytes = in.readOptionalString();
                memoryUsageInBytes = in.readOptionalString();
            } else {
                memoryControlGroup = null;
                memoryLimitInBytes = null;
                memoryUsageInBytes = null;
            }
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeString(cpuAcctControlGroup);
            out.writeLong(cpuAcctUsageNanos);
            out.writeString(cpuControlGroup);
            out.writeLong(cpuCfsPeriodMicros);
            out.writeLong(cpuCfsQuotaMicros);
            cpuStat.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
                out.writeOptionalString(memoryControlGroup);
                out.writeOptionalString(memoryLimitInBytes);
                out.writeOptionalString(memoryUsageInBytes);
            }
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject("cgroup");
            {
                builder.startObject("cpuacct");
                {
                    builder.field("control_group", cpuAcctControlGroup);
                    builder.field("usage_nanos", cpuAcctUsageNanos);
                }
                builder.endObject();
                builder.startObject("cpu");
                {
                    builder.field("control_group", cpuControlGroup);
                    builder.field("cfs_period_micros", cpuCfsPeriodMicros);
                    builder.field("cfs_quota_micros", cpuCfsQuotaMicros);
                    cpuStat.toXContent(builder, params);
                }
                builder.endObject();
                if (memoryControlGroup != null) {
                    builder.startObject("memory");
                    {
                        builder.field("control_group", memoryControlGroup);
                        if (memoryLimitInBytes != null) {
                            builder.field("limit_in_bytes", memoryLimitInBytes);
                        }
                        if (memoryUsageInBytes != null) {
                            builder.field("usage_in_bytes", memoryUsageInBytes);
                        }
                    }
                    builder.endObject();
                }
            }
            builder.endObject();
            return builder;
        }

        /**
         * Encapsulates CPU time statistics.
         */
        public static class CpuStat implements Writeable, ToXContentFragment {

            private final long numberOfElapsedPeriods;
            private final long numberOfTimesThrottled;
            private final long timeThrottledNanos;

            /**
             * The number of elapsed periods.
             *
             * @return the number of elapsed periods as measured by
             * {@code cpu.cfs_period_us}
             */
            public long getNumberOfElapsedPeriods() {
                return numberOfElapsedPeriods;
            }

            /**
             * The number of times tasks in the control group have been
             * throttled.
             *
             * @return the number of times
             */
            public long getNumberOfTimesThrottled() {
                return numberOfTimesThrottled;
            }

            /**
             * The total time duration for which tasks in the control
             * group have been throttled.
             *
             * @return the total time in nanoseconds
             */
            public long getTimeThrottledNanos() {
                return timeThrottledNanos;
            }

            public CpuStat(final long numberOfElapsedPeriods, final long numberOfTimesThrottled, final long timeThrottledNanos) {
                this.numberOfElapsedPeriods = numberOfElapsedPeriods;
                this.numberOfTimesThrottled = numberOfTimesThrottled;
                this.timeThrottledNanos = timeThrottledNanos;
            }

            CpuStat(final StreamInput in) throws IOException {
                numberOfElapsedPeriods = in.readLong();
                numberOfTimesThrottled = in.readLong();
                timeThrottledNanos = in.readLong();
            }

            @Override
            public void writeTo(final StreamOutput out) throws IOException {
                out.writeLong(numberOfElapsedPeriods);
                out.writeLong(numberOfTimesThrottled);
                out.writeLong(timeThrottledNanos);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject("stat");
                {
                    builder.field("number_of_elapsed_periods", numberOfElapsedPeriods);
                    builder.field("number_of_times_throttled", numberOfTimesThrottled);
                    builder.field("time_throttled_nanos", timeThrottledNanos);
                }
                builder.endObject();
                return builder;
            }

        }

    }

    public static short calculatePercentage(long used, long max) {
        return max <= 0 ? 0 : (short) (Math.round((100d * used) / max));
    }

}
