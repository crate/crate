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
package io.crate.es.index.recovery;

import io.crate.es.common.io.stream.StreamInput;
import io.crate.es.common.io.stream.StreamOutput;
import io.crate.es.common.io.stream.Streamable;
import io.crate.es.common.unit.TimeValue;
import io.crate.es.common.xcontent.ToXContentFragment;
import io.crate.es.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Recovery related statistics, starting at the shard level and allowing aggregation to
 * indices and node level
 */
public class RecoveryStats implements ToXContentFragment, Streamable {

    private final AtomicInteger currentAsSource = new AtomicInteger();
    private final AtomicInteger currentAsTarget = new AtomicInteger();
    private final AtomicLong throttleTimeInNanos = new AtomicLong();

    public RecoveryStats() {
    }

    public void add(RecoveryStats recoveryStats) {
        if (recoveryStats != null) {
            this.currentAsSource.addAndGet(recoveryStats.currentAsSource());
            this.currentAsTarget.addAndGet(recoveryStats.currentAsTarget());
        }
        addTotals(recoveryStats);
    }

    public void addTotals(RecoveryStats recoveryStats) {
        if (recoveryStats != null) {
            this.throttleTimeInNanos.addAndGet(recoveryStats.throttleTime().nanos());
        }
    }

    /**
     * Number of ongoing recoveries for which a shard serves as a source
     */
    public int currentAsSource() {
        return currentAsSource.get();
    }

    /**
     * Number of ongoing recoveries for which a shard serves as a target
     */
    public int currentAsTarget() {
        return currentAsTarget.get();
    }

    /**
     * Total time recoveries waited due to throttling
     */
    public TimeValue throttleTime() {
        return TimeValue.timeValueNanos(throttleTimeInNanos.get());
    }

    public void incCurrentAsTarget() {
        currentAsTarget.incrementAndGet();
    }

    public void decCurrentAsTarget() {
        currentAsTarget.decrementAndGet();
    }

    public void incCurrentAsSource() {
        currentAsSource.incrementAndGet();
    }

    public void decCurrentAsSource() {
        currentAsSource.decrementAndGet();
    }

    public void addThrottleTime(long nanos) {
        throttleTimeInNanos.addAndGet(nanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.RECOVERY);
        builder.field(Fields.CURRENT_AS_SOURCE, currentAsSource());
        builder.field(Fields.CURRENT_AS_TARGET, currentAsTarget());
        builder.humanReadableField(Fields.THROTTLE_TIME_IN_MILLIS, Fields.THROTTLE_TIME, throttleTime());
        builder.endObject();
        return builder;
    }

    public static RecoveryStats readRecoveryStats(StreamInput in) throws IOException {
        RecoveryStats stats = new RecoveryStats();
        stats.readFrom(in);
        return stats;
    }

    static final class Fields {
        static final String RECOVERY = "recovery";
        static final String CURRENT_AS_SOURCE = "current_as_source";
        static final String CURRENT_AS_TARGET = "current_as_target";
        static final String THROTTLE_TIME = "throttle_time";
        static final String THROTTLE_TIME_IN_MILLIS = "throttle_time_in_millis";
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        currentAsSource.set(in.readVInt());
        currentAsTarget.set(in.readVInt());
        throttleTimeInNanos.set(in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(currentAsSource.get());
        out.writeVInt(currentAsTarget.get());
        out.writeLong(throttleTimeInNanos.get());
    }

    @Override
    public String toString() {
        return "recoveryStats, currentAsSource [" + currentAsSource() + "],currentAsTarget ["
                + currentAsTarget() + "], throttle [" + throttleTime() + "]";
    }
}
