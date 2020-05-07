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

package org.elasticsearch.index.recovery;

import io.crate.common.unit.TimeValue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Recovery related statistics, starting at the shard level and allowing aggregation to
 * indices and node level
 */
public class RecoveryStats {

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
    public String toString() {
        return "recoveryStats, currentAsSource [" + currentAsSource() + "],currentAsTarget ["
                + currentAsTarget() + "], throttle [" + throttleTime() + "]";
    }
}
