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

package io.crate.metadata.sys;

import io.crate.planner.operators.StatementClassifier.Classification;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class ClassifiedMetrics implements Iterable<MetricsView> {

    private static final long HIGHEST_TRACKABLE_VALUE = TimeUnit.MINUTES.toMillis(10);
    private static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 3;

    private final ConcurrentHashMap<Classification, Metrics> metrics = new ConcurrentHashMap<>();

    public static class Metrics {

        private final Classification classification;
        private final LongAdder sumOfDurations = new LongAdder();
        private final LongAdder failedCount = new LongAdder();
        private final LongAdder affectedRowCount = new LongAdder();
        private final Recorder recorder;

        private final Histogram totalHistogram = new Histogram(HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);

        public Metrics(Classification classification) {
            this.classification = classification;
            this.recorder = new Recorder(HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
        }

        public void recordValues(long duration, long affectedRowCount) {
            // We use start and end time to calculate the duration (since we track them anyway)
            // If the system time is adjusted this can lead to negative durations
            // so we protect here against it.
            recorder.recordValue(Math.min(Math.max(0, duration), HIGHEST_TRACKABLE_VALUE));
            // we record the real duration (with no upper capping) in the sum of durations as there are no upper limits
            // for the values we record as it is the case with the histogram
            sumOfDurations.add(Math.max(0, duration));
            this.affectedRowCount.add(affectedRowCount);
        }

        public void recordFailedExecution(long duration) {
            recordValues(duration, 0);
            failedCount.increment();
        }

        public MetricsView createMetricsView() {
            Histogram histogram;
            synchronized (totalHistogram) {
                // getIntervalHistogram resets the internal histogram afterwards;
                // so we keep `totalHistogram` to not lose any measurements.
                Histogram intervalHistogram = recorder.getIntervalHistogram();
                totalHistogram.add(intervalHistogram);
                histogram = totalHistogram.copy();
            }
            return new MetricsView(
                histogram,
                sumOfDurations.longValue(),
                failedCount.longValue(),
                affectedRowCount.longValue(),
                classification
            );
        }
    }

    public void recordValue(Classification classification, long duration, long affectedRowCount) {
        getOrCreate(classification).recordValues(duration, affectedRowCount);
    }

    public void recordFailedExecution(Classification classification, long duration) {
        getOrCreate(classification).recordFailedExecution(duration);
    }

    private Metrics getOrCreate(Classification classification) {
        Metrics histogram = metrics.get(classification);
        if (histogram == null) {
            histogram = new Metrics(classification);
            metrics.put(classification, histogram);
        }
        return histogram;
    }

    public void reset() {
        metrics.clear();
    }

    @Override
    public Iterator<MetricsView> iterator() {
        return metrics.values()
            .stream()
            .map(Metrics::createMetricsView)
            .iterator();
    }
}
