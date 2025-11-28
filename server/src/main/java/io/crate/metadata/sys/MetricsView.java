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

import io.crate.planner.operators.StatementClassifier;
import org.HdrHistogram.Histogram;

import io.crate.common.annotations.Immutable;

@Immutable
public final class MetricsView {

    private final Histogram histogram;
    private final long sumOfDurations;
    private final long failedCount;
    private final long affectedRowCount;
    private final StatementClassifier.Classification classification;

    /**
     * Create a read-only view onto the provided metrics.
     *
     * The given histogram must not be modified by whoever creates the MetricsView.
     */
    public MetricsView(Histogram histogram,
                       long sumOfDurations,
                       long failedCount,
                       long affectedRowCount,
                       StatementClassifier.Classification classification) {
        this.histogram = histogram;
        this.sumOfDurations = sumOfDurations;
        this.failedCount = failedCount;
        this.affectedRowCount = affectedRowCount;
        this.classification = classification;
    }

    public long totalCount() {
        return histogram.getTotalCount();
    }

    public double mean() {
        return histogram.getMean();
    }

    public double stdDeviation() {
        return histogram.getStdDeviation();
    }

    public long maxValue() {
        return histogram.getMaxValue();
    }

    public long minValue() {
        long minValue = histogram.getMinValue();
        return minValue == Long.MAX_VALUE ? 0L : minValue;
    }

    public long getValueAtPercentile(double percentile) {
        return histogram.getValueAtPercentile(percentile);
    }

    public long sumOfDurations() {
        return sumOfDurations;
    }

    public long failedCount() {
        return failedCount;
    }

    public StatementClassifier.Classification classification() {
        return classification;
    }

    public long affectedRowCount() {
        return affectedRowCount;
    }
}
