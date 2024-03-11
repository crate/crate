/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.statistics;

import java.util.Collection;

import org.apache.datasketches.theta.UpdateSketch;

import io.crate.types.DataType;

public class ColumnSketchBuilder<T> {

    private final DataType<T> dataType;

    private long sampleCount;
    private int nullCount;
    private long totalBytes;
    private final UpdateSketch distinctSketch = UpdateSketch.builder().build();
    private final MostCommonValuesSketch<T> mostCommonValuesSketch;
    private final HistogramSketch<T> histogramSketch;

    public ColumnSketchBuilder(Class<T> clazz, DataType<T> dataType) {
        this.dataType = dataType;
        this.mostCommonValuesSketch = new MostCommonValuesSketch<>(dataType.streamer());
        this.histogramSketch = new HistogramSketch<>(clazz, dataType);
    }

    public void add(T value) {
        sampleCount++;
        if (value == null) {
            nullCount++;
        } else {
            totalBytes += dataType.valueBytes(value);
            distinctSketch.update(value.toString());
            mostCommonValuesSketch.update(value);
            histogramSketch.update(value);
        }
    }

    public void addAll(Collection<T> values) {
        for (var v : values) {
            add(v);
        }
    }

    public ColumnSketch<T> toSketch() {
        return new ColumnSketch<>(
            dataType,
            sampleCount,
            nullCount,
            totalBytes,
            distinctSketch,
            mostCommonValuesSketch,
            histogramSketch
        );
    }

}
