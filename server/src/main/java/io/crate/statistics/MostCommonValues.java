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

package io.crate.statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;

public final class MostCommonValues<T> {

    public static <T> MostCommonValues<T> empty() {
        return new MostCommonValues<>(List.of(), new double[0]);
    }

    private final List<T> values;
    private final double[] frequencies;

    public MostCommonValues(List<T> values, double[] frequencies) {
        assert values.size() == frequencies.length : "values and frequencies must have the same number of items";
        this.values = values;
        this.frequencies = frequencies;
    }

    public MostCommonValues(Streamer<T> valueStreamer, StreamInput in) throws IOException {
        int numValues = in.readVInt();
        values = new ArrayList<>();
        frequencies = new double[numValues];
        for (int i = 0; i < numValues; i++) {
            values.add(valueStreamer.readValueFrom(in));
            frequencies[i] = in.readDouble();
        }

    }

    public void writeTo(Streamer<T> valueStreamer, StreamOutput out) throws IOException {
        out.writeVInt(values.size());
        for (int i = 0; i < values.size(); i++) {
            valueStreamer.writeValueTo(out, values.get(i));
            out.writeDouble(frequencies[i]);
        }
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public int length() {
        return values.size();
    }

    public T value(int index) {
        return values.get(index);
    }

    public double frequency(int index) {
        return frequencies[index];
    }

    public List<T> values() {
        return values;
    }

    public double[] frequencies() {
        return frequencies;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MostCommonValues<?> that = (MostCommonValues<?>) o;

        if (Objects.equals(values, that.values) == false) {
            return false;
        }
        return Arrays.equals(frequencies, that.frequencies);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(values);
        result = 31 * result + Arrays.hashCode(frequencies);
        return result;
    }
}
