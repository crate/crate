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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.Streamer;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public final class ColumnStats<T> implements Writeable {

    private final double nullFraction;
    private final double averageSizeInBytes;
    private final double approxDistinct;
    private final DataType<T> type;
    private final MostCommonValues mostCommonValues;
    private final List<T> histogram;

    public ColumnStats(double nullFraction,
                       double averageSizeInBytes,
                       double approxDistinct,
                       DataType<T> type,
                       MostCommonValues mostCommonValues,
                       List<T> histogram) {
        this.nullFraction = nullFraction;
        this.averageSizeInBytes = averageSizeInBytes;
        this.approxDistinct = approxDistinct;
        this.type = type;
        this.mostCommonValues = mostCommonValues;
        this.histogram = histogram;
    }

    public ColumnStats(StreamInput in) throws IOException {
        //noinspection unchecked
        this.type = (DataType<T>) DataTypes.fromStream(in);

        this.nullFraction = in.readDouble();
        this.averageSizeInBytes = in.readDouble();
        this.approxDistinct = in.readDouble();
        Streamer<T> streamer = type.streamer();
        this.mostCommonValues = new MostCommonValues(streamer, in);
        int numHistogramValues = in.readVInt();
        ArrayList<T> histogram = new ArrayList<>(numHistogramValues);
        for (int i = 0; i < numHistogramValues; i++) {
            histogram.add(streamer.readValueFrom(in));
        }
        this.histogram = histogram;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Streamer<T> streamer = this.type.streamer();
        DataTypes.toStream(type, out);
        out.writeDouble(nullFraction);
        out.writeDouble(averageSizeInBytes);
        out.writeDouble(approxDistinct);
        mostCommonValues.writeTo(type.streamer(), out);
        out.writeVInt(histogram.size());
        for (T o : histogram) {
            streamer.writeValueTo(out, o);
        }
    }

    public double averageSizeInBytes() {
        return averageSizeInBytes;
    }

    public double nullFraction() {
        return nullFraction;
    }

    public double approxDistinct() {
        return approxDistinct;
    }

    public MostCommonValues mostCommonValues() {
        return mostCommonValues;
    }

    public List<T> histogram() {
        return histogram;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ColumnStats<?> that = (ColumnStats<?>) o;

        if (Double.compare(that.nullFraction, nullFraction) != 0) {
            return false;
        }
        if (Double.compare(that.averageSizeInBytes, averageSizeInBytes) != 0) {
            return false;
        }
        if (Double.compare(that.approxDistinct, approxDistinct) != 0) {
            return false;
        }
        if (!type.equals(that.type)) {
            return false;
        }
        if (!mostCommonValues.equals(that.mostCommonValues)) {
            return false;
        }
        return histogram.equals(that.histogram);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(nullFraction);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(averageSizeInBytes);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(approxDistinct);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + type.hashCode();
        result = 31 * result + mostCommonValues.hashCode();
        result = 31 * result + histogram.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ColumnStats{" +
            "nullFraction=" + nullFraction +
            ", approxDistinct=" + approxDistinct +
            ", mcv=" + Arrays.toString(mostCommonValues.values()) +
            ", frequencies=" + Arrays.toString(mostCommonValues.frequencies()) + '}';
    }
}
