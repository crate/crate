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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;

import org.apache.datasketches.theta.UpdateSketch;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.types.DataType;

/**
 * Constructs a {@link ColumnSketch} from a set of samples
 */
public abstract class ColumnSketchBuilder<T> {

    protected final DataType<T> dataType;

    protected long sampleCount;
    protected int nullCount;
    protected long totalBytes;
    protected final UpdateSketch distinctSketch = UpdateSketch.builder().build();

    /**
     * Creates a new ColumnSketchBuilder for the given DataType
     */
    public ColumnSketchBuilder(DataType<T> dataType) {
        this.dataType = dataType;
    }

    /**
     * Add a sample to the sketch
     */
    public void add(T value) {
        sampleCount++;
        if (value == null) {
            nullCount++;
        } else {
            totalBytes += dataType.valueBytes(value);
            distinctSketch.update(value.toString());
            updateSketches(value);
        }
    }

    protected abstract void updateSketches(T value);

    /**
     * Add a collection of samples to the sketch
     */
    public final void addAll(Collection<T> values) {
        for (var v : values) {
            add(v);
        }
    }

    /**
     * Produce a streamable and merge-able representation of the sketch
     */
    public abstract ColumnSketch<T> toSketch();

    /**
     * An implementation for single-valued data types
     */
    // TODO - create a specialized implementation for numeric data
    public static class SingleValued<T> extends ColumnSketchBuilder<T> {

        private final MostCommonValuesSketch<T> mostCommonValuesSketch;
        private final HistogramSketch<T> histogramSketch;

        public SingleValued(Class<T> clazz, DataType<T> dataType) {
            super(dataType);
            this.mostCommonValuesSketch = new MostCommonValuesSketch<>(dataType.streamer());
            this.histogramSketch = new HistogramSketch<>(clazz, dataType);
        }

        @Override
        protected void updateSketches(T value) {
            mostCommonValuesSketch.update(value);
            histogramSketch.update(value);
        }

        @Override
        public ColumnSketch<T> toSketch() {
            return new ColumnSketch.SingleValued<>(
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

    /**
     * An implementation for array data types
     */
    // TODO - rework so that stats are produced for the individual items in the arrays
    public static class Composite<T> extends ColumnSketchBuilder<T> {

        private final MostCommonValuesSketch<BytesRef> mostCommonValuesSketch;
        private final BytesStreamOutput scratch = new BytesStreamOutput();

        public Composite(DataType<T> dataType) {
            super(dataType);
            this.mostCommonValuesSketch = new MostCommonValuesSketch<>(BYTE_ARRAY_STREAMER);
        }

        @Override
        protected void updateSketches(T value) {
            try {
                scratch.reset();
                dataType.streamer().writeValueTo(scratch, value);
                // TODO update DataSketches to take byte, offset, length
                BytesRef bv = scratch.copyBytes().toBytesRef();
                mostCommonValuesSketch.update(bv);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public ColumnSketch<T> toSketch() {
            return new ColumnSketch.Composite<>(
                dataType,
                sampleCount,
                nullCount,
                totalBytes,
                distinctSketch,
                mostCommonValuesSketch
            );
        }
    }

    public static final Streamer<BytesRef> BYTE_ARRAY_STREAMER = new Streamer<>() {
        @Override
        public BytesRef readValueFrom(StreamInput in) throws IOException {
            byte[] b = in.readByteArray();
            return new BytesRef(b);
        }

        @Override
        public void writeValueTo(StreamOutput out, BytesRef v) throws IOException {
            out.writeVInt(v.length);
            out.writeBytes(v.bytes, v.offset, v.length);
        }
    };

}
