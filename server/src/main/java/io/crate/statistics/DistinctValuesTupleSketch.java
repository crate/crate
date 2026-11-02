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
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSetOperationBuilder;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketchIterator;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUnion;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jspecify.annotations.Nullable;

/// A streamable tuple sketch which keeps the number of occurrences of each retained hash.
/// Unions sum the occurrences per hash, so the counts stay correct across nodes.
public final class DistinctValuesTupleSketch {

    private static final int NOMINAL_ENTRIES = 16384;
    private static final double[] ONE = {1.0};

    /// `null` once the sketch was merged or streamed.
    @Nullable
    private ArrayOfDoublesUpdatableSketch updatable;
    private ArrayOfDoublesSketch sketch;

    DistinctValuesTupleSketch() {
        this.updatable = new ArrayOfDoublesUpdatableSketchBuilder()
            .setNominalEntries(NOMINAL_ENTRIES)
            .build();
        this.sketch = updatable;
    }

    private DistinctValuesTupleSketch(ArrayOfDoublesSketch sketch) {
        this.updatable = null;
        this.sketch = sketch;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(sketch.toByteArray());
    }

    public static DistinctValuesTupleSketch fromStream(StreamInput in) throws IOException {
        byte[] bytes = in.readByteArray();
        return new DistinctValuesTupleSketch(ArrayOfDoublesSketch.heapify(MemorySegment.ofArray(bytes)));
    }

    public void add(Object val) {
        if (updatable == null) {
            throw new UnsupportedOperationException("Cannot add values to a merged or streamed sketch");
        }
        switch (val) {
            case Long l -> updatable.update(l.longValue(), ONE);
            case Integer i -> updatable.update(i.longValue(), ONE);
            case Short s -> updatable.update(s.longValue(), ONE);
            case Byte b -> updatable.update(b.longValue(), ONE);
            case Double d -> updatable.update(d.doubleValue(), ONE);
            case Float f -> updatable.update(f.doubleValue(), ONE);
            default -> updatable.update(val.toString(), ONE);
        }
    }

    public DistinctValuesTupleSketch merge(DistinctValuesTupleSketch other) {
        ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder()
            .setNominalEntries(NOMINAL_ENTRIES)
            .buildUnion();
        union.union(sketch);
        union.union(other.sketch);
        this.sketch = union.getResult();
        this.updatable = null;
        return this;
    }

    /// Estimated number of distinct values.
    public double distinctValues() {
        return sketch.getEstimate();
    }

    /// Estimated number of distinct values that appear exactly once.
    public double singletons() {
        int retainedSingletons = 0;
        ArrayOfDoublesSketchIterator it = sketch.iterator();
        while (it.next()) {
            if (it.getValues()[0] == 1.0) {
                retainedSingletons++;
            }
        }
        return retainedSingletons / sketch.getTheta();
    }
}
