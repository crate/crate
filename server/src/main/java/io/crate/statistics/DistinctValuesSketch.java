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

/**
 * A streamable representation of a distinct values sketch
 */
public abstract class DistinctValuesSketch {

    private static final int NOMINAL_ENTRIES = 4096;
    private static final double[] ONE = {1.0};

    /**
     * Add a new value to the sketch
     */
    public abstract void update(String v);

    /**
     * Merge this sketch with another
     */
    public abstract DistinctValuesSketch merge(DistinctValuesSketch other);

    /**
     * Get the internal sketch data structure
     */
    public abstract ArrayOfDoublesSketch getSketch();

    /**
     * Estimated number of distinct values that were added exactly once
     */
    public double singletons() {
        ArrayOfDoublesSketch sketch = getSketch();
        int retainedSingletons = 0;
        ArrayOfDoublesSketchIterator it = sketch.iterator();
        while (it.next()) {
            if (it.getValues()[0] == 1.0) {
                retainedSingletons++;
            }
        }
        return retainedSingletons / sketch.getTheta();
    }

    /**
     * Creates a new empty sketch
     */
    public static DistinctValuesSketch newSketch() {
        return new DistinctValuesSketch() {

            final ArrayOfDoublesUpdatableSketch distinctSketch = new ArrayOfDoublesUpdatableSketchBuilder()
                .setNominalEntries(NOMINAL_ENTRIES)
                .build();

            @Override
            public void update(String v) {
                distinctSketch.update(v, ONE);
            }

            @Override
            public DistinctValuesSketch merge(DistinctValuesSketch other) {
                ArrayOfDoublesUnion union = newUnion();
                union.union(distinctSketch);
                union.union(other.getSketch());
                return mergedSketch(union);
            }

            @Override
            public ArrayOfDoublesSketch getSketch() {
                return distinctSketch;
            }
        };
    }

    private static ArrayOfDoublesUnion newUnion() {
        return new ArrayOfDoublesSetOperationBuilder()
            .setNominalEntries(NOMINAL_ENTRIES)
            .buildUnion();
    }

    private static DistinctValuesSketch mergedSketch(ArrayOfDoublesUnion union) {
        return new DistinctValuesSketch() {
            @Override
            public void update(String v) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DistinctValuesSketch merge(DistinctValuesSketch other) {
                union.union(other.getSketch());
                return mergedSketch(union);
            }

            @Override
            public ArrayOfDoublesSketch getSketch() {
                return union.getResult();
            }
        };
    }

    /**
     * Reads a sketch from a StreamInput
     */
    public static DistinctValuesSketch fromStream(StreamInput in) throws IOException {
        byte[] distinctSketchBytes = in.readByteArray();
        var sketch = ArrayOfDoublesSketch.heapify(MemorySegment.ofArray(distinctSketchBytes));
        return new DistinctValuesSketch() {
            @Override
            public void update(String v) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DistinctValuesSketch merge(DistinctValuesSketch other) {
                ArrayOfDoublesUnion union = newUnion();
                union.union(sketch);
                union.union(other.getSketch());
                return mergedSketch(union);
            }

            @Override
            public ArrayOfDoublesSketch getSketch() {
                return sketch;
            }
        };
    }

}
