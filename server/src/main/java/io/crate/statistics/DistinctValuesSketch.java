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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.elasticsearch.common.io.stream.StreamInput;

/**
 * A streamable representation of a distinct values sketch
 */
public abstract class DistinctValuesSketch {

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
    public abstract Sketch getSketch();

    /**
     * Creates a new empty sketch
     */
    public static DistinctValuesSketch newSketch() {
        return new DistinctValuesSketch() {

            final UpdateSketch distinctSketch = UpdateSketch.builder().build();

            @Override
            public void update(String v) {
                distinctSketch.update(v);
            }

            @Override
            public DistinctValuesSketch merge(DistinctValuesSketch other) {
                Union union = SetOperation.builder().buildUnion();
                union.union(distinctSketch);
                union.union(other.getSketch());
                return mergedSketch(union);
            }

            @Override
            public Sketch getSketch() {
                return distinctSketch;
            }
        };
    }

    private static DistinctValuesSketch mergedSketch(Union union) {
        return new DistinctValuesSketch() {
            @Override
            public void update(String v) {
                union.update(v);
            }

            @Override
            public DistinctValuesSketch merge(DistinctValuesSketch other) {
                union.union(other.getSketch());
                return mergedSketch(union);
            }

            @Override
            public Sketch getSketch() {
                return union.getResult();
            }
        };
    }

    /**
     * Reads a sketch from a StreamInput
     */
    public static DistinctValuesSketch fromStream(StreamInput in) throws IOException {
        byte[] distinctSketchBytes = in.readByteArray();
        var sketch = Sketches.wrapSketch(Memory.wrap(distinctSketchBytes));
        return new DistinctValuesSketch() {
            @Override
            public void update(String v) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DistinctValuesSketch merge(DistinctValuesSketch other) {
                Union union = SetOperation.builder().buildUnion();
                union.union(sketch);
                union.union(other.getSketch());
                return mergedSketch(union);
            }

            @Override
            public Sketch getSketch() {
                return sketch;
            }
        };
    }

}
