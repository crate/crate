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

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
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
    public abstract CpcSketch getSketch();

    /**
     * Creates a new empty sketch
     */
    public static DistinctValuesSketch newSketch() {
        return new Impl(new CpcSketch());
    }

    /**
     * Reads a sketch from a StreamInput
     */
    public static DistinctValuesSketch fromStream(StreamInput in) throws IOException {
        byte[] distinctSketchBytes = in.readByteArray();
        return new Impl(CpcSketch.heapify(distinctSketchBytes));
    }

    private static final class Impl extends DistinctValuesSketch {

        private final CpcSketch sketch;

        Impl(CpcSketch sketch) {
            this.sketch = sketch;
        }

        @Override
        public void update(String v) {
            sketch.update(v);
        }

        @Override
        public DistinctValuesSketch merge(DistinctValuesSketch other) {
            CpcUnion union = new CpcUnion();
            union.update(this.sketch);
            union.update(other.getSketch());
            return new Impl(union.getResult());
        }

        @Override
        public CpcSketch getSketch() {
            return sketch;
        }
    }

}
