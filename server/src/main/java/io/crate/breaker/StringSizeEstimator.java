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

package io.crate.breaker;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

import javax.annotation.Nullable;

public final class StringSizeEstimator extends SizeEstimator<String> {

    public static final StringSizeEstimator INSTANCE = new StringSizeEstimator();
    /**
     * In this base value, we account for the {@link BytesRef} object itself as
     * well as the header of the byte[] array it holds, and some lost bytes due
     * to object alignment. So consumers of this constant just have to add the
     * length of the byte[] (assuming it is not shared between multiple
     * instances). */
    private static final long BASE_BYTES_PER_BYTES_REF =
        // shallow memory usage of the BytesRef object
        RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) +
        // header of the byte[] array
        RamUsageEstimator.NUM_BYTES_ARRAY_HEADER +
        // with an alignment size (-XX:ObjectAlignmentInBytes) of 8 (default),
        // there could be between 0 and 7 lost bytes, so we account for 3
        // lost bytes on average
        3;

    private StringSizeEstimator() {
    }

    public static long estimate(@Nullable String value) {
        return RamUsageEstimator.sizeOf(value);
    }

    @Override
    public long estimateSize(@Nullable String value) {
        return RamUsageEstimator.sizeOf(value);
    }

    public static long estimateSize(@Nullable BytesRef value) {
        if (value == null) {
            return 8;
        }
        return RamUsageEstimator.alignObjectSize(BASE_BYTES_PER_BYTES_REF + value.length);
    }
}
