/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.breaker;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

import javax.annotation.Nullable;

public final class StringSizeEstimator extends SizeEstimator<String> {

    public static final StringSizeEstimator INSTANCE = new StringSizeEstimator();
    private static final int BYTES_REF_INSTANCE_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

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
        return RamUsageEstimator.alignObjectSize(BYTES_REF_INSTANCE_SIZE + value.length);
    }
}
