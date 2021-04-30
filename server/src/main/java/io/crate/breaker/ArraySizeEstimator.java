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

import javax.annotation.Nullable;
import java.util.List;

public final class ArraySizeEstimator {

    public static <T> SizeEstimator<List<T>> create(SizeEstimator<T> elementEstimator) {
        if (elementEstimator instanceof ConstSizeEstimator) {
            long elementSize = ((ConstSizeEstimator) elementEstimator).size();
            return new ConstElementArraySizeEstimator<>(elementSize);
        } else {
            return new DynamicArraySizeEstimator<>(elementEstimator);
        }
    }

    private static class ConstElementArraySizeEstimator<T> extends SizeEstimator<List<T>> {
        private final long elementSize;

        ConstElementArraySizeEstimator(long elementSize) {
            this.elementSize = elementSize;
        }

        @Override
        public long estimateSize(@Nullable List<T> values) {
            if (values == null) {
                return 8;
            }
            // 16 for the container
            return 16 + values.size() * elementSize;
        }

    }

    private static class DynamicArraySizeEstimator<T> extends SizeEstimator<List<T>> {

        private final SizeEstimator<T> elementEstimator;

        DynamicArraySizeEstimator(SizeEstimator<T> elementEstimator) {
            this.elementEstimator = elementEstimator;
        }

        @Override
        public long estimateSize(@Nullable List<T> values) {
            if (values == null) {
                return 8;
            }
            long size = 16;
            for (T val : values) {
                size += elementEstimator.estimateSize(val);
            }
            return size;
        }
    }
}
