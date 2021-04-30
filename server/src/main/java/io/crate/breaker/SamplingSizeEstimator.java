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

/**
 * A SizeEstimator implementation which only does a real size estimation `everyNth` `estimateSize` calls.
 *
 * This can be used to reduce the cost of size estimation if the inner SizeEstimator
 * is expensive and it is assumed that the data records have mostly the same size.
 */
public final class SamplingSizeEstimator<T> extends SizeEstimator<T> {

    private final int everyNth;
    private final SizeEstimator<T> estimator;

    private long i = 0;
    private long lastMeasurement = -1L;

    public SamplingSizeEstimator(int everyNth, SizeEstimator<T> estimator) {
        this.everyNth = everyNth;
        this.estimator = estimator;
    }

    @Override
    public long estimateSize(@Nullable T value) {
        if ((i % everyNth) == 0 || lastMeasurement == -1) {
            lastMeasurement = estimator.estimateSize(value);
        }
        i++;
        return lastMeasurement;
    }
}
