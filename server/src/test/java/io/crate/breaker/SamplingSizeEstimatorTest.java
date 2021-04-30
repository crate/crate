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

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class SamplingSizeEstimatorTest {


    @Test
    public void test_sampling_size_estimator_takes_sample_on_first_estimation() {
        var estimator = new SamplingSizeEstimator<>(10, new ConstSizeEstimator(10));
        assertThat(estimator.estimateSize(20), is(10L));
    }

    @Test
    public void test_sampling_size_estimator_updates_estimate_after_nth_calls() {
        var estimator = new SamplingSizeEstimator<>(4, StringSizeEstimator.INSTANCE);
        assertThat(estimator.estimateSize("foobar"), is(56L));
        assertThat(estimator.estimateSize("a"), is(56L));
        assertThat(estimator.estimateSize("a"), is(56L));
        assertThat(estimator.estimateSize("a"), is(56L));
        assertThat(estimator.estimateSize("a"), is(48L));
        assertThat(estimator.estimateSize("a"), is(48L));
    }
}
