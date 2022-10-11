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

package org.elasticsearch.common.breaker;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.junit.Test;

public class ChildMemoryCircuitBreakerTest {

    @Test
    public void test_get_free_returns_max_long_if_breaking_is_disabled() {
        var breaker = new ChildMemoryCircuitBreaker(
            new BreakerSettings("test", -1, CircuitBreaker.Type.MEMORY),
            Loggers.getLogger(getClass()),
            null
        );

        assertThat(breaker.getFree()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void test_get_free_returns_zero_if_usage_is_disabled() {
        var breaker = new ChildMemoryCircuitBreaker(
            new BreakerSettings("test", 0, CircuitBreaker.Type.MEMORY),
            Loggers.getLogger(getClass()),
            null
        );

        assertThat(breaker.getFree()).isEqualTo(0L);
    }
}
