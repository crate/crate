/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.breaker;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class ChildMemoryCircuitBreakerTest {

    @Test
    public void test_add_bytes_and_range_returns_lower_bytes_than_wanted_if_free_is_below_wanted() throws Exception {
        var breaker = new ChildMemoryCircuitBreaker(
            new BreakerSettings(
                "dummy",
                500L,
                1.0d,
                CircuitBreaker.Type.MEMORY
            ),
            LogManager.getLogger(ChildMemoryCircuitBreakerTest.class),
            new NoneCircuitBreakerService(),
            "dummy"
        );
        breaker.addEstimateBytesAndMaybeBreak(400L, "reserve 400");
        long reserved = breaker.addBytesRangeAndMaybeBreak(50L, 300L, "reserve another 300");
        assertThat(reserved, is(100L));
    }

    @Test
    public void test_add_bytes_and_range_returns_trips_if_free_is_below_min_acceptable_bytes() throws Exception {
        var breaker = new ChildMemoryCircuitBreaker(
            new BreakerSettings(
                "dummy",
                500L,
                1.0d,
                CircuitBreaker.Type.MEMORY
            ),
            LogManager.getLogger(ChildMemoryCircuitBreakerTest.class),
            new NoneCircuitBreakerService(),
            "dummy"
        );
        breaker.addEstimateBytesAndMaybeBreak(400L, "reserve 400");
        Assertions.assertThrows(
            CircuitBreakingException.class,
            () -> breaker.addBytesRangeAndMaybeBreak(200L, 300L, "reserve another 300")
        );
    }

    @Test
    public void test_add_bytes_range_where_wanted_bytes_is_lower_than_min_acceptable_bytes_is_illegal() throws Exception {
        var breaker = new ChildMemoryCircuitBreaker(
            new BreakerSettings(
                "dummy",
                500L,
                1.0d,
                CircuitBreaker.Type.MEMORY
            ),
            LogManager.getLogger(ChildMemoryCircuitBreakerTest.class),
            new NoneCircuitBreakerService(),
            "dummy"
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> breaker.addBytesRangeAndMaybeBreak(100L, 50L, "I need memory")
        );
    }
}
