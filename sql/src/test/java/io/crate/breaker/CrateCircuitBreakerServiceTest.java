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

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.junit.Test;

import java.util.Locale;

import static org.hamcrest.Matchers.*;

public class CrateCircuitBreakerServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testQueryCircuitBreakerRegistration() throws Exception {
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(
            Settings.EMPTY, dummyClusterService.getClusterSettings());
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, dummyClusterService.getClusterSettings(), esBreakerService);

        CircuitBreaker breaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        assertThat(breaker, notNullValue());
        assertThat(breaker, instanceOf(CircuitBreaker.class));
        assertThat(breaker.getName(), is(CrateCircuitBreakerService.QUERY));
    }

    @Test
    public void testQueryCircuitBreakerDynamicSettings() throws Exception {
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(
            Settings.EMPTY, dummyClusterService.getClusterSettings());
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, dummyClusterService.getClusterSettings(), esBreakerService);

        fail("TODO: update test");
        Settings newSettings = Settings.builder()
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING, 2.0)
            .build();

        //listeners[0].onRefreshSettings(newSettings);

        CircuitBreaker breaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        assertThat(breaker, notNullValue());
        assertThat(breaker, instanceOf(CircuitBreaker.class));
        assertThat(breaker.getOverhead(), is(2.0));
    }

    @Test
    public void testBreakingExceptionMessage() throws Exception {
        String message = CrateCircuitBreakerService.breakingExceptionMessage("dummy", 1234);
        assertThat(message, is(String.format(Locale.ENGLISH, CrateCircuitBreakerService.BREAKING_EXCEPTION_MESSAGE, "dummy", 1234, new ByteSizeValue(1234))));
    }

    @Test
    public void testStats() throws Exception {
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(
            Settings.EMPTY, dummyClusterService.getClusterSettings());
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, dummyClusterService.getClusterSettings(), esBreakerService);

        CircuitBreakerStats[] stats = breakerService.stats().getAllStats();
        assertThat(stats.length, is(5));

        CircuitBreakerStats queryBreakerStats = breakerService.stats(CrateCircuitBreakerService.QUERY);
        assertThat(queryBreakerStats.getEstimated(), is(0L));
    }

}
