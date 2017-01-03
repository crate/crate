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

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.Locale;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CrateCircuitBreakerServiceTest extends CrateUnitTest {

    @Test
    public void testQueryCircuitBreakerRegistration() throws Exception {
        NodeSettingsService settingsService = new NodeSettingsService(Settings.EMPTY);
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, settingsService);
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, settingsService, esBreakerService);

        CircuitBreaker breaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        assertThat(breaker, notNullValue());
        assertThat(breaker, instanceOf(CircuitBreaker.class));
        assertThat(breaker.getName(), is(CrateCircuitBreakerService.QUERY));
    }

    @Test
    public void testQueryCircuitBreakerDynamicSettings() throws Exception {
        final NodeSettingsService.Listener[] listeners = new NodeSettingsService.Listener[1];
        NodeSettingsService settingsService = new NodeSettingsService(Settings.EMPTY) {
            @Override
            public void addListener(Listener listener) {
                listeners[0] = listener;
            }
        };
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, settingsService);
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, settingsService, esBreakerService);

        Settings newSettings = Settings.settingsBuilder()
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING, 2.0)
            .build();

        listeners[0].onRefreshSettings(newSettings);

        CircuitBreaker breaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        assertThat(breaker, notNullValue());
        assertThat(breaker, instanceOf(CircuitBreaker.class));
        assertThat(breaker.getOverhead(), is(2.0));
    }

    @Test
    public void testBreakerSettingsAssignment() throws Exception {
        Settings settings = Settings.builder()
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING, "10m")
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING, 1.0)
            .build();
        final NodeSettingsService.Listener[] listeners = new NodeSettingsService.Listener[1];
        NodeSettingsService settingsService = new NodeSettingsService(settings) {
            @Override
            public void addListener(Listener listener) {
                listeners[0] = listener;
            }
        };
        CircuitBreakerService esBreakerService = spy(new HierarchyCircuitBreakerService(Settings.EMPTY, settingsService));
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(settings, settingsService, esBreakerService);

        CircuitBreaker breaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        assertThat(breaker.getLimit(), is(10_485_760L));
        assertThat(breaker.getOverhead(), is(1.0));

        Settings newSettings = Settings.settingsBuilder()
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING, "100m")
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING, 2.0)
            .build();

        listeners[0].onRefreshSettings(newSettings);
        // expecting 4 times because registerBreaker() is also called from constructor of CrateCircuitBreakerService 3 times
        verify(esBreakerService, times(4)).registerBreaker(Matchers.any());

        breaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        assertThat(breaker.getLimit(), is(104_857_600L));
        assertThat(breaker.getOverhead(), is(2.0));

        // updating with same settings should not register a new breaker
        listeners[0].onRefreshSettings(newSettings);
        verify(esBreakerService, times(4)).registerBreaker(Matchers.any());
    }

    @Test
    public void testBreakingExceptionMessage() throws Exception {
        String message = CrateCircuitBreakerService.breakingExceptionMessage("dummy", 1234);
        assertThat(message, is(String.format(Locale.ENGLISH, CrateCircuitBreakerService.BREAKING_EXCEPTION_MESSAGE, "dummy", 1234, new ByteSizeValue(1234))));
    }

    @Test
    public void testStats() throws Exception {
        NodeSettingsService settingsService = new NodeSettingsService(Settings.EMPTY);
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, settingsService);
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, settingsService, esBreakerService);

        CircuitBreakerStats[] stats = breakerService.stats().getAllStats();
        assertThat(stats.length, is(7));

        CircuitBreakerStats queryBreakerStats = breakerService.stats(CrateCircuitBreakerService.QUERY);
        assertThat(queryBreakerStats.getEstimated(), is(0L));
    }

}
