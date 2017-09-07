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

import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

public class CrateCircuitBreakerServiceTest extends CrateUnitTest {

    private ClusterSettings clusterSettings;

    @Before
    public void registerSettings() {
        SQLPlugin sqlPlugin = new SQLPlugin(Settings.EMPTY);
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.addAll(sqlPlugin.getSettings());
        clusterSettings = new ClusterSettings(Settings.EMPTY, settings);
    }

    @Test
    public void testQueryCircuitBreakerRegistration() throws Exception {
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(
            Settings.EMPTY, clusterSettings);
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, clusterSettings, esBreakerService);

        CircuitBreaker breaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        assertThat(breaker, notNullValue());
        assertThat(breaker, instanceOf(CircuitBreaker.class));
        assertThat(breaker.getName(), is(CrateCircuitBreakerService.QUERY));
    }

    @Test
    public void testQueryCircuitBreakerDynamicSettings() throws Exception {
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(
            Settings.EMPTY, clusterSettings);
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, clusterSettings, esBreakerService);

        Settings newSettings = Settings.builder()
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 2.0)
            .build();

        clusterSettings.applySettings(newSettings);

        CircuitBreaker breaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        assertThat(breaker, notNullValue());
        assertThat(breaker, instanceOf(CircuitBreaker.class));
        assertThat(breaker.getOverhead(), is(2.0));
    }

    @Test
    public void testBreakerSettingsAssignment() throws Exception {
        Settings settings = Settings.builder()
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10m")
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
            .build();
        CircuitBreakerService esBreakerService = spy(new HierarchyCircuitBreakerService(Settings.EMPTY, clusterSettings));
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(settings, clusterSettings, esBreakerService);

        CircuitBreaker breaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        assertThat(breaker.getLimit(), is(10_485_760L));
        assertThat(breaker.getOverhead(), is(1.0));

        Settings newSettings = Settings.builder()
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100m")
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 2.0)
            .build();
        clusterSettings.applySettings(newSettings);

        // expecting 4 times because registerBreaker() is also called from constructor of CrateCircuitBreakerService 3 times
        verify(esBreakerService, times(4)).registerBreaker(Matchers.any());

        breaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        assertThat(breaker.getLimit(), is(104_857_600L));
        assertThat(breaker.getOverhead(), is(2.0));

        // updating with same settings should not register a new breaker
        clusterSettings.applySettings(newSettings);

        verify(esBreakerService, times(4)).registerBreaker(Matchers.any());
    }

    @Test
    public void testBreakingExceptionMessage() throws Exception {
        String message = CrateCircuitBreakerService.breakingExceptionMessage("dummy", 1234);
        assertThat(message, is(String.format(Locale.ENGLISH, CrateCircuitBreakerService.BREAKING_EXCEPTION_MESSAGE, "dummy", 1234, new ByteSizeValue(1234))));
    }

    @Test
    public void testStats() throws Exception {
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(
            Settings.EMPTY, clusterSettings);
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, clusterSettings, esBreakerService);

        CircuitBreakerStats[] stats = breakerService.stats().getAllStats();
        assertThat(stats.length, is(7));

        CircuitBreakerStats queryBreakerStats = breakerService.stats(CrateCircuitBreakerService.QUERY);
        assertThat(queryBreakerStats.getEstimated(), is(0L));
    }

}
