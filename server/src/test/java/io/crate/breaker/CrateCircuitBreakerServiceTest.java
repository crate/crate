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

import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.junit.Test;

import java.util.Collection;
import java.util.Locale;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CrateCircuitBreakerServiceTest extends CrateDummyClusterServiceUnitTest {

    @Override
    protected Collection<Setting<?>> additionalClusterSettings() {
        SQLPlugin sqlPlugin = new SQLPlugin(Settings.EMPTY);
        return Sets.newHashSet(sqlPlugin.getSettings());
    }

    @Test
    public void testQueryCircuitBreakerRegistration() throws Exception {
        CircuitBreakerService breakerService = new HierarchyCircuitBreakerService(
            Settings.EMPTY,
            clusterService.getClusterSettings()
        );

        CircuitBreaker breaker = breakerService.getBreaker(HierarchyCircuitBreakerService.QUERY);
        assertThat(breaker, notNullValue());
        assertThat(breaker, instanceOf(CircuitBreaker.class));
        assertThat(breaker.getName(), is(HierarchyCircuitBreakerService.QUERY));
    }

    @Test
    public void testQueryBreakerAssignment() throws Exception {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10m")
            .build();
        HierarchyCircuitBreakerService breakerService = new HierarchyCircuitBreakerService(
            settings,
            clusterService.getClusterSettings()
        );

        CircuitBreaker breaker = breakerService.getBreaker(HierarchyCircuitBreakerService.QUERY);
        assertThat(breaker.getLimit(), is(10_485_760L));

        Settings newSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100m")
            .build();
        clusterService.getClusterSettings().applySettings(newSettings);

        breaker = breakerService.getBreaker(HierarchyCircuitBreakerService.QUERY);
        assertThat(breaker.getLimit(), is(104_857_600L));
    }

    @Test
    public void testStatsBreakerAssignment() throws Exception {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10m")
            .put(HierarchyCircuitBreakerService.OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10m")
            .build();
        CircuitBreakerService breakerService = new HierarchyCircuitBreakerService(
            settings,
            clusterService.getClusterSettings()
        );
        CircuitBreaker breaker;

        breaker = breakerService.getBreaker(HierarchyCircuitBreakerService.JOBS_LOG);
        assertThat(breaker.getLimit(), is(10_485_760L));
        breaker = breakerService.getBreaker(HierarchyCircuitBreakerService.OPERATIONS_LOG);
        assertThat(breaker.getLimit(), is(10_485_760L));

        Settings newSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100m")
            .put(HierarchyCircuitBreakerService.OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100m")
            .build();
         clusterService.getClusterSettings().applySettings(newSettings);

        breaker = breakerService.getBreaker(HierarchyCircuitBreakerService.JOBS_LOG);
        assertThat(breaker.getLimit(), is(104_857_600L));
        breaker = breakerService.getBreaker(HierarchyCircuitBreakerService.OPERATIONS_LOG);
        assertThat(breaker.getLimit(), is(104_857_600L));
    }

    @Test
    public void testBreakingExceptionMessage() throws Exception {
        String message = HierarchyCircuitBreakerService.breakingExceptionMessage("dummy", 1234);
        assertThat(message, is(String.format(Locale.ENGLISH, HierarchyCircuitBreakerService.BREAKING_EXCEPTION_MESSAGE, "dummy", 1234, new ByteSizeValue(1234))));
    }

    @Test
    public void testStats() throws Exception {
        CircuitBreakerService breakerService = new HierarchyCircuitBreakerService(
            Settings.EMPTY, clusterService.getClusterSettings());

        CircuitBreakerStats queryBreakerStats = breakerService.stats(HierarchyCircuitBreakerService.QUERY);
        assertThat(queryBreakerStats.getUsed(), is(0L));
    }
}
