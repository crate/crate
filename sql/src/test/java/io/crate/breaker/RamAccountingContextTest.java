/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.breaker;

import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateUnitTest;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class RamAccountingContextTest extends CrateUnitTest {

    private long originalBufferSize;
    private RamAccountingContext ramAccountingContext;
    private ChildMemoryCircuitBreaker breaker;

    @Before
    public void reduceFlushBufferSize() {
        originalBufferSize = RamAccountingContext.FLUSH_BUFFER_SIZE;
        RamAccountingContext.FLUSH_BUFFER_SIZE = 20;

        SQLPlugin sqlPlugin = new SQLPlugin(Settings.EMPTY);
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.addAll(sqlPlugin.getSettings());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settings);

        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(
            Settings.EMPTY, clusterSettings);
        CrateCircuitBreakerService breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, clusterSettings, esBreakerService);        BreakerSettings breakerSettings = new BreakerSettings(
            "query", 40, 1.01f, CircuitBreaker.Type.MEMORY);

        breaker = new ChildMemoryCircuitBreaker(
            breakerSettings, LogManager.getLogger(RamAccountingContextTest.class), breakerService, breakerSettings.getName());
        ramAccountingContext = new RamAccountingContext("test", breaker);
    }

    @After
    public void resetFlushBufferSize() {
        RamAccountingContext.FLUSH_BUFFER_SIZE = originalBufferSize;
    }

    @Test
    public void testTotalBytesIsNotResetOnClose() {
        ramAccountingContext.addBytes(20);
        ramAccountingContext.addBytesWithoutBreaking(10);
        ramAccountingContext.close();

        assertThat(ramAccountingContext.totalBytes(), is(30L));
        assertThat(breaker.getUsed(), is(0L));
    }

    @Test
    public void testTotalBytesIsResetAfterRelease() {
        ramAccountingContext.addBytes(20);
        ramAccountingContext.addBytesWithoutBreaking(10);
        ramAccountingContext.release();

        assertThat(ramAccountingContext.totalBytes(), is(0L));
        assertThat(breaker.getUsed(), is(0L));
    }

    @Test
    public void testTotalWhenAddingBytesAfterRelease() {
        ramAccountingContext.addBytes(20);
        ramAccountingContext.addBytesWithoutBreaking(10);
        ramAccountingContext.release();

        ramAccountingContext.addBytes(20);

        assertThat(ramAccountingContext.totalBytes(), is(20L));
        assertThat(breaker.getUsed(), is(20L));
    }

    @Test
    public void testMultipleReleases() {
        ramAccountingContext.addBytes(20);
        ramAccountingContext.release();

        ramAccountingContext.addBytes(10);
        ramAccountingContext.release();

        assertThat(ramAccountingContext.totalBytes(), is(0L));
        assertThat(breaker.getUsed(), is(0L));
    }
}
