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

package io.crate.operation.collect.stats;

import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.elasticsearch.test.ESTestCase.terminate;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class TimeExpiringRamAccountingLogSinkTest {

    private ScheduledExecutorService scheduler;
    private static CrateCircuitBreakerService breakerService;

    @Before
    public void createScheduler() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void terminateScheduler() throws InterruptedException {
        terminate(scheduler);
    }

    @BeforeClass
    public static void beforeClass() {
        NodeSettingsService settingsService = new NodeSettingsService(Settings.EMPTY);
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, settingsService);
        breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, settingsService, esBreakerService);
    }

    @Test
    public void testRemoveExpiredLogs() {
        RamAccountingContext context = new RamAccountingContext("testRamAccountingContext",
            breakerService.getBreaker(CrateCircuitBreakerService.LOGS));
        TimeExpiringRamAccountingLogSink<JobContextLog> sink = new TimeExpiringRamAccountingLogSink<>(context,
            mock(ScheduledExecutorService.class), TimeValue.timeValueSeconds(5L), StatsTablesService.JOB_CONTEXT_LOG_ESTIMATOR::estimateSize);

        sink.add(new JobContextLog(new JobContext(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff01"),
            "select 1", 1L), null, 2000L));
        sink.add(new JobContextLog(new JobContext(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff02"),
            "select 1", 1L), null, 4000L));
        sink.add(new JobContextLog(new JobContext(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff03"),
            "select 1", 1L), null, 7000L));

        sink.removeExpiredLogs(10000L, 5000L);
        assertThat(sink.size(), is(1));
        assertThat(sink.iterator().next().id(), is(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff03")));
    }
}
