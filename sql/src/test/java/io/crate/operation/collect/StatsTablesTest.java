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

package io.crate.operation.collect;

import io.crate.core.collections.NonBlockingArrayQueue;
import io.crate.core.collections.NoopQueue;
import io.crate.metadata.settings.CrateSettings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class StatsTablesTest {


    @Test
    public void testSettingsChanges() {
        NodeSettingsService nodeSettingsService = new NodeSettingsService(ImmutableSettings.EMPTY);
        StatsTables stats = new StatsTables(ImmutableSettings.EMPTY, nodeSettingsService);

        assertThat(stats.isEnabled(), is(false));
        assertThat(stats.lastJobsLogSize, is(CrateSettings.JOBS_LOG_SIZE.defaultValue()));
        assertThat(stats.lastOperationsLogSize, is(CrateSettings.OPERATIONS_LOG_SIZE.defaultValue()));

        // even though logSizes are > 0 it must be a NoopQueue because the stats are disabled
        assertThat(stats.jobsLog.get(), Matchers.instanceOf(NoopQueue.class));

        stats.listener.onRefreshSettings(ImmutableSettings.builder()
                .put(CrateSettings.COLLECT_STATS.settingName(), true)
                .put(CrateSettings.OPERATIONS_LOG_SIZE.settingName(), 200).build());

        assertThat(stats.isEnabled(), is(true));
        assertThat(stats.lastJobsLogSize, is(CrateSettings.JOBS_LOG_SIZE.defaultValue()));
        assertThat(stats.lastOperationsLogSize, is(200));

        assertThat(stats.jobsLog.get(), Matchers.instanceOf(NonBlockingArrayQueue.class));


        stats.listener.onRefreshSettings(ImmutableSettings.builder()
                .put(CrateSettings.COLLECT_STATS.settingName(), false).build());

        // logs got wiped:
        assertThat(stats.jobsLog.get(), Matchers.instanceOf(NoopQueue.class));
        assertThat(stats.isEnabled(), is(false));
    }
}