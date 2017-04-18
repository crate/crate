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

package io.crate.operation.reference.sys.cluster;

import io.crate.cluster.gracefulstop.DecommissioningService;
import io.crate.core.collections.StringObjectMaps;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.collect.stats.JobsLogService;
import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterSettingsExpressionTest extends CrateDummyClusterServiceUnitTest {

    @Override
    protected Collection<Setting<?>> additionalClusterSettings() {
        return new SQLPlugin(Settings.EMPTY).getSettings();
    }

    @Test
    public void testSettingsAreAppliedImmediately() throws Exception {
        Settings settings = Settings.builder().put("bulk.request_timeout", "20s").build();
        // build cluster service mock to pass initial settings
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(settings);

        ClusterSettingsExpression clusterSettingsExpression = new ClusterSettingsExpression(
            clusterService, new CrateSettings(clusterService));

        assertThat(((BytesRef) clusterSettingsExpression
                .getChildImplementation("bulk")
                .getChildImplementation("request_timeout").value()).utf8ToString(),
            is("20s"));
    }

    @Test
    public void testSettingsAreUpdated() throws Exception {
        ClusterSettingsExpression expression = new ClusterSettingsExpression(
            clusterService, new CrateSettings(clusterService));

        Settings settings = Settings.builder()
            .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 1)
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), false)
            .put(DecommissioningService.GRACEFUL_STOP_MIN_AVAILABILITY_SETTING.getKey(), "full")
            .build();
        CountDownLatch latch = new CountDownLatch(1);
        clusterService.addListener(event -> latch.countDown());
        clusterService.submitStateUpdateTask("update settings", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState).metaData(MetaData.builder().transientSettings(settings)).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                fail(e.getMessage());
            }
        });
        latch.await(5, TimeUnit.SECONDS);

        Map<String, Object> values = expression.value();
        assertThat(
            StringObjectMaps.getByPath(values, JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey()), is(1));

        assertThat(
            StringObjectMaps.getByPath(values, JobsLogService.STATS_ENABLED_SETTING.getKey()), is(false));

        assertThat(
            StringObjectMaps.getByPath(values, DecommissioningService.GRACEFUL_STOP_MIN_AVAILABILITY_SETTING.getKey()), is("FULL"));
    }
}
