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

package io.crate.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import io.crate.action.sql.Session;
import io.crate.action.sql.Sessions;
import io.crate.common.unit.TimeValue;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class TableStatsServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testSettingsChanges() {
        // Initially disabled
        TableStatsService statsService = new TableStatsService(
            Settings.builder().put(TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING.getKey(), 0).build(),
            THREAD_POOL,
            clusterService,
            Mockito.mock(Sessions.class, Answers.RETURNS_MOCKS));

        assertThat(statsService.refreshInterval).isEqualTo(TimeValue.timeValueMinutes(0));
        assertThat(statsService.scheduledRefresh).isNull();

        // Default setting
        statsService = new TableStatsService(
            Settings.EMPTY,
            THREAD_POOL,
            clusterService,
            Mockito.mock(Sessions.class, Answers.RETURNS_MOCKS));

        assertThat(statsService.refreshInterval)
            .isEqualTo(TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING.getDefault(Settings.EMPTY));
        assertThat(statsService.scheduledRefresh).isNotNull();

        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        // Update setting
        clusterSettings.applySettings(Settings.builder()
            .put(TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING.getKey(), "10m").build());

        assertThat(statsService.refreshInterval).isEqualTo(TimeValue.timeValueMinutes(10));
        assertThat(statsService.scheduledRefresh).isNotNull();

        // Disable
        clusterSettings.applySettings(Settings.builder()
            .put(TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING.getKey(), 0).build());

        assertThat(statsService.refreshInterval).isEqualTo(TimeValue.timeValueMillis(0));
        assertThat(statsService.scheduledRefresh).isNull();

        // Reset setting
        clusterSettings.applySettings(Settings.builder().build());

        assertThat(statsService.refreshInterval)
            .isEqualTo(TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING.getDefault(Settings.EMPTY));
        assertThat(statsService.scheduledRefresh).isNotNull();
    }

    @Test
    public void testStatsQueriesCorrectly() {
        Sessions sqlOperations = Mockito.mock(Sessions.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(sqlOperations.newSystemSession()).thenReturn(session);

        TableStatsService statsService = new TableStatsService(
            Settings.EMPTY,
            THREAD_POOL,
            clusterService,
            sqlOperations
        );
        statsService.run();

        Mockito.verify(session, Mockito.times(1)).quickExec(ArgumentMatchers.eq(TableStatsService.STMT), ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    @Test
    public void testNoUpdateIfLocalNodeNotAvailable() {
        final ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.localNode()).thenReturn(null);
        Mockito.when(clusterService.getClusterSettings()).thenReturn(this.clusterService.getClusterSettings());
        Sessions sqlOperations = Mockito.mock(Sessions.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(sqlOperations.newSession(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(session);

        TableStatsService statsService = new TableStatsService(
            Settings.EMPTY,
            THREAD_POOL,
            clusterService,
            sqlOperations
        );

        statsService.run();
        Mockito.verify(session, Mockito.times(0)).sync();
    }
}
