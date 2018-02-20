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

package io.crate.planner;

import com.carrotsearch.hppc.ObjectObjectMap;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.data.RowN;
import io.crate.metadata.TableIdent;
import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;
import org.mockito.Answers;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableStatsServiceTest extends CrateDummyClusterServiceUnitTest {

    @Override
    protected Collection<Setting<?>> additionalClusterSettings() {
        return new SQLPlugin(Settings.EMPTY).getSettings();
    }

    @Test
    public void testSettingsChanges() {
        // Initially disabled
        TableStatsService statsService = new TableStatsService(
            Settings.builder().put(TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING.getKey(), 0).build(),
            THREAD_POOL,
            clusterService,
            new TableStats(),
            mock(SQLOperations.class, Answers.RETURNS_MOCKS.get()));

        assertThat(statsService.refreshInterval,
            is(TimeValue.timeValueMinutes(0)));
        assertThat(statsService.refreshScheduledTask, is(nullValue()));

        // Default setting
        statsService = new TableStatsService(
            Settings.EMPTY,
            THREAD_POOL,
            clusterService,
            new TableStats(),
            mock(SQLOperations.class, Answers.RETURNS_MOCKS.get()));

        assertThat(statsService.refreshInterval,
            is(TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING.getDefault()));
        assertThat(statsService.refreshScheduledTask, is(notNullValue()));

        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        // Update setting
        clusterSettings.applySettings(Settings.builder()
            .put(TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING.getKey(), "10m").build());

        assertThat(statsService.refreshInterval, is(TimeValue.timeValueMinutes(10)));
        assertThat(statsService.refreshScheduledTask,
            is(notNullValue()));

        // Disable
        clusterSettings.applySettings(Settings.builder()
            .put(TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING.getKey(), 0).build());

        assertThat(statsService.refreshInterval, is(TimeValue.timeValueMillis(0)));
        assertThat(statsService.refreshScheduledTask,
            is(nullValue()));

        // Reset setting
        clusterSettings.applySettings(Settings.builder().build());

        assertThat(statsService.refreshInterval,
            is(TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING.getDefault()));
        assertThat(statsService.refreshScheduledTask, is(notNullValue()));
    }

    @Test
    public void testRowsToTableStatConversion() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<ObjectObjectMap<TableIdent, TableStats.Stats>> statsFuture = new CompletableFuture<>();
        TableStatsService.TableStatsResultReceiver receiver =
            new TableStatsService.TableStatsResultReceiver(statsFuture::complete);

        receiver.setNextRow(new RowN(new Object[]{0L, 10L, "empty", "foo"}));
        receiver.setNextRow(new RowN(new Object[]{1L, 10L, "custom", "foo"}));
        receiver.setNextRow(new RowN(new Object[]{2L, 20L, "doc", "foo"}));
        receiver.setNextRow(new RowN(new Object[]{3L, 30L, "bar", "foo"}));
        receiver.allFinished(false);

        ObjectObjectMap<TableIdent, TableStats.Stats> stats = statsFuture.get(10, TimeUnit.SECONDS);
        assertThat(stats.size(), is(4));
        TableStats.Stats statValues = stats.get(new TableIdent("bar", "foo"));
        assertThat(statValues.numDocs, is(3L));
        assertThat(statValues.sizeInBytes, is(30L));

        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(stats);
        assertThat(tableStats.estimatedSizePerRow(new TableIdent("bar", "foo")), is(10L));
        assertThat(tableStats.estimatedSizePerRow(new TableIdent("empty", "foo")), is(0L));
        assertThat(tableStats.estimatedSizePerRow(new TableIdent("notInCache", "foo")), is(-1L));
    }

    @Test
    public void testStatsQueriesCorrectly() {
        SQLOperations sqlOperations = mock(SQLOperations.class);
        Session session = mock(Session.class);
        when(sqlOperations.newSystemSession()).thenReturn(session);

        TableStatsService statsService = new TableStatsService(
            Settings.EMPTY,
            THREAD_POOL,
            clusterService,
            new TableStats(),
            sqlOperations
        );
        statsService.run();

        verify(session, times(1)).quickExec(eq(TableStatsService.STMT), any(), any(), any());
    }

    @Test
    public void testNoUpdateIfLocalNodeNotAvailable() {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(null);
        when(clusterService.getClusterSettings()).thenReturn(this.clusterService.getClusterSettings());
        SQLOperations sqlOperations = mock(SQLOperations.class);
        Session session = mock(Session.class);
        when(sqlOperations.createSession(anyString(), anyObject(), any(), anyInt())).thenReturn(session);

        TableStatsService statsService = new TableStatsService(
            Settings.EMPTY,
            THREAD_POOL,
            clusterService,
            new TableStats(),
            sqlOperations
        );

        statsService.run();
        verify(session, times(0)).sync();
    }
}
