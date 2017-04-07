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

import com.carrotsearch.hppc.ObjectLongMap;
import io.crate.action.sql.SQLOperations;
import io.crate.data.RowN;
import io.crate.metadata.TableIdent;
import io.crate.metadata.settings.CrateSettings;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class TableStatsServiceTest extends CrateUnitTest {

    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = new ThreadPool("dummy");
    }

    @After
    public void clearThreadPool() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(30, TimeUnit.SECONDS);
    }

    @Test
    public void testSettingsChanges() {
        ClusterService clusterService = mock(ClusterService.class);

        // Initially disabled
        TableStatsService statsService = new TableStatsService(
            Settings.builder().put(CrateSettings.STATS_SERVICE_INTERVAL.settingName(), 0).build(),
            threadPool,
            clusterService,
            new TableStats(),
            new NodeSettingsService(Settings.EMPTY),
            mock(SQLOperations.class, Answers.RETURNS_MOCKS.get()));

        assertThat(statsService.lastRefreshInterval,
            is(TimeValue.timeValueMinutes(0)));
        assertThat(statsService.refreshScheduledTask, is(nullValue()));

        // Default setting
        statsService = new TableStatsService(
            Settings.EMPTY,
            threadPool,
            clusterService,
            new TableStats(),
            new NodeSettingsService(Settings.EMPTY),
            mock(SQLOperations.class, Answers.RETURNS_MOCKS.get()));

        assertThat(statsService.lastRefreshInterval,
            is(CrateSettings.STATS_SERVICE_INTERVAL.defaultValue()));
        assertThat(statsService.refreshScheduledTask, is(notNullValue()));

        // Update setting
        statsService.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_SERVICE_INTERVAL.settingName(), "10m").build());

        assertThat(statsService.lastRefreshInterval, is(TimeValue.timeValueMinutes(10)));
        assertThat(statsService.refreshScheduledTask,
            is(notNullValue()));

        // Disable
        statsService.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_SERVICE_INTERVAL.settingName(), 0).build());

        assertThat(statsService.lastRefreshInterval, is(TimeValue.timeValueMillis(0)));
        assertThat(statsService.refreshScheduledTask,
            is(nullValue()));

        // Reset setting
        statsService.onRefreshSettings(Settings.builder().build());

        assertThat(statsService.lastRefreshInterval,
            is(CrateSettings.STATS_SERVICE_INTERVAL.defaultValue()));
        assertThat(statsService.refreshScheduledTask, is(notNullValue()));
    }

    @Test
    public void testRowsToTableStatConversion() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<ObjectLongMap<TableIdent>> statsFuture = new CompletableFuture<>();
        TableStatsService.TableStatsResultReceiver receiver =
            new TableStatsService.TableStatsResultReceiver(statsFuture::complete);

        receiver.setNextRow(new RowN(new Object[]{1L, "custom", "foo"}));
        receiver.setNextRow(new RowN(new Object[]{2L, "doc", "foo"}));
        receiver.setNextRow(new RowN(new Object[]{3L, "bar", "foo"}));
        receiver.allFinished(false);

        ObjectLongMap<TableIdent> stats = statsFuture.get(10, TimeUnit.SECONDS);
        assertThat(stats.size(), is(3));
        assertThat(stats.get(new TableIdent("bar", "foo")), is(3L));
    }

    @Test
    public void testStatsQueriesCorrectly() throws Throwable {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(mock(DiscoveryNode.class));
        final SQLOperations sqlOperations = mock(SQLOperations.class);
        SQLOperations.SQLDirectExecutor sqlDirectExecutor = mock(SQLOperations.SQLDirectExecutor.class);
        when(sqlOperations.createSQLDirectExecutor(
            eq("sys"),
            eq(TableStatsService.TABLE_STATS),
            eq(TableStatsService.STMT),
            eq(TableStatsService.DEFAULT_SOFT_LIMIT)))
            .thenReturn(sqlDirectExecutor);

        TableStatsService statsService = new TableStatsService(
            Settings.EMPTY,
            threadPool,
            clusterService,
            new TableStats(),
            new NodeSettingsService(Settings.EMPTY),
            sqlOperations
        );
        statsService.run();

        verify(sqlDirectExecutor, times(1)).execute(
            any(TableStatsService.TableStatsResultReceiver.class),
            eq(Collections.emptyList()));
    }

    @Test
    public void testNoUpdateIfLocalNodeNotAvailable() throws Exception {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(null);
        SQLOperations sqlOperations = mock(SQLOperations.class);
        SQLOperations.Session session = mock(SQLOperations.Session.class);
        when(sqlOperations.createSession(anyString(), any(), anyInt())).thenReturn(session);


        TableStatsService statsService = new TableStatsService(
            Settings.EMPTY,
            threadPool,
            clusterService,
            new TableStats(),
            new NodeSettingsService(Settings.EMPTY),
            sqlOperations
        );

        statsService.run();
        Mockito.verify(session, times(0)).sync();
    }
}
