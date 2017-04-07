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
import com.google.common.collect.ImmutableList;
import io.crate.action.sql.Option;
import io.crate.action.sql.SQLOperations;
import io.crate.metadata.TableIdent;
import io.crate.metadata.settings.CrateSettings;
import io.crate.protocols.postgres.FormatCodes;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.mockito.Matchers.anyByte;
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
            new NodeSettingsService(Settings.EMPTY),
            () -> mock(SQLOperations.class));

        assertThat(statsService.lastRefreshInterval,
            is(TimeValue.timeValueMinutes(0)));
        assertThat(statsService.refreshScheduledTask, is(nullValue()));

        // Default setting
        statsService = new TableStatsService(
            Settings.EMPTY,
            threadPool,
            clusterService,
            new NodeSettingsService(Settings.EMPTY),
            () -> mock(SQLOperations.class));

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
    public void testRowsToTableStatConversion() {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(mock(DiscoveryNode.class));

        final TableStatsService statsService = new TableStatsService(
            Settings.EMPTY,
            threadPool,
            clusterService,
            new NodeSettingsService(Settings.EMPTY),
            new Provider<SQLOperations>() {
                @Override
                public SQLOperations get() {
                    return mock(SQLOperations.class);
                }
            });
        ObjectLongMap<TableIdent> stats = statsService.statsFromRows(ImmutableList.of(
            new Object[]{1L, "custom", "foo"},
            new Object[]{2L, "doc", "foo"},
            new Object[]{3L, "bar", "foo"}));
        assertThat(stats.size(), is(3));
        assertThat(stats.get(new TableIdent("bar", "foo")), is(3L));
    }

    @Test
    public void testStatsQueriesCorrectly() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(mock(DiscoveryNode.class));
        final SQLOperations sqlOperations = mock(SQLOperations.class);
        SQLOperations.Session session = mock(SQLOperations.Session.class);
        when(sqlOperations.createSession(eq("sys"), eq(Option.NONE), eq(TableStatsService.DEFAULT_SOFT_LIMIT)))
            .thenReturn(session);

        TableStatsService statsService = new TableStatsService(
            Settings.EMPTY,
            threadPool,
            clusterService,
            new NodeSettingsService(Settings.EMPTY),
            new Provider<SQLOperations>() {
                @Override
                public SQLOperations get() {
                    return sqlOperations;
                }
            });
        statsService.run();

        verify(session, times(1)).parse(
            eq(TableStatsService.UNNAMED),
            eq(TableStatsService.STMT),
            eq(Collections.<DataType>emptyList()));
        verify(session, times(1)).bind(
            eq(TableStatsService.UNNAMED),
            eq(TableStatsService.UNNAMED),
            eq(Collections.emptyList()),
            isNull(FormatCodes.FormatCode[].class));
        verify(session, times(1)).execute(
            eq(TableStatsService.UNNAMED),
            eq(0),
            any(TableStatsService.TableStatsResultReceiver.class));
        verify(session, times(1)).sync();
    }

    @Test
    public void testNoUpdateIfLocalNodeNotAvailable() throws Exception {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(null);
        final SQLOperations sqlOperations = mock(SQLOperations.class);

        TableStatsService statsService = new TableStatsService(
            Settings.EMPTY,
            threadPool,
            clusterService,
            new NodeSettingsService(Settings.EMPTY),
            new Provider<SQLOperations>() {
                @Override
                public SQLOperations get() {
                    return sqlOperations;
                }
            });

        statsService.run();
        Mockito.verify(sqlOperations, times(0)).createSession(anyString(), anySetOf(Option.class), anyByte());
    }
}
