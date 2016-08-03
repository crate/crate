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

import com.google.common.collect.ImmutableSet;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.action.sql.TransportSQLAction;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableStatsServiceTest extends CrateUnitTest  {

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

    private TransportSQLAction getTransportSQLAction(final AtomicInteger numRequests) {
        return new TransportSQLAction(
            mock(SQLOperations.class),
            Settings.EMPTY,
            threadPool,
            mock(TransportService.class, Answers.RETURNS_MOCKS.get()),
            new ActionFilters(ImmutableSet.<ActionFilter>of()),
            mock(IndexNameExpressionResolver.class)
        ) {
            @Override
            protected void doExecute(SQLRequest request, ActionListener<SQLResponse> listener) {
                Object[] row;
                if (numRequests.get() == 0) {
                    row = new Object[] { 2L, "foo", "bar"};
                } else {
                    row = new Object[] { 4L, "foo", "bar"};
                }
                listener.onResponse(new SQLResponse(
                    new String[] {"cast(sum(num_docs) as long)", "schema_name", "table_name"},
                    new Object[][] { row },
                    new DataType[] {DataTypes.LONG, DataTypes.STRING, DataTypes.STRING},
                    1L,
                    1,
                    false
                ));
                numRequests.incrementAndGet();
            }
        };
    }

    @Test
    public void testNumDocs() throws Exception {
        final AtomicInteger numRequests = new AtomicInteger(0);
        final TransportSQLAction transportSQLAction = getTransportSQLAction(numRequests);

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(mock(DiscoveryNode.class));

        final TableStatsService statsService = new TableStatsService(Settings.EMPTY,
                                                               threadPool,
                                                               clusterService,
                                                               TimeValue.timeValueMillis(100),
                                                               new Provider<TransportSQLAction>() {
            @Override
            public TransportSQLAction get() {
                return transportSQLAction;
            }
        });

        assertBusy(new Runnable() {
                       @Override
                       public void run() {
                           // first call triggers request
                           assertThat(statsService.numDocs(new TableIdent("foo", "bar")), is(2L));
                           // second call hits cache
                           assertThat(statsService.numDocs(new TableIdent("foo", "bar")), is(2L));
                       }
                   });

        int slept = 0;
        while (numRequests.get() < 2 && slept < 1000) {
            Thread.sleep(50);
            slept += 50;
        }
        // periodic update happened
        assertThat(numRequests.get(), Matchers.greaterThanOrEqualTo(2));
        assertThat(statsService.numDocs(new TableIdent("foo", "bar")), is(4L));

        assertThat(statsService.numDocs(new TableIdent("unknown", "table")), is(-1L));
    }

    @Test
    public void testNoUpdateIfLocalNodeNotAvailable() throws Exception {
        final AtomicInteger numRequests = new AtomicInteger(0);
        final TransportSQLAction transportSQLAction = getTransportSQLAction(numRequests);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(null);

        TableStatsService statsService = new TableStatsService(Settings.EMPTY,
            threadPool,
            clusterService,
            TimeValue.timeValueSeconds(60L),
            new Provider<TransportSQLAction>() {
                @Override
                public TransportSQLAction get() {
                    return transportSQLAction;
                }
            });

        statsService.run();
        assertThat(statsService.numDocs(new TableIdent("foo", "bar")), is(-1L));
        assertThat(numRequests.get(), Matchers.greaterThanOrEqualTo(0));
    }
}
