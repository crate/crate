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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.base.Function;
import io.crate.action.sql.query.CrateSearchContext;
import io.crate.breaker.RamAccountingContext;
import io.crate.jobs.ContextCallback;
import io.crate.planner.node.dql.CollectNode;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingProjector;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * This class requires PowerMock in order to mock the final {@link SearchContext#close} method.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CrateSearchContext.class)
public class JobCollectContextTest extends CrateUnitTest {

    /*
    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

    static final Function<JobQueryShardContext, LuceneDocCollector> CONTEXT_FUNCTION =
            new Function<JobQueryShardContext, LuceneDocCollector>() {
                @Nullable
                @Override
                public LuceneDocCollector apply(JobQueryShardContext shardContext) {
                    CrateSearchContext searchContext = mock(CrateSearchContext.class);
                    shardContext.searchContext(searchContext);
                    when(searchContext.engineSearcher()).thenReturn(shardContext.engineSearcher());
                    doNothing().when(searchContext).close();
                    LuceneDocCollector docCollector = mock(LuceneDocCollector.class);
                    when(docCollector.searchContext()).thenReturn(searchContext);
                    when(docCollector.producedRows()).thenReturn(true);
                    return docCollector;
                }
            };


    private JobCollectContext jobCollectContext;
    private IndexShard indexShard;
    private ShardId shardId;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        jobCollectContext = spy(new JobCollectContext(
                UUID.randomUUID(),
                mock(CollectNode.class),
                mock(CollectOperation.class), RAM_ACCOUNTING_CONTEXT, new CollectingProjector()));
        indexShard = mock(IndexShard.class);
        shardId = new ShardId("dummy", 1);
        when(indexShard.shardId()).thenReturn(shardId);
        doReturn(mock(Engine.Searcher.class)).when(jobCollectContext).acquireNewSearcher(indexShard);
    }

    @After
    public void cleanUp() throws Exception {
        jobCollectContext.close();
    }

    @Test
    public void testCreateAndCloseQuerySubContext() throws Exception {
        final Field queryContexts = JobCollectContext.class.getDeclaredField("queryContexts");
        queryContexts.setAccessible(true);

        int jobSearchContextId = 1;

        JobQueryShardContext shardContext = new JobQueryShardContext(
                indexShard,
                jobSearchContextId,
                false,
                CONTEXT_FUNCTION);
        jobCollectContext.addContext(jobSearchContextId, shardContext);
        assertThat(shardContext.collector(), instanceOf(LuceneDocCollector.class));
        assertThat(shardContext.searchContext(), instanceOf(CrateSearchContext.class));

        assertThat(((IntObjectOpenHashMap) queryContexts.get(jobCollectContext)).size(), is(1));

        shardContext.close();
        assertThat(((IntObjectOpenHashMap) queryContexts.get(jobCollectContext)).size(), is(0));
    }

    @Test
    public void testGetFetchContext() throws Exception {
        CollectNode collectNode = mock(CollectNode.class);
        when(collectNode.keepContextForFetcher()).thenReturn(true);
        JobCollectContext jobCollectContext = spy(new JobCollectContext(
                UUID.randomUUID(),
                collectNode,
                mock(CollectOperation.class), RAM_ACCOUNTING_CONTEXT, new CollectingProjector()));
        doReturn(mock(Engine.Searcher.class)).when(jobCollectContext).acquireNewSearcher(indexShard);
        int jobSearchContextId = 1;

        Field producedRows = LuceneDocCollector.class.getDeclaredField("producedRows");
        producedRows.setAccessible(true);

        try {
            // no context created, expect null
            assertNull(jobCollectContext.getFetchContext(jobSearchContextId));

            JobQueryShardContext queryContext = new JobQueryShardContext(
                    indexShard,
                    jobSearchContextId,
                    true,
                    CONTEXT_FUNCTION);
            jobCollectContext.addContext(jobSearchContextId, queryContext);

            // even after query context was closed without a failure and with produced rows,
            // fetch context (and so the search context) must survive
            queryContext.close();

            JobFetchShardContext fetchContext = jobCollectContext.getFetchContext(jobSearchContextId);
            assertNotNull(fetchContext);
            assertEquals(queryContext.searchContext(), fetchContext.searchContext());
        } finally {
            jobCollectContext.close();
        }
    }

    @Test
    public void testClose() throws Exception {
        Field closed = JobCollectContext.class.getDeclaredField("closed");
        closed.setAccessible(true);
        Field queryContexts = JobCollectContext.class.getDeclaredField("queryContexts");
        queryContexts.setAccessible(true);

        assertThat(((AtomicBoolean)closed.get(jobCollectContext)).get(), is(false));

        int jobSearchContextId = 1;

        JobQueryShardContext queryContext = new JobQueryShardContext(
                indexShard,
                jobSearchContextId,
                false,
                CONTEXT_FUNCTION);
        jobCollectContext.addContext(jobSearchContextId, queryContext);

        jobCollectContext.close();
        assertThat(((AtomicBoolean) closed.get(jobCollectContext)).get(), is(true));
        assertThat(((IntObjectOpenHashMap) queryContexts.get(jobCollectContext)).size(), is(0));
    }

    @Test
    public void testKill() throws Exception {
        final Field closed = JobCollectContext.class.getDeclaredField("closed");
        closed.setAccessible(true);
        Field queryContexts = JobCollectContext.class.getDeclaredField("queryContexts");
        queryContexts.setAccessible(true);

        for (int i = 0; i < 5; i++) {
            JobQueryShardContext queryContext = new JobQueryShardContext(
                    indexShard,
                    i,
                    false,
                    CONTEXT_FUNCTION);
            jobCollectContext.addContext(i, queryContext);
        }
        ContextCallback closeCallback = mock(ContextCallback.class);
        jobCollectContext.addCallback(closeCallback);
        jobCollectContext.kill();

        verify(closeCallback, times(1)).onClose(Mockito.any(Throwable.class), anyLong());
        assertThat(jobCollectContext.isKilled(), is(true));

        assertThat(((AtomicBoolean) closed.get(jobCollectContext)).get(), is(true));
        assertThat(((IntObjectOpenHashMap) queryContexts.get(jobCollectContext)).size(), is(0));
    }

*/
}
