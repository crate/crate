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

import com.google.common.base.Function;
import io.crate.action.sql.query.CrateSearchContext;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.*;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * This class requires PowerMock in order to mock the final {@link SearchContext#close} method.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CrateSearchContext.class)
public class JobCollectContextTest extends CrateUnitTest {

    static final Function<Engine.Searcher, LuceneDocCollector> CONTEXT_FUNCTION =
            new Function<Engine.Searcher, LuceneDocCollector>() {
                @Nullable
                @Override
                public LuceneDocCollector apply(Engine.Searcher input) {
                    CrateSearchContext searchContext = mock(CrateSearchContext.class);
                    when(searchContext.engineSearcher()).thenReturn(input);
                    when(searchContext.isEngineSearcherShared()).thenCallRealMethod();
                    doCallRealMethod().when(searchContext).sharedEngineSearcher(Mockito.anyBoolean());
                    doNothing().when(searchContext).close();
                    LuceneDocCollector docCollector = mock(LuceneDocCollector.class);
                    when(docCollector.searchContext()).thenReturn(searchContext);
                    return docCollector;
                }
            };


    private JobCollectContext jobCollectContext;
    private IndexShard indexShard;
    private ShardId shardId;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        jobCollectContext = spy(new JobCollectContext(UUID.randomUUID()));
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
    public void testRegisterJobContextId() throws Exception {
        final Field jobContextIdMap = JobCollectContext.class.getDeclaredField("jobContextIdMap");
        jobContextIdMap.setAccessible(true);
        final Field shardsMap = JobCollectContext.class.getDeclaredField("shardsMap");
        shardsMap.setAccessible(true);

        final ExecutorService executorService = Executors.newFixedThreadPool(10);
        final CountDownLatch latch = new CountDownLatch(10);
        List<Callable<Void>> tasks = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            final int jobSearchContextId = i;
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    jobCollectContext.registerJobContextId(shardId, jobSearchContextId);
                    latch.countDown();
                    assertThat((ShardId) ((Map) jobContextIdMap.get(jobCollectContext)).get(jobSearchContextId), is(shardId));
                    return null;
                }
            });
        }
        executorService.invokeAll(tasks);
        latch.await();

        assertThat(((Map) jobContextIdMap.get(jobCollectContext)).size(), is(10));
        assertThat(((Map)shardsMap.get(jobCollectContext)).size(), is(1));
        assertThat((List<Integer>) ((Map) shardsMap.get(jobCollectContext)).get(shardId), containsInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    @Test
    public void testCreateAndCloseCollectorWithContext() throws Exception {
        final Field activeCollectors = JobCollectContext.class.getDeclaredField("activeCollectors");
        activeCollectors.setAccessible(true);

        int jobSearchContextId = 1;
        jobCollectContext.registerJobContextId(shardId, jobSearchContextId);

        LuceneDocCollector collector1 = jobCollectContext.createCollectorAndContext(indexShard, jobSearchContextId, CONTEXT_FUNCTION);
        assertThat(collector1, instanceOf(LuceneDocCollector.class));
        assertThat(collector1.searchContext(), instanceOf(CrateSearchContext.class));

        // calling again with same arguments results in same context
        LuceneDocCollector collector2 = jobCollectContext.createCollectorAndContext(indexShard, jobSearchContextId, CONTEXT_FUNCTION);
        assertEquals(collector1, collector2);
        assertEquals(collector1.searchContext(), collector2.searchContext());
        assertThat(((Map)activeCollectors.get(jobCollectContext)).size(), is(1));

        jobCollectContext.closeContext(jobSearchContextId);
        assertThat(((Map) activeCollectors.get(jobCollectContext)).size(), is(0));
    }

    @Test
    public void testFindCollector() throws Exception {
        int jobSearchContextId = 1;
        jobCollectContext.registerJobContextId(shardId, jobSearchContextId);

        // no context created, expect null
        assertNull(jobCollectContext.findCollector(1));

        LuceneDocCollector collector1 = jobCollectContext.createCollectorAndContext(indexShard, jobSearchContextId, CONTEXT_FUNCTION);
        LuceneDocCollector collector2 = jobCollectContext.findCollector(jobSearchContextId);
        assertEquals(collector1, collector2);
        assertEquals(collector1.searchContext(), collector2.searchContext());
    }

    @Test
    public void testSharedEngineSearcher() throws Exception {
        final Field engineSearchersRefCount = JobCollectContext.class.getDeclaredField("engineSearchersRefCount");
        engineSearchersRefCount.setAccessible(true);

        jobCollectContext.registerJobContextId(shardId, 1);
        jobCollectContext.registerJobContextId(shardId, 2);

        CrateSearchContext ctx1 = jobCollectContext.createCollectorAndContext(indexShard, 1, CONTEXT_FUNCTION).searchContext();
        CrateSearchContext ctx2 = jobCollectContext.createCollectorAndContext(indexShard, 2, CONTEXT_FUNCTION).searchContext();

        assertEquals(ctx1.engineSearcher(), ctx2.engineSearcher());
        assertThat(ctx1.isEngineSearcherShared(), is(true));
        assertThat(ctx2.isEngineSearcherShared(), is(true));
        assertThat(((Map<ShardId, Integer>)engineSearchersRefCount.get(jobCollectContext)).get(shardId), is(2));

        jobCollectContext.closeContext(1);
        assertThat(((Map<ShardId, Integer>) engineSearchersRefCount.get(jobCollectContext)).get(shardId), is(1));
        jobCollectContext.closeContext(2);
        assertThat(((Map<ShardId, Integer>) engineSearchersRefCount.get(jobCollectContext)).get(shardId), is(0));
    }

    @Test
    public void testSharedEngineSearcherConcurrent() throws Exception {
        final Field engineSearchersRefCount = JobCollectContext.class.getDeclaredField("engineSearchersRefCount");
        engineSearchersRefCount.setAccessible(true);

        // open contexts concurrent (all sharing same engine searcher)
        final ExecutorService executorService = Executors.newFixedThreadPool(10);
        final CountDownLatch latch = new CountDownLatch(10);
        List<Callable<Void>> tasks = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            final int jobSearchContextId = i;
            jobCollectContext.registerJobContextId(shardId, jobSearchContextId);
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    jobCollectContext.createCollectorAndContext(indexShard, jobSearchContextId, CONTEXT_FUNCTION);
                    latch.countDown();
                    return null;
                }
            });
        }
        executorService.invokeAll(tasks);
        latch.await();
        assertThat(((Map<ShardId, Integer>) engineSearchersRefCount.get(jobCollectContext)).get(shardId), is(10));

        // close contexts concurrent (
        final CountDownLatch latch2 = new CountDownLatch(10);
        List<Callable<Void>> tasks2 = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            final int jobSearchContextId = i;
            tasks2.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    jobCollectContext.closeContext(jobSearchContextId);
                    latch2.countDown();
                    return null;
                }
            });
        }
        executorService.invokeAll(tasks2);
        latch2.await();
        assertThat(((Map<ShardId, Integer>) engineSearchersRefCount.get(jobCollectContext)).get(shardId), is(0));
    }

    @Test
    public void testClose() throws Exception {
        final Field closed = JobCollectContext.class.getDeclaredField("closed");
        closed.setAccessible(true);
        final Field activeCollectors = JobCollectContext.class.getDeclaredField("activeCollectors");
        activeCollectors.setAccessible(true);

        assertThat(((AtomicBoolean)closed.get(jobCollectContext)).get(), is(false));

        int jobSearchContextId = 1;
        jobCollectContext.registerJobContextId(shardId, jobSearchContextId);

        CrateSearchContext ctx1 = jobCollectContext.createCollectorAndContext(indexShard, jobSearchContextId, CONTEXT_FUNCTION).searchContext();
        assertThat(ctx1, instanceOf(CrateSearchContext.class));

        jobCollectContext.close();
        assertThat(((AtomicBoolean) closed.get(jobCollectContext)).get(), is(true));
        assertThat(((Map) activeCollectors.get(jobCollectContext)).size(), is(0));
    }

    @Test
    public void testAcquireAndReleaseContext() throws Exception {
        int jobSearchContextId = 1;
        jobCollectContext.registerJobContextId(shardId, jobSearchContextId);

        SearchContext ctx1 = jobCollectContext.createCollectorAndContext(indexShard, jobSearchContextId, CONTEXT_FUNCTION).searchContext();
        assertThat(ctx1, instanceOf(CrateSearchContext.class));

        jobCollectContext.acquireContext(ctx1);
        assertThat(SearchContext.current(), is(ctx1));

        jobCollectContext.releaseContext(ctx1);
        assertThat(SearchContext.current(), nullValue());
    }
}
