/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.elasticsearch.index.shard.IndexShardTestCase.flushShard;
import static org.elasticsearch.index.shard.IndexShardTestCase.getEngine;
import static org.elasticsearch.test.InternalSettingsPlugin.TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.testing.Asserts;

@IntegTestCase.ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false)
public class IndexServiceTests extends IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    public void testBaseAsyncTask() throws Exception {
        execute("create table test (x int) clustered into 1 shards");
        IndexService indexService = getIndexService("test");

        AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
        AtomicReference<CountDownLatch> latch2 = new AtomicReference<>(new CountDownLatch(1));
        final AtomicInteger count = new AtomicInteger();
        IndexService.BaseAsyncTask task = new IndexService.BaseAsyncTask(indexService, TimeValue.timeValueMillis(1)) {
            @Override
            protected void runInternal() {
                final CountDownLatch l1 = latch.get();
                final CountDownLatch l2 = latch2.get();
                count.incrementAndGet();
                assertTrue("generic threadpool is configured", Thread.currentThread().getName().contains("[generic]"));
                l1.countDown();
                try {
                    l2.await();
                } catch (InterruptedException e) {
                    fail("interrupted");
                }
                if (randomBoolean()) { // task can throw exceptions!!
                    if (randomBoolean()) {
                        throw new RuntimeException("foo");
                    } else {
                        throw new RuntimeException("bar");
                    }
                }
            }

            @Override
            protected String getThreadPool() {
                return ThreadPool.Names.GENERIC;
            }
        };

        latch.get().await();
        latch.set(new CountDownLatch(1));
        assertEquals(1, count.get());
        // here we need to swap first before we let it go otherwise threads might be very fast and run that task twice due to
        // random exception and the schedule interval is 1ms
        latch2.getAndSet(new CountDownLatch(1)).countDown();
        latch.get().await();
        assertEquals(2, count.get());
        task.close();
        latch2.get().countDown();
        assertEquals(2, count.get());

        task = new IndexService.BaseAsyncTask(indexService, TimeValue.timeValueMillis(1000000)) {
            @Override
            protected void runInternal() {

            }
        };
        assertTrue(task.mustReschedule());

        // now close the index
        execute("alter table test close");
        final Index index = indexService.index();
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getIndicesService().hasIndex(index)));
        final IndexService closedIndexService = getIndicesService().indexServiceSafe(index);
        assertNotSame(indexService, closedIndexService);
        assertFalse(task.mustReschedule());
        assertFalse(task.isClosed());
        assertEquals(1000000, task.getInterval().millis());

        assertNotSame(indexService, closedIndexService);
        assertFalse(task.mustReschedule());
        assertFalse(task.isClosed());
        assertEquals(1000000, task.getInterval().millis());

        // now reopen the index
        execute("alter table test open");
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getIndicesService().hasIndex(index)));
        indexService = getIndicesService().indexServiceSafe(index);
        assertNotSame(closedIndexService, indexService);

        task = new IndexService.BaseAsyncTask(indexService, TimeValue.timeValueMillis(100000)) {
            @Override
            protected void runInternal() {

            }
        };
        assertTrue(task.mustReschedule());
        assertFalse(task.isClosed());
        assertTrue(task.isScheduled());

        indexService.close("simon says", false);
        assertFalse("no shards left", task.mustReschedule());
        assertTrue(task.isScheduled());
        task.close();
        assertFalse(task.isScheduled());
    }

    @Test
    public void testRefreshTaskIsUpdated() throws Exception {
        execute("create table test (x int) clustered into 1 shards");
        IndexService indexService = getIndexService("test");
        IndexService.AsyncRefreshTask refreshTask = indexService.getRefreshTask();
        assertEquals(1000, refreshTask.getInterval().millis());
        assertTrue(indexService.getRefreshTask().mustReschedule());

        // now disable
        execute("alter table test set (refresh_interval = -1)");
        assertNotSame(refreshTask, indexService.getRefreshTask());
        assertTrue(refreshTask.isClosed());
        assertFalse(refreshTask.isScheduled());

        execute("alter table test set (refresh_interval = '100ms')");
        assertNotSame(refreshTask, indexService.getRefreshTask());
        assertTrue(refreshTask.isClosed());

        refreshTask = indexService.getRefreshTask();
        assertTrue(refreshTask.mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertEquals(100, refreshTask.getInterval().millis());

        execute("alter table test set (refresh_interval = '200ms')");
        assertNotSame(refreshTask, indexService.getRefreshTask());
        assertTrue(refreshTask.isClosed());

        refreshTask = indexService.getRefreshTask();
        assertTrue(refreshTask.mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertEquals(200, refreshTask.getInterval().millis());

        // set it to 200ms again
        execute("alter table test set (refresh_interval = '200ms')");
        assertSame(refreshTask, indexService.getRefreshTask());
        assertTrue(indexService.getRefreshTask().mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertFalse(refreshTask.isClosed());
        assertEquals(200, refreshTask.getInterval().millis());

        // now close the index
        execute("alter table test close");
        final Index index = indexService.index();
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getIndicesService().hasIndex(index)));

        final IndexService closedIndexService = getIndicesService().indexServiceSafe(index);
        assertNotSame(indexService, closedIndexService);
        assertNotSame(refreshTask, closedIndexService.getRefreshTask());
        assertFalse(closedIndexService.getRefreshTask().mustReschedule());
        assertFalse(closedIndexService.getRefreshTask().isClosed());
        assertEquals(200, closedIndexService.getRefreshTask().getInterval().millis());

        // now reopen the index
        execute("alter table test open");
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getIndicesService().hasIndex(index)));
        indexService = getIndicesService().indexServiceSafe(index);
        assertNotSame(closedIndexService, indexService);
        refreshTask = indexService.getRefreshTask();
        assertTrue(indexService.getRefreshTask().mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertFalse(refreshTask.isClosed());

        indexService.close("simon says", false);
        assertFalse(refreshTask.isScheduled());
        assertTrue(refreshTask.isClosed());
    }

    public void testFsyncTaskIsRunning() throws Exception {
        execute("create table test(x int) clustered into 1 shards with (\"translog.durability\" = 'ASYNC')");
        IndexService indexService = getIndexService("test");
        IndexService.AsyncTranslogFSync fsyncTask = indexService.getFsyncTask();
        assertNotNull(fsyncTask);
        assertEquals(5000, fsyncTask.getInterval().millis());
        assertTrue(fsyncTask.mustReschedule());
        assertTrue(fsyncTask.isScheduled());

        // now close the index
        execute("alter table test close");
        final Index index = indexService.index();
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getIndicesService().hasIndex(index)));

        final IndexService closedIndexService = getIndicesService().indexServiceSafe(index);
        assertNotSame(indexService, closedIndexService);
        assertNotSame(fsyncTask, closedIndexService.getFsyncTask());
        assertFalse(closedIndexService.getFsyncTask().mustReschedule());
        assertFalse(closedIndexService.getFsyncTask().isClosed());
        assertEquals(5000, closedIndexService.getFsyncTask().getInterval().millis());

        // now reopen the index
        execute("alter table test open");
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getIndicesService().hasIndex(index)));
        indexService = getIndicesService().indexServiceSafe(index);
        assertNotSame(closedIndexService, indexService);
        fsyncTask = indexService.getFsyncTask();
        assertTrue(indexService.getRefreshTask().mustReschedule());
        assertTrue(fsyncTask.isScheduled());
        assertFalse(fsyncTask.isClosed());

        indexService.close("simon says", false);
        assertFalse(fsyncTask.isScheduled());
        assertTrue(fsyncTask.isClosed());

        execute("create table test1 (x int, data text)");
        indexService = getIndexService("test1");
        assertNull(indexService.getFsyncTask());
    }

    @Test
    public void testRefreshActuallyWorks() throws Exception {
        execute("create table test (x int, data text) clustered into 1 shards");
        var indexService = getIndexService("test");
        var indexName = indexService.index().getName();
        ensureGreen(indexName);
        IndexService.AsyncRefreshTask refreshTask = indexService.getRefreshTask();
        assertEquals(1000, refreshTask.getInterval().millis());
        assertTrue(indexService.getRefreshTask().mustReschedule());
        IndexShard shard = indexService.getShard(0);
        execute("insert into test (x, data) values (1, 'foo')");
        // now disable the refresh
        execute("alter table test set (refresh_interval = -1)");
        // when we update we reschedule the existing task AND fire off an async refresh to make sure we make everything visible
        // before that this is why we need to wait for the refresh task to be unscheduled and the first doc to be visible
        assertTrue(refreshTask.isClosed());
        refreshTask = indexService.getRefreshTask();
        assertBusy(() -> {
            // this one either becomes visible due to a concurrently running scheduled refresh OR due to the force refresh
            // we are running on updateMetadata if the interval changes
            try (Engine.Searcher searcher = shard.acquireSearcher(indexName)) {
                TopDocs search = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(1, search.totalHits.value);
            }
        });
        assertFalse(refreshTask.isClosed());
        // refresh every millisecond
        execute("insert into test (x, data) values (2, 'foo')");
        execute("alter table test set (refresh_interval = '1ms')");
        assertTrue(refreshTask.isClosed());

        assertBusy(() -> {
            // this one becomes visible due to the force refresh we are running on updateMetadata if the interval changes
            try (Engine.Searcher searcher = shard.acquireSearcher(indexName)) {
                TopDocs search = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(2, search.totalHits.value);
            }
        });
        execute("insert into test (x, data) values (3, 'foo')");

        assertBusy(() -> {
            // this one becomes visible due to the scheduled refresh
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                TopDocs search = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(3, search.totalHits.value);
            }
        });
    }

    public void testAsyncFsyncActuallyWorks() throws Exception {
        execute("create table test(x int, data string) clustered into 1 shards with (\"translog.sync_interval\" = '100ms', " +
                "\"translog.durability\" = 'ASYNC')");
        IndexService indexService = getIndexService("test");
        var indexName = indexService.index().getName();
        ensureGreen(indexName);
        assertTrue(indexService.getRefreshTask().mustReschedule());
        execute("insert into test (x, data) values (1, 'foo')");
        IndexShard shard = indexService.getShard(0);
        assertBusy(() -> assertFalse(shard.isSyncNeeded()));
    }

    public void testRescheduleAsyncFsync() throws Exception {
        execute("create table test(x int, data string) clustered into 1 shards with (\"translog.sync_interval\" = '100ms', \"translog.durability\" = 'REQUEST')");
        IndexService indexService = getIndexService("test");
        var indexName = indexService.index().getName();

        ensureGreen(indexName);
        assertNull(indexService.getFsyncTask());

        execute("alter table test set (\"translog.durability\" = 'ASYNC')");

        assertNotNull(indexService.getFsyncTask());
        assertTrue(indexService.getFsyncTask().mustReschedule());
        execute("insert into test (x, data) values (1, 'foo')");
        assertNotNull(indexService.getFsyncTask());
        final IndexShard shard = indexService.getShard(0);
        assertBusy(() -> assertFalse(shard.isSyncNeeded()));

        execute("alter table test set (\"translog.durability\" = 'REQUEST')");
        assertNull(indexService.getFsyncTask());

        execute("alter table test set (\"translog.durability\" = 'ASYNC')");
        assertNotNull(indexService.getFsyncTask());
    }

    public void testAsyncTranslogTrimActuallyWorks() throws Exception {
        execute("create table test(x int, data string) clustered into 1 shards with (\"translog.sync_interval\" = '100ms')");
        IndexService indexService = getIndexService("test");

        ensureGreen(indexService.index().getName());
        assertTrue(indexService.getTrimTranslogTask().mustReschedule());
        execute("insert into test (x, data) values (1, 'foo')");
        IndexShard shard = indexService.getShard(0);
        flushShard(shard, true);
        assertBusy(() -> assertThat(EngineTestCase.getTranslog(getEngine(shard)).totalOperations(), equalTo(0)));
    }

    @Test
    public void testAsyncTranslogTrimTaskOnClosedIndex() throws Exception {
        execute ("create table test(x int) clustered into 1 shards");
        var indexService = getIndexService("test");
        var indexName = indexService.index().getName();

        // Setting not exposed via SQL
        client()
            .admin()
            .indices()
            .updateSettings(new UpdateSettingsRequest(
                Settings.builder()
                    .put(TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                    .build(),
                indexName
            )).get();

        Translog translog = IndexShardTestCase.getTranslog(indexService.getShard(0));

        int translogOps = 0;
        final int numDocs = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            execute("insert into test (x) values (?)", new Object[]{i});
            translogOps++;
            if(randomBoolean()) {
                for (IndexShard indexShard : indexService) {
                    flushShard(indexShard, true);
                }
                if (indexService.getIndexSettings().isSoftDeleteEnabled()) {
                    translogOps = 0;
                }
            }
        }
        assertThat(translog.totalOperations(), equalTo(translogOps));
        assertThat(translog.stats().estimatedNumberOfOperations(), equalTo(translogOps));

        execute("alter table test close");

        indexService =  getIndicesService().indexServiceSafe(indexService.index());
        assertTrue(indexService.getTrimTranslogTask().mustReschedule());

        final Engine readOnlyEngine = getEngine(indexService.getShard(0));
        assertBusy(() ->
            assertThat(readOnlyEngine.getTranslogStats().getTranslogSizeInBytes(), equalTo((long) Translog.DEFAULT_HEADER_SIZE_IN_BYTES)));

        execute("alter table test open");
        ensureGreen(indexName);

        indexService = getIndexService("test");
        translog = IndexShardTestCase.getTranslog(indexService.getShard(0));
        assertThat(translog.totalOperations(), equalTo(0));
        assertThat(translog.stats().estimatedNumberOfOperations(), equalTo(0));
    }

    public void testIllegalFsyncInterval() {
        Asserts.assertSQLError(() -> execute("create table test(x int, data string) clustered into 1 shards with (\"translog.sync_interval\" = '0ms')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("failed to parse value [0ms] for setting [index.translog.sync_interval], must be >= [100ms]");
    }

    @Test
    public void testUpdateSyncIntervalDynamically() throws Exception {
        execute("create table test (x int) clustered into 1 shards with (\"translog.sync_interval\" = '10s')");
        IndexService indexService = getIndexService("test");
        var indexName = indexService.index().getName();

        ensureGreen(indexName);
        assertNull(indexService.getFsyncTask());

        execute("alter table test set (\"translog.sync_interval\" = '5s', \"translog.durability\" = 'async')");

        assertNotNull(indexService.getFsyncTask());
        assertTrue(indexService.getFsyncTask().mustReschedule());

        IndexMetadata indexMetadata = client().admin().cluster()
            .state(new ClusterStateRequest())
            .get().getState().metadata().index(indexName);
        assertEquals("5s", indexMetadata.getSettings().get(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey()));

        execute("alter table test close");
        execute("alter table test set (\"translog.sync_interval\" = '20s')");
        indexMetadata = client().admin().cluster()
            .state(new ClusterStateRequest())
            .get().getState().metadata().index(indexName);
        assertEquals("20s", indexMetadata.getSettings().get(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey()));
    }

    private IndexService getIndexService(String index) {
        return getIndicesService().indexServiceSafe(resolveIndex(getFqn(index)));
    }

    private IndicesService getIndicesService() {
        return cluster().getInstances(IndicesService.class).iterator().next();
    }
}
