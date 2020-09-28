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
package io.crate.integrationtests.disruption.discovery;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import io.crate.common.unit.TimeValue;
import io.crate.integrationtests.Setup;
import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiskDisruptionIT extends AbstractDisruptionTestCase {

    private Setup setup = new Setup(sqlExecutor);
    private static DisruptTranslogFileSystemProvider disruptTranslogFileSystemProvider;

    @BeforeClass
    public static void installDisruptTranslogFS() {
        FileSystem current = FileSystems.getDefault();
        disruptTranslogFileSystemProvider = new DisruptTranslogFileSystemProvider(current);
        PathUtilsForTesting.installMock(disruptTranslogFileSystemProvider.getFileSystem(null));
    }

    @AfterClass
    public static void removeDisruptTranslogFS() {
        PathUtilsForTesting.teardown();
    }

    void injectTranslogFailures() {
        disruptTranslogFileSystemProvider.injectFailures.set(true);
    }

    @After
    void stopTranslogFailures() {
        disruptTranslogFileSystemProvider.injectFailures.set(false);
    }

    static class DisruptTranslogFileSystemProvider extends FilterFileSystemProvider {

        AtomicBoolean injectFailures = new AtomicBoolean();

        DisruptTranslogFileSystemProvider(FileSystem inner) {
            super("disrupttranslog://", inner);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            if (injectFailures.get() && path.toString().endsWith(".ckp")) {
                // prevents checkpoint file to be updated
                throw new IOException("fake IOException");
            }
            return super.newFileChannel(path, options, attrs);
        }
    }

    /**
     * This test checks that all operations below the global checkpoint are properly persisted.
     * It simulates a full power outage by preventing translog checkpoint files to be written and restart the cluster. This means that
     * all un-fsynced data will be lost.
     */
    @TestLogging("org.elasticsearch:INFO, org.elasticsearch.test:DEBUG")
    public void testGlobalCheckpointIsSafe() throws Exception {
        final List<String> nodes = startCluster(3);

        var numberOfShards = 1 + randomInt(2);
        var numberOfReplicas =  randomInt(2);

        sqlExecutor.execute("create table test (id int, x string) clustered into " + numberOfShards +
                            " shards with (number_of_replicas = ?" +
                            ")",
                            new Object[] {numberOfReplicas, 1}).actionGet();

        ensureGreen();

        AtomicBoolean stopGlobalCheckpointFetcher = new AtomicBoolean();

        Map<Integer, Long> shardToGcp = new ConcurrentHashMap<>();
        for (int i = 0; i < numberOfShards; i++) {
            shardToGcp.put(i, SequenceNumbers.NO_OPS_PERFORMED);
        }
        final Thread globalCheckpointSampler = new Thread(() -> {
            while (stopGlobalCheckpointFetcher.get() == false) {
                try {
                    var response = sqlExecutor.execute("select id, seq_no_stats['global_checkpoint'] " +
                                                       "from sys.shards where table_name='test'", null).actionGet();
                    for (var row : response.rows()) {
                        final int shardId = (int) row[0];
                        final long globalCheckpoint = (long) row[1];
                        shardToGcp.compute(shardId, (i, v) -> Math.max(v, globalCheckpoint));
                    }
                } catch (Exception e) {
                    // ignore
                    logger.debug("failed to fetch shard stats", e);
                }
            }
        });

        globalCheckpointSampler.start();

        try (BackgroundIndexer indexer = new BackgroundIndexer("test", sqlExecutor, -1, RandomizedTest.scaledRandomIntBetween(2, 5),
                                                               false, random())) {
            indexer.setRequestTimeout(TimeValue.ZERO);
            indexer.setIgnoreIndexingFailures(true);
            indexer.setFailureAssertion(e -> {});
            indexer.start(-1);

            waitForDocs(randomIntBetween(1, 100), indexer, "test");

            logger.info("injecting failures");
            injectTranslogFailures();
            logger.info("stopping indexing");
        }

        logger.info("full cluster restart");
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {

            @Override
            public void onAllNodesStopped() {
                logger.info("stopping failures");
                stopTranslogFailures();
            }

        });

        stopGlobalCheckpointFetcher.set(true);

        logger.info("waiting for global checkpoint sampler");
        globalCheckpointSampler.join();

        logger.info("waiting for green");
        ensureGreen();

        var response = sqlExecutor.execute("select distinct id, seq_no_stats['max_seq_no'] from sys.shards where table_name='test' " +
                                      "and routing_state in ('STARTED', 'RELOCATING')", null).actionGet();

        assertThat(response.rowCount(), is((long) numberOfShards));

        for (var row : response.rows()) {
            final int shardId = (int) row[0];
            final long maxSeqNo = (long) row[1];
            assertThat(maxSeqNo, greaterThanOrEqualTo(shardToGcp.get(shardId)));
        }
    }

    /**
     * Waits until at least a give number of document is visible for searchers
     *
     * @param numDocs number of documents to wait for
     * @param indexer a {@link org.elasticsearch.test.BackgroundIndexer}. It will be first checked for documents indexed.
     *                This saves on unneeded searches.
     */
    public void waitForDocs(final long numDocs, final BackgroundIndexer indexer, String table) throws Exception {
        // indexing threads can wait for up to ~1m before retrying when they first try to index into a shard which is not STARTED.
        final long maxWaitTimeMs = Math.max(90 * 1000, 200 * numDocs);

        assertBusy(
            () -> {
                long lastKnownCount = indexer.totalIndexedDocs();

                if (lastKnownCount >= numDocs) {
                    try {
                        var response = sqlExecutor.execute("select count(*) from " + table, null).actionGet();
                        long count = (long) response.rows()[0][0];
                        if (count == lastKnownCount) {
                            // no progress - try to refresh for the next time
                            client().admin().indices().prepareRefresh().get();
                        }
                        lastKnownCount = count;
                    } catch (Exception e) { // count now acts like search and barfs if all shards failed...
                        logger.debug("failed to executed count", e);
                        throw e;
                    }
                }

                if (logger.isDebugEnabled()) {
                    if (lastKnownCount < numDocs) {
                        logger.debug("[{}] docs indexed. waiting for [{}]", lastKnownCount, numDocs);
                    } else {
                        logger.debug("[{}] docs visible for search (needed [{}])", lastKnownCount, numDocs);
                    }
                }

                assertThat(lastKnownCount, greaterThanOrEqualTo(numDocs));
            },
            maxWaitTimeMs,
            TimeUnit.MILLISECONDS
        );
    }
}
