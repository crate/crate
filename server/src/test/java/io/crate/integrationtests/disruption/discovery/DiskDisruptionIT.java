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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import io.crate.common.unit.TimeValue;
import io.crate.testing.UseRandomizedSchema;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
@IntegTestCase.Slow
public class DiskDisruptionIT extends AbstractDisruptionTestCase {

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
    @UseRandomizedSchema(random = false)
    @Test
    public void testGlobalCheckpointIsSafe() throws Exception {
        startCluster(rarely() ? 5 : 3);

        var numberOfShards = 1 + randomInt(2);
        var numberOfReplicas = randomInt(2);

        execute("create table doc.test (id int primary key, data string) "
            + "clustered into " + numberOfShards + " shards "
            + "with ("
            + " number_of_replicas = ?, "
            + " \"global_checkpoint_sync.interval\" = '1s' "
            + ")",
            new Object[] {numberOfReplicas}
        );
        ensureGreen();

        AtomicBoolean stopGlobalCheckpointFetcher = new AtomicBoolean();

        Map<Integer, Long> shardToGcp = new ConcurrentHashMap<>();
        for (int i = 0; i < numberOfShards; i++) {
            shardToGcp.put(i, SequenceNumbers.NO_OPS_PERFORMED);
        }
        final Thread globalCheckpointSampler = new Thread(() -> {
            while (stopGlobalCheckpointFetcher.get() == false) {
                try {
                    var response = execute("select id, seq_no_stats['global_checkpoint'] " +
                                                       "from sys.shards where table_name='test'");
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

        int numDocsToIndex = RandomizedTest.randomIntBetween(2, 50);
        try (BackgroundIndexer indexer = new BackgroundIndexer(
                "doc",
                "test",
                "data",
                sqlExecutor.jdbcUrl(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random())) {
            indexer.setRequestTimeout(TimeValue.ZERO);
            indexer.setIgnoreIndexingFailures(true);
            indexer.setFailureAssertion(e -> {});
            indexer.start(-1);

            waitForDocs(randomIntBetween(1, numDocsToIndex), indexer, sqlExecutor);

            logger.info("injecting failures");
            injectTranslogFailures();
            logger.info("stopping indexing");
        }

        logger.info("full cluster restart");
        internalCluster().fullRestart(new TestCluster.RestartCallback() {

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

        execute("select distinct id, seq_no_stats['max_seq_no'] from sys.shards where table_name='test' and " +
                "routing_state in ('STARTED', 'RELOCATING')");

        assertThat(response.rowCount(), is((long) numberOfShards));

        for (var row : response.rows()) {
            final int shardId = (int) row[0];
            final long maxSeqNo = (long) row[1];
            assertThat(maxSeqNo, greaterThanOrEqualTo(shardToGcp.get(shardId)));
        }
    }
}
