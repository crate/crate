/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import io.crate.blob.PutChunkAction;
import io.crate.blob.PutChunkRequest;
import io.crate.blob.StartBlobAction;
import io.crate.blob.StartBlobRequest;
import io.crate.blob.v2.BlobAdminClient;
import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;
import io.crate.common.Hex;
import io.crate.test.utils.Blobs;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0)
@ThreadLeakFilters(filters = {RecoveryTests.RecoveryTestThreadFilter.class})
public class RecoveryTests extends BlobIntegrationTestBase {

    public static class RecoveryTestThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return (t.getName().contains("blob-uploader"));
        }
    }

    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(25, TimeUnit.MINUTES);

    // the time to sleep between chunk requests in upload
    private AtomicInteger timeBetweenChunks = new AtomicInteger();

    static {
        System.setProperty("tests.short_timeouts", "true");
    }

    private String uploadFile(Client client, String content) {
        byte[] digest = Blobs.digest(content);
        String digestString = Hex.encodeHexString(digest);
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        logger.trace("Uploading {} digest {}", content, digestString);
        BytesArray bytes = new BytesArray(new byte[]{contentBytes[0]});
        if (content.length() == 1) {
            client.execute(StartBlobAction.INSTANCE,
                new StartBlobRequest(BlobIndex.fullIndexName("test"), digest, bytes, true))
                .actionGet();
        } else {
            StartBlobRequest startBlobRequest = new StartBlobRequest(
                BlobIndex.fullIndexName("test"), digest, bytes, false);
            client.execute(StartBlobAction.INSTANCE, startBlobRequest).actionGet();
            for (int i = 1; i < contentBytes.length; i++) {
                try {
                    Thread.sleep(timeBetweenChunks.get());
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
                bytes = new BytesArray(new byte[]{contentBytes[i]});
                try {
                    client.execute(PutChunkAction.INSTANCE,
                        new PutChunkRequest(
                            BlobIndex.fullIndexName("test"), digest,
                            startBlobRequest.transferId(), bytes, i,
                            (i + 1) == content.length())
                    ).actionGet();
                } catch (IllegalStateException ex) {
                    Thread.interrupted();
                }
            }
        }
        logger.trace("Upload finished {} digest {}", content, digestString);

        return digestString;
    }


    private String genFile(long numChars) {
        StringBuilder sb = new StringBuilder();
        int charValue = 64;
        for (long i = 0; i <= numChars + 10; i++) {
            charValue++;
            if (charValue > 90) {
                charValue = 64;
            }
            sb.append(Character.toChars(charValue));
        }

        return sb.toString();
    }

    @Test
    public void testPrimaryRelocationWhileIndexing() throws Exception {
        final int numberOfRelocations = 1;
        final int numberOfWriters = 2;

        final String node1 = internalCluster().startNode();

        BlobAdminClient blobAdminClient = internalCluster().getInstance(BlobAdminClient.class, node1);

        logger.trace("--> creating test index ...");
        Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            // SETTING_AUTO_EXPAND_REPLICAS is enabled by default
            // but for this test it needs to be disabled so we can have 0 replicas
            .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "false")
            .build();

        blobAdminClient.createBlobTable("test", indexSettings).get();

        logger.trace("--> starting [node2] ...");
        final String node2 = internalCluster().startNode();
        ensureGreen();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[numberOfWriters];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);

        logger.trace("--> starting {} blob upload threads", writers.length);
        final List<String> uploadedDigests = Collections.synchronizedList(new ArrayList<String>(writers.length));
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            writers[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        logger.trace("**** starting blob upload thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            String digest = uploadFile(internalCluster().client(node1), genFile(id));
                            uploadedDigests.add(digest);
                            indexCounter.incrementAndGet();
                        }
                        logger.trace("**** done indexing thread {}", indexerId);
                    } catch (Exception e) {
                        logger.warn("**** failed indexing thread {}", e, indexerId);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].setName("blob-uploader-thread");
            // dispatch threads from parent, ignoring possible leaking threads
            writers[i].setDaemon(true);
            writers[i].start();
        }

        logger.trace("--> waiting for 2 blobs to be uploaded ...");
        while (uploadedDigests.size() < 2) {
            Thread.sleep(10);
        }
        logger.trace("--> 2 blobs uploaded");

        // increase time between chunks in order to make sure that the upload is taking place while relocating
        timeBetweenChunks.set(10);

        logger.trace("--> starting relocations...");
        for (int i = 0; i < numberOfRelocations; i++) {
            String fromNode = (i % 2 == 0) ? node1 : node2;
            String toNode = node1.equals(fromNode) ? node2 : node1;
            logger.trace("--> START relocate the shard from {} to {}", fromNode, toNode);
            internalCluster().client(node1).admin().cluster().prepareReroute()
                .add(new MoveAllocationCommand(BlobIndex.fullIndexName("test"), 0, fromNode, toNode))
                .execute().actionGet();
            ClusterHealthResponse clusterHealthResponse = internalCluster().client(node1).admin().cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNoRelocatingShards(true)
                .setTimeout(ACCEPTABLE_RELOCATION_TIME).execute().actionGet();

            assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
            clusterHealthResponse = internalCluster().client(node2).admin().cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNoRelocatingShards(true)
                .setTimeout(ACCEPTABLE_RELOCATION_TIME).execute().actionGet();
            assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
            logger.trace("--> DONE relocate the shard from {} to {}", fromNode, toNode);
        }
        logger.trace("--> done relocations");

        logger.trace("--> marking and waiting for upload threads to stop ...");
        timeBetweenChunks.set(0);
        stop.set(true);
        assertThat(stopLatch.await(60, TimeUnit.SECONDS), is(true));
        logger.trace("--> uploading threads stopped");

        logger.trace("--> expected {} got {}", indexCounter.get(), uploadedDigests.size());
        assertEquals(indexCounter.get(), uploadedDigests.size());

        BlobIndicesService blobIndicesService = internalCluster().getInstance(BlobIndicesService.class, node2);
        for (String digest : uploadedDigests) {
            BlobShard blobShard = blobIndicesService.localBlobShard(BlobIndex.fullIndexName("test"), digest);
            long length = blobShard.blobContainer().getFile(digest).length();
            assertThat(length, greaterThanOrEqualTo(1L));
        }

        for (Thread writer : writers) {
            writer.join(6000);
        }
    }
}
