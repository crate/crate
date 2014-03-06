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

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import io.crate.blob.PutChunkAction;
import io.crate.blob.PutChunkRequest;
import io.crate.blob.StartBlobAction;
import io.crate.blob.StartBlobRequest;
import io.crate.blob.stats.BlobStatsAction;
import io.crate.blob.stats.BlobStatsRequest;
import io.crate.blob.stats.BlobStatsResponse;
import io.crate.common.Hex;
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.core.IsEqual.equalTo;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 0)
public class RecoveryTests extends CrateIntegrationTest {

    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(25, TimeUnit.MINUTES);

    // the time to sleep between chunk requests in upload
    private AtomicInteger timeBetweenChunks = new AtomicInteger();

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);

        Logger logger;
        ConsoleAppender consoleAppender;

        logger = Logger.getLogger(
                "org.elasticsearch.io.crate.blob.recovery.BlobRecoveryHandler");
        //logger.setLevel(Level.TRACE);
        consoleAppender = new ConsoleAppender(new PatternLayout("%r [%t] %-5p %c %x - %m\n"));
        logger.addAppender(consoleAppender);

        logger = Logger.getLogger("org.elasticsearch.io.crate.blob.DigestBlob");
        //logger.setLevel(Level.TRACE);
        consoleAppender = new ConsoleAppender(new PatternLayout("%r [%t] %-5p %c %x - %m\n"));
        logger.addAppender(consoleAppender);

        logger = Logger.getLogger(
                "org.elasticsearch.io.crate.integrationtests.RecoveryTests");
        //logger.setLevel(Level.TRACE);
        consoleAppender = new ConsoleAppender(new PatternLayout("%r [%t] %-5p %c %x - %m\n"));
        logger.addAppender(consoleAppender);
    }


    private byte[] getDigest(String content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            digest.reset();
            digest.update(content.getBytes());
            return digest.digest();
        } catch (NoSuchAlgorithmException e) {
        }
        return null;
    }

    private void uploadFile(Client client, String content) {
        byte[] digest = getDigest(content);
        byte[] contentBytes = content.getBytes();
        logger.trace("Uploading {} digest {}", content, Hex.encodeHexString(digest));
        BytesArray bytes = new BytesArray(new byte[]{contentBytes[0]});
        if (content.length() == 1) {
            client.execute(StartBlobAction.INSTANCE, new StartBlobRequest("test", digest, bytes,
                    true)).actionGet();
        } else {
            StartBlobRequest startBlobRequest = new StartBlobRequest("test", digest, bytes, false);
            client.execute(StartBlobAction.INSTANCE, startBlobRequest).actionGet();
            for (int i = 1; i < contentBytes.length; i++) {
                try {
                    Thread.sleep(timeBetweenChunks.get());
                } catch (InterruptedException ex) {
                }
                bytes = new BytesArray(new byte[]{contentBytes[i]});
                client.execute(PutChunkAction.INSTANCE,
                        new PutChunkRequest(
                                "test", digest, startBlobRequest.transferId(), bytes, i,
                                (i + 1) == content.length())
                ).actionGet();
            }
        }
        logger.trace("Upload finished {} digest {}", content, Hex.encodeHexString(digest));

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

        final String node1 = cluster().startNode();

        logger.trace("--> creating test index ...");
        cluster().client(node1).admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("blobs.enabled", true)
                ).execute().actionGet();

        logger.trace("--> starting [node2] ...");
        final String node2 = cluster().startNode();

        ensureGreen();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[numberOfWriters];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);

        logger.trace("--> starting {} blob upload threads", writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            writers[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        logger.trace("**** starting blob upload thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            uploadFile(cluster().client(node1), genFile(id));
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
            writers[i].start();
        }

        logger.trace("--> waiting for 2 blobs to be uploaded ...");
        while (cluster().client(node1).execute(BlobStatsAction.INSTANCE, new BlobStatsRequest().indices(
                "test"))
                .actionGet().blobShardStats()[0].blobStats().count() < 2) {
            Thread.sleep(10);
        }
        logger.trace("--> 2 blobs uploaded");

        // increase time between chunks in order to make sure that the upload is taking place while relocating
        timeBetweenChunks.set(10);
        stop.set(true);
        logger.trace("--> starting relocations...");
        for (int i = 0; i < numberOfRelocations; i++) {
            String fromNode = (i % 2 == 0) ? node1 : node2;
            String toNode = node1.equals(fromNode) ? node2 : node1;
            logger.trace("--> START relocate the shard from {} to {}", fromNode, toNode);
            cluster().client(node1).admin().cluster().prepareReroute()
                    .add(new MoveAllocationCommand(new ShardId("test", 0), fromNode, toNode))
                    .execute().actionGet();
            ClusterHealthResponse clusterHealthResponse = cluster().client(node1).admin().cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForRelocatingShards(0)
                    .setTimeout(ACCEPTABLE_RELOCATION_TIME).execute().actionGet();

            assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
            clusterHealthResponse = cluster().client(node2).admin().cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForRelocatingShards(0)
                    .setTimeout(ACCEPTABLE_RELOCATION_TIME).execute().actionGet();
            assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
            logger.trace("--> DONE relocate the shard from {} to {}", fromNode, toNode);
        }
        logger.trace("--> done relocations");

        logger.trace("--> marking and waiting for upload threads to stop ...");
        timeBetweenChunks.set(0);
        stop.set(true);
        stopLatch.await(60, TimeUnit.SECONDS);
        logger.trace("--> uploading threads stopped");

        BlobStatsResponse response = cluster().client(node2)
                .execute(BlobStatsAction.INSTANCE, new BlobStatsRequest().indices(
                        "test")).actionGet();
        assertTrue(response.blobShardStats().length > 0);
        long numBlobs = response.blobShardStats()[0].blobStats().count();
        long uploadedDocs = indexCounter.get();
        logger.trace("--> expected {} got {}", uploadedDocs, numBlobs);
        assertEquals(uploadedDocs, numBlobs);
    }
}
