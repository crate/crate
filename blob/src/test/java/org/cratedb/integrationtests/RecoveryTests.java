package org.cratedb.integrationtests;

import org.cratedb.blob.PutChunkAction;
import org.cratedb.blob.PutChunkRequest;
import org.cratedb.blob.StartBlobAction;
import org.cratedb.blob.StartBlobRequest;
import org.cratedb.blob.stats.BlobStatsAction;
import org.cratedb.blob.stats.BlobStatsRequest;
import org.cratedb.blob.stats.BlobStatsResponse;
import org.cratedb.common.Hex;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.junit.After;
import org.junit.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.core.IsEqual.equalTo;

public class RecoveryTests extends AbstractCrateNodesTests {

    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(25, TimeUnit.MINUTES);

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);

        Logger logger;
        ConsoleAppender consoleAppender;

        logger = Logger.getLogger(
                "org.elasticsearch.org.cratedb.blob.recovery.BlobRecoveryHandler");
        logger.setLevel(Level.DEBUG);
        consoleAppender = new ConsoleAppender(new PatternLayout("%r [%t] %-5p %c %x - %m\n"));
        logger.addAppender(consoleAppender);

        logger = Logger.getLogger("org.elasticsearch.org.cratedb.blob.DigestBlob");
        logger.setLevel(Level.TRACE);
        consoleAppender = new ConsoleAppender(new PatternLayout("%r [%t] %-5p %c %x - %m\n"));
        logger.addAppender(consoleAppender);

        logger = Logger.getLogger(
                "org.elasticsearch.org.cratedb.integrationtests.RecoveryTests");
        logger.setLevel(Level.INFO);
        consoleAppender = new ConsoleAppender(new PatternLayout("%r [%t] %-5p %c %x - %m\n"));
        logger.addAppender(consoleAppender);
    }

    @After
    public void shutdownNodes() {
        closeAllNodes();
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
        logger.info("Uploading {} digest {}", content, Hex.encodeHexString(digest));
        BytesArray bytes = new BytesArray(new byte[]{contentBytes[0]});
        if (content.length() == 1) {
            client.execute(StartBlobAction.INSTANCE, new StartBlobRequest("test", digest, bytes,
                    true)).actionGet();
        } else {
            StartBlobRequest startBlobRequest = new StartBlobRequest("test", digest, bytes, false);
            client.execute(StartBlobAction.INSTANCE, startBlobRequest).actionGet();
            for (int i = 1; i < contentBytes.length; i++) {
                try {
                    Thread.sleep(200);
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
        logger.info("--> starting [node1] ...");
        startNode("node1");

        logger.info("--> creating test index ...");
        client("node1").admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("blobs.enabled", true)
                            ).execute().actionGet();

        logger.info("--> starting [node2] ...");
        startNode("node2");

        client("node1").admin().cluster().prepareHealth(
                "test").setWaitForGreenStatus().execute().actionGet();
        client("node2").admin().cluster().prepareHealth(
                "test").setWaitForGreenStatus().execute().actionGet();

        final AtomicLong idGenerator = new AtomicLong();
        final AtomicLong indexCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] writers = new Thread[numberOfWriters];
        final CountDownLatch stopLatch = new CountDownLatch(writers.length);

        logger.info("--> starting {} blob upload threads", writers.length);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            writers[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        logger.info("**** starting blob upload thread {}", indexerId);
                        while (!stop.get()) {
                            long id = idGenerator.incrementAndGet();
                            uploadFile(client("node1"), genFile(id));
                            indexCounter.incrementAndGet();
                        }
                        logger.info("**** done indexing thread {}", indexerId);
                    } catch (Exception e) {
                        logger.warn("**** failed indexing thread {}", e, indexerId);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        logger.info("--> waiting for 2 blobs to be uploaded ...");
        while (client("node1").execute(BlobStatsAction.INSTANCE, new BlobStatsRequest().indices(
                "test"))
                .actionGet().blobShardStats()[0].blobStats().count() < 2) {
            Thread.sleep(10);
        }
        logger.info("--> 2 blobs uploaded");

        logger.info("--> starting relocations...");
        for (int i = 0; i < numberOfRelocations; i++) {
            String fromNode = "node" + (1 + (i % 2));
            String toNode = "node1".equals(fromNode) ? "node2" : "node1";
            logger.info("--> START relocate the shard from {} to {}", fromNode, toNode);
            client("node1").admin().cluster().prepareReroute()
                    .add(new MoveAllocationCommand(new ShardId("test", 0), fromNode, toNode))
                    .execute().actionGet();
            ClusterHealthResponse clusterHealthResponse = client("node1").admin().cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForRelocatingShards(0)
                    .setTimeout(ACCEPTABLE_RELOCATION_TIME).execute().actionGet();

            assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
            clusterHealthResponse = client("node2").admin().cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForRelocatingShards(0)
                    .setTimeout(ACCEPTABLE_RELOCATION_TIME).execute().actionGet();
            assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
            logger.info("--> DONE relocate the shard from {} to {}", fromNode, toNode);
        }
        logger.info("--> done relocations");

        logger.info("--> marking and waiting for upload threads to stop ...");
        stop.set(true);
        stopLatch.await();
        logger.info("--> uploading threads stopped");

        BlobStatsResponse response = client("node2")
                .execute(BlobStatsAction.INSTANCE, new BlobStatsRequest().indices(
                        "test")).actionGet();
        long numBlobs = response.blobShardStats()[0].blobStats().count();
        long uploadedDocs = indexCounter.get();
        logger.info("--> expected {} got {}", uploadedDocs, numBlobs);
        assertEquals(uploadedDocs, numBlobs);
    }
}
