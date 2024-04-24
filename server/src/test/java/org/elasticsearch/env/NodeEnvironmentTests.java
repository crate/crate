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
package org.elasticsearch.env;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import io.crate.common.collections.Sets;
import io.crate.common.io.IOUtils;

@LuceneTestCase.SuppressFileSystems("ExtrasFS") // TODO: fix test to allow extras
public class NodeEnvironmentTests extends ESTestCase {
    private final IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("foo", Settings.EMPTY);

    public void testNodeLock() throws IOException {
        final Settings settings = buildEnvSettings(Settings.EMPTY);
        NodeEnvironment env = newNodeEnvironment(settings);
        List<String> dataPaths = Environment.PATH_DATA_SETTING.get(settings);

        // Reuse the same location and attempt to lock again
        assertThatThrownBy(() -> new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings)))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("failed to obtain node lock");

        // Close the environment that holds the lock and make sure we can get the lock after release
        env.close();
        env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
        assertThat(env.nodeDataPaths()).hasSize(dataPaths.size());

        for (int i = 0; i < dataPaths.size(); i++) {
            assertThat(env.nodeDataPaths()[i].startsWith(PathUtils.get(dataPaths.get(i)))).isTrue();
        }
        env.close();
        assertThat(env.lockedShards()).isEmpty();
    }

    public void testSegmentInfosTracing() {
        // Defaults to not hooking up std out
        assertThat(SegmentInfos.getInfoStream()).isNull();

        try {
            // False means don't hook up std out
            NodeEnvironment.applySegmentInfosTrace(
                Settings.builder().put(NodeEnvironment.ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING.getKey(), false).build());
            assertThat(SegmentInfos.getInfoStream()).isNull();

            // But true means hook std out up statically
            NodeEnvironment.applySegmentInfosTrace(
                Settings.builder().put(NodeEnvironment.ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING.getKey(), true).build());
            assertThat(SegmentInfos.getInfoStream()).isEqualTo(System.out);
        } finally {
            // Clean up after ourselves
            SegmentInfos.setInfoStream(null);
        }
    }

    public void testShardLock() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();

        Index index = new Index("foo", "fooUUID");
        ShardLock fooLock = env.shardLock(new ShardId(index, 0), "1");
        assertThat(fooLock.getShardId()).isEqualTo(new ShardId(index, 0));

        try {
            env.shardLock(new ShardId(index, 0), "2");
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }
        for (Path path : env.indexPaths(index)) {
            Files.createDirectories(path.resolve("0"));
            Files.createDirectories(path.resolve("1"));
        }
        try {
            env.lockAllForIndex(index, idxSettings, "3", randomIntBetween(0, 10));
            fail("shard 0 is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }

        fooLock.close();
        // can lock again?
        env.shardLock(new ShardId(index, 0), "4").close();

        List<ShardLock> locks = env.lockAllForIndex(index, idxSettings, "5", randomIntBetween(0, 10));
        try {
            env.shardLock(new ShardId(index, 0), "6");
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }
        IOUtils.close(locks);
        assertThat(env.lockedShards().isEmpty())
            .as("LockedShards: " + env.lockedShards())
            .isTrue();
        env.close();
    }

    public void testAvailableIndexFolders() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final int numIndices = randomIntBetween(1, 10);
        Set<String> actualPaths = new HashSet<>();
        for (int i = 0; i < numIndices; i++) {
            Index index = new Index("foo" + i, "fooUUID" + i);
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
                actualPaths.add(path.getFileName().toString());
            }
        }

        assertThat(actualPaths).isEqualTo(env.availableIndexFolders());
        assertThat(env.lockedShards().isEmpty())
            .as("LockedShards: " + env.lockedShards())
            .isTrue();
        env.close();
    }

    public void testAvailableIndexFoldersWithExclusions() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final int numIndices = randomIntBetween(1, 10);
        Set<String> excludedPaths = new HashSet<>();
        Set<String> actualPaths = new HashSet<>();
        for (int i = 0; i < numIndices; i++) {
            Index index = new Index("foo" + i, "fooUUID" + i);
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
                actualPaths.add(path.getFileName().toString());
            }
            if (randomBoolean()) {
                excludedPaths.add(env.indexPaths(index)[0].getFileName().toString());
            }
        }

        assertThat(Sets.difference(actualPaths, excludedPaths)).isEqualTo(env.availableIndexFolders(excludedPaths::contains));
        assertThat(env.lockedShards().isEmpty())
            .as("LockedShards: " + env.lockedShards())
            .isTrue();
        env.close();
    }

    public void testResolveIndexFolders() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final int numIndices = randomIntBetween(1, 10);
        Map<String, List<Path>> actualIndexDataPaths = new HashMap<>();
        for (int i = 0; i < numIndices; i++) {
            Index index = new Index("foo" + i, "fooUUID" + i);
            Path[] indexPaths = env.indexPaths(index);
            for (Path path : indexPaths) {
                Files.createDirectories(path);
                String fileName = path.getFileName().toString();
                List<Path> paths = actualIndexDataPaths.get(fileName);
                if (paths == null) {
                    paths = new ArrayList<>();
                }
                paths.add(path);
                actualIndexDataPaths.put(fileName, paths);
            }
        }
        for (Map.Entry<String, List<Path>> actualIndexDataPathEntry : actualIndexDataPaths.entrySet()) {
            List<Path> actual = actualIndexDataPathEntry.getValue();
            Path[] actualPaths = actual.toArray(new Path[actual.size()]);
            assertThat(actualPaths).isEqualTo(env.resolveIndexFolder(actualIndexDataPathEntry.getKey()));
        }
        assertThat(env.lockedShards().isEmpty())
            .as("LockedShards: " + env.lockedShards())
            .isTrue();
        env.close();
    }

    public void testDeleteSafe() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final Index index = new Index("foo", "fooUUID");
        ShardLock fooLock = env.shardLock(new ShardId(index, 0), "1");
        assertThat(fooLock.getShardId()).isEqualTo(new ShardId(index, 0));

        for (Path path : env.indexPaths(index)) {
            Files.createDirectories(path.resolve("0"));
            Files.createDirectories(path.resolve("1"));
        }

        try {
            env.deleteShardDirectorySafe(new ShardId(index, 0), idxSettings);
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }

        for (Path path : env.indexPaths(index)) {
            assertThat(Files.exists(path.resolve("0"))).isTrue();
            assertThat(Files.exists(path.resolve("1"))).isTrue();
        }

        env.deleteShardDirectorySafe(new ShardId(index, 1), idxSettings);

        for (Path path : env.indexPaths(index)) {
            assertThat(Files.exists(path.resolve("0"))).isTrue();
            assertThat(Files.exists(path.resolve("1"))).isFalse();
        }

        try {
            env.deleteIndexDirectorySafe(index, randomIntBetween(0, 10), idxSettings);
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }
        fooLock.close();

        for (Path path : env.indexPaths(index)) {
            assertThat(Files.exists(path)).isTrue();
        }

        final AtomicReference<Throwable> threadException = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch blockLatch = new CountDownLatch(1);
        final CountDownLatch start = new CountDownLatch(1);
        if (randomBoolean()) {
            Thread t = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.error("unexpected error", e);
                    threadException.set(e);
                    latch.countDown();
                    blockLatch.countDown();
                }

                @Override
                protected void doRun() throws Exception {
                    start.await();
                    try (ShardLock autoCloses = env.shardLock(new ShardId(index, 0), "2")) {
                        blockLatch.countDown();
                        Thread.sleep(randomIntBetween(1, 10));
                    }
                    latch.countDown();
                }
            });
            t.start();
        } else {
            latch.countDown();
            blockLatch.countDown();
        }
        start.countDown();
        blockLatch.await();

        env.deleteIndexDirectorySafe(index, 5000, idxSettings);

        assertThat(threadException.get()).isNull();

        for (Path path : env.indexPaths(index)) {
            assertThat(Files.exists(path)).isFalse();
        }
        latch.await();
        assertThat(env.lockedShards().isEmpty())
            .as("LockedShards: " + env.lockedShards())
            .isTrue();
        env.close();
    }

    public void testStressShardLock() throws IOException, InterruptedException {
        class Int {
            int value = 0;
        }
        final NodeEnvironment env = newNodeEnvironment();
        final int shards = randomIntBetween(2, 10);
        final Int[] counts = new Int[shards];
        final AtomicInteger[] countsAtomic = new AtomicInteger[shards];
        final AtomicInteger[] flipFlop = new AtomicInteger[shards];

        for (int i = 0; i < counts.length; i++) {
            counts[i] = new Int();
            countsAtomic[i] = new AtomicInteger();
            flipFlop[i] = new AtomicInteger();
        }

        Thread[] threads = new Thread[randomIntBetween(2, 5)];
        final CountDownLatch latch = new CountDownLatch(1);
        final int iters = scaledRandomIntBetween(10000, 100000);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        fail(e.getMessage());
                    }
                    for (int i = 0; i < iters; i++) {
                        int shard = randomIntBetween(0, counts.length - 1);
                        try {
                            try (ShardLock autoCloses = env.shardLock(new ShardId("foo", "fooUUID", shard), "1",
                                                                      scaledRandomIntBetween(0, 10))) {
                                counts[shard].value++;
                                countsAtomic[shard].incrementAndGet();
                                assertThat(1).isEqualTo(flipFlop[shard].incrementAndGet());
                                assertThat(0).isEqualTo(flipFlop[shard].decrementAndGet());
                            }
                        } catch (ShardLockObtainFailedException ex) {
                            // ok
                        }
                    }
                }
            };
            threads[i].start();
        }
        latch.countDown(); // fire the threads up
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        assertThat(env.lockedShards().isEmpty())
            .as("LockedShards: " + env.lockedShards())
            .isTrue();
        for (int i = 0; i < counts.length; i++) {
            assertThat(counts[i].value > 0).isTrue();
            assertThat(0).isEqualTo(flipFlop[i].get());
            assertThat(countsAtomic[i].get()).isEqualTo(counts[i].value);
        }
        env.close();
    }

    public void testCustomDataPaths() throws Exception {
        String[] dataPaths = tmpPaths();
        NodeEnvironment env = newNodeEnvironment(dataPaths, "/tmp", Settings.EMPTY);

        Index index = new Index("myindex", "myindexUUID");
        ShardId sid = new ShardId(index, 0);

        assertThat(env.availableShardPaths(sid)).isEqualTo(env.availableShardPaths(sid));
        assertThat(env.resolveCustomLocation("/tmp/foo", sid).toAbsolutePath())
            .isEqualTo(PathUtils.get("/tmp/foo/0/" + index.getUUID() + "/0").toAbsolutePath());

        assertThat(env.availableShardPaths(sid))
            .as("shard paths with a custom data_path should contain only regular paths")
            .isEqualTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID() + "/0"));

        assertThat(env.indexPaths(index))
            .as("index paths uses the regular template")
            .isEqualTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID()));

        assertThat(env.availableShardPaths(sid)).isEqualTo(env.availableShardPaths(sid));
        assertThat(env.resolveCustomLocation("/tmp/foo", sid).toAbsolutePath())
            .isEqualTo(PathUtils.get("/tmp/foo/0/" + index.getUUID() + "/0").toAbsolutePath());

        assertThat(env.availableShardPaths(sid))
            .as("shard paths with a custom data_path should contain only regular paths")
            .isEqualTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID() + "/0"));

        assertThat(env.indexPaths(index))
            .as("index paths uses the regular template")
            .isEqualTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID()));

        env.close();
    }

    public void testNodeIdNotPersistedAtInitialization() throws IOException {
        NodeEnvironment env = newNodeEnvironment(new String[0], Settings.builder()
            .put("node.local_storage", false)
            .put("node.master", false)
            .put("node.data", false)
            .build());
        String nodeID = env.nodeId();
        env.close();
        final String[] paths = tmpPaths();
        env = newNodeEnvironment(paths, Settings.EMPTY);
        assertThat(env.nodeId())
            .as("previous node didn't have local storage enabled, id should change")
            .isNotEqualTo(nodeID);
        nodeID = env.nodeId();
        env.close();
        env = newNodeEnvironment(paths, Settings.EMPTY);
        assertThat(env.nodeId()).isNotEqualTo(nodeID);
        env.close();
        env = newNodeEnvironment(Settings.EMPTY);
        assertThat(env.nodeId()).isNotEqualTo(nodeID);
        env.close();
    }

    public void testExistingTempFiles() throws IOException {
        String[] paths = tmpPaths();
        // simulate some previous left over temp files
        for (String path : randomSubsetOf(randomIntBetween(1, paths.length), paths)) {
            final Path nodePath = NodeEnvironment.resolveNodePath(PathUtils.get(path));
            Files.createDirectories(nodePath);
            Files.createFile(nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME));
            if (randomBoolean()) {
                Files.createFile(nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".tmp"));
            }
            if (randomBoolean()) {
                Files.createFile(nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".final"));
            }
        }
        NodeEnvironment env = newNodeEnvironment(paths, Settings.EMPTY);
        env.close();

        // check we clean up
        for (String path: paths) {
            final Path nodePath = NodeEnvironment.resolveNodePath(PathUtils.get(path));
            final Path tempFile = nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME);
            assertThat(Files.exists(tempFile))
                .as(tempFile + " should have been cleaned")
                .isFalse();
            final Path srcTempFile = nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".src");
            assertThat(Files.exists(srcTempFile))
                .as(srcTempFile + " should have been cleaned")
                .isFalse();
            final Path targetTempFile = nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".target");
            assertThat(Files.exists(targetTempFile))
                .as(targetTempFile + " should have been cleaned")
                .isFalse();
        }
    }

    public void testEnsureNoShardDataOrIndexMetadata() throws IOException {
        Settings settings = buildEnvSettings(Settings.EMPTY);
        Index index = new Index("test", "testUUID");

        // build settings using same path.data as original but with node.data=false and node.master=false
        Settings noDataNoMasterSettings = Settings.builder()
            .put(settings)
            .put(Node.NODE_DATA_SETTING.getKey(), false)
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .build();

        // test that we can create data=false and master=false with no meta information
        newNodeEnvironment(noDataNoMasterSettings).close();

        Path indexPath;
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
            }
            indexPath = env.indexPaths(index)[0];
        }

        verifyFailsOnMetadata(noDataNoMasterSettings, indexPath);

        // build settings using same path.data as original but with node.data=false
        Settings noDataSettings = Settings.builder()
            .put(settings)
            .put(Node.NODE_DATA_SETTING.getKey(), false).build();

        String shardDataDirName = Integer.toString(randomInt(10));

        // test that we can create data=false env with only meta information. Also create shard data for following asserts
        try (NodeEnvironment env = newNodeEnvironment(noDataSettings)) {
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(shardDataDirName));
            }
        }

        verifyFailsOnShardData(noDataSettings, indexPath, shardDataDirName);

        // assert that we get the stricter message on meta-data when both conditions fail
        verifyFailsOnMetadata(noDataNoMasterSettings, indexPath);

        // build settings using same path.data as original but with node.master=false
        Settings noMasterSettings = Settings.builder()
            .put(settings)
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .build();

        // test that we can create master=false env regardless of data.
        newNodeEnvironment(noMasterSettings).close();

        // test that we can create data=true, master=true env. Also remove state dir to leave only shard data for following asserts
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            for (Path path : env.indexPaths(index)) {
                Files.delete(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
            }
        }

        // assert that we fail on shard data even without the metadata dir.
        verifyFailsOnShardData(noDataSettings, indexPath, shardDataDirName);
        verifyFailsOnShardData(noDataNoMasterSettings, indexPath, shardDataDirName);
    }

    private void verifyFailsOnShardData(Settings settings, Path indexPath, String shardDataDirName) {
        assertThatThrownBy(() -> newNodeEnvironment(settings).close())
            .as("Must fail creating NodeEnvironment on a data path that has shard data if node.data=false")
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining(indexPath.resolve(shardDataDirName).toAbsolutePath().toString())
            .hasMessageStartingWith(
                "Node is started with "+ Node.NODE_DATA_SETTING.getKey() + "=false, but has shard data");
    }

    private void verifyFailsOnMetadata(Settings settings, Path indexPath) {
        assertThatThrownBy(() -> newNodeEnvironment(settings).close())
            .as("Must fail creating NodeEnvironment on a data path that has index meta-data if node.data=false and node.master=false")
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining(indexPath.resolve(MetadataStateFormat.STATE_DIR_NAME).toAbsolutePath().toString())
            .hasMessageStartingWith(
                    "Node is started with "
                            + Node.NODE_DATA_SETTING.getKey()
                            + "=false and "
                            + Node.NODE_MASTER_SETTING.getKey()
                            + "=false, but has index metadata");
    }

    /** Converts an array of Strings to an array of Paths, adding an additional child if specified */
    private Path[] stringsToPaths(String[] strings, String additional) {
        Path[] locations = new Path[strings.length];
        for (int i = 0; i < strings.length; i++) {
            locations[i] = PathUtils.get(strings[i], additional);
        }
        return locations;
    }

    @Override
    public String[] tmpPaths() {
        final int numPaths = randomIntBetween(1, 3);
        final String[] absPaths = new String[numPaths];
        for (int i = 0; i < numPaths; i++) {
            absPaths[i] = createTempDir().toAbsolutePath().toString();
        }
        return absPaths;
    }

    @Override
    public NodeEnvironment newNodeEnvironment() throws IOException {
        return newNodeEnvironment(Settings.EMPTY);
    }

    @Override
    public NodeEnvironment newNodeEnvironment(Settings settings) throws IOException {
        Settings build = buildEnvSettings(settings);
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }

    public Settings buildEnvSettings(Settings settings) {
        return Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put(settings).build();
    }

    public NodeEnvironment newNodeEnvironment(String[] dataPaths, Settings settings) throws IOException {
        Settings build = Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .putList(Environment.PATH_DATA_SETTING.getKey(), dataPaths).build();
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }

    public NodeEnvironment newNodeEnvironment(String[] dataPaths, String sharedDataPath, Settings settings) throws IOException {
        Settings build = Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), sharedDataPath)
            .putList(Environment.PATH_DATA_SETTING.getKey(), dataPaths).build();
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }
}
