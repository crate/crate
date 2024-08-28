/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import static io.crate.testing.SQLTransportExecutor.REQUEST_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.tests.mockfile.FilterFileStore;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.apache.lucene.util.Constants;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.testing.UseRandomizedSchema;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
@UseRandomizedSchema(random = false)
public class DiskThresholdDeciderIT extends IntegTestCase {

    private static TestFileSystemProvider fileSystemProvider;

    private FileSystem defaultFileSystem;

    @Before
    public void installFilesystemProvider() {
        assertThat(defaultFileSystem).isNull();
        defaultFileSystem = PathUtils.getDefaultFileSystem();
        assertThat(fileSystemProvider).isNull();
        fileSystemProvider = new TestFileSystemProvider(defaultFileSystem, createTempDir());
        PathUtilsForTesting.installMock(fileSystemProvider.getFileSystem(null));
    }

    @After
    public void removeFilesystemProvider() {
        fileSystemProvider = null;
        assertThat(defaultFileSystem).isNotNull();
        PathUtilsForTesting.installMock(defaultFileSystem); // set the default filesystem back
        defaultFileSystem = null;
    }

    private static final long WATERMARK_BYTES = new ByteSizeValue(10, ByteSizeUnit.KB).getBytes();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Path dataPath = fileSystemProvider.getRootDir().resolve("node-" + nodeOrdinal);
        try {
            Files.createDirectories(dataPath);
        } catch (IOException e) {
            throw new AssertionError("unexpected", e);
        }
        fileSystemProvider.addTrackedPath(dataPath);
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Environment.PATH_DATA_SETTING.getKey(), dataPath)
                .put(FsService.ALWAYS_REFRESH_SETTING.getKey(), true)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(InternalSettingsPlugin.class);
    }

    public void testHighWatermarkNotExceeded() throws Throwable {
        cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        final String dataNodeName = cluster().startDataOnlyNode();

        final InternalClusterInfoService clusterInfoService
                = (InternalClusterInfoService) cluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        cluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> clusterInfoService.refresh());

        final String dataNode0Id = cluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();
        final Path dataNode0Path = cluster().getInstance(Environment.class, dataNodeName).dataFiles()[0];

        String indexName = "test";
        execute("create table " + indexName + "(x text) " +
            "clustered into 6 shards " +
            "with (column_policy='strict', \"store.stats_refresh_interval\" = '0ms', number_of_replicas = 0)");

        final long minShardSize = createReasonableSizedShards(indexName);

        // reduce disk size of node 0 so that no shards fit below the high watermark, forcing all shards onto the other data node
        // (subtract the translog size since the disk threshold decider ignores this and may therefore move the shard back again)
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES - 1L);
        assertBusy(() -> {
            refreshDiskUsage();
            assertThat(getShardRoutings(dataNode0Id, indexName)).isEmpty();
        });

        // increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES + 1L);
        assertBusy(() -> {
            refreshDiskUsage();
            assertThat(getShardRoutings(dataNode0Id, indexName)).hasSize(1);
        });
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testRestoreSnapshotAllocationDoesNotExceedWatermark() throws Exception {
        cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        final String dataNodeName = cluster().startDataOnlyNode();
        ensureStableCluster(3);

        execute(
            "create repository repo type fs with (location = ?, compress = ?)",
            new Object[] {
                randomRepoPath().toAbsolutePath().toString(),
                randomBoolean()
            }
        );

        final InternalClusterInfoService clusterInfoService
            = (InternalClusterInfoService) cluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        cluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> clusterInfoService.refresh());

        final String dataNode0Id = cluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();
        final Path dataNode0Path = cluster().getInstance(Environment.class, dataNodeName).dataFiles()[0];

        execute("create table tbl (x text) clustered into 6 shards with (number_of_replicas = 0, \"store.stats_refresh_interval\" = '0ms')");
        final long minShardSize = createReasonableSizedShards("tbl");

        execute("create snapshot repo.snap table tbl with (wait_for_completion = true)");
        execute("drop table tbl");

        //// reduce disk size of node 0 so that no shards fit below the low watermark, forcing shards to be assigned to the other data node
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES - 1L);
        refreshDiskUsage();

        execute("set global transient \"cluster.routing.rebalance.enable\" = 'none'");

        execute("restore snapshot repo.snap table tbl with (wait_for_completion = true)");

        assertBusy(() -> assertThat(getShardRoutings(dataNode0Id, "tbl")).isEmpty());


        execute("reset global \"cluster.routing.rebalance.enable\"");

        //// increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES + 1L);
        assertBusy(() -> {
            refreshDiskUsage();
            assertThat(getShardRoutings(dataNode0Id, "tbl")).hasSize(1);
        });
    }

    private Set<ShardRouting> getShardRoutings(String nodeId, String indexName) {
        final Set<ShardRouting> shardRoutings = new HashSet<>();
        ClusterStateResponse clusterStateResponse = FutureUtils.get(client().admin().cluster().state(new ClusterStateRequest().routingTable(true)));
        for (IndexShardRoutingTable indexShardRoutingTable : clusterStateResponse.getState().routingTable().index(indexName)) {
            for (ShardRouting shard : indexShardRoutingTable.shards()) {
                assertThat(shard.state()).isEqualTo(ShardRoutingState.STARTED);
                if (shard.currentNodeId().equals(nodeId)) {
                    shardRoutings.add(shard);
                }
            }
        }
        return shardRoutings;
    }

    /**
     * Index documents until all the shards are at least WATERMARK_BYTES in size, and return the size of the smallest shard
     * @param indexName
     */
    private long createReasonableSizedShards(String indexName) throws InterruptedException, ExecutionException {
        String tableName = "doc." + indexName; // UseRandomizedSchema is set to false
        while (true) {

            execute("insert into " + tableName + "(x) values (?)", new Object[]{randomAlphaOfLengthBetween(1000, 2000)});

            ensureGreen();
            execute("optimize table " + tableName + " with (max_num_segments=1)");

            refresh();

            var indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName).store(true)).get();
            final List<ShardStats> shardStatses = indicesStats.getIndex(indexName).getShards();

            final long[] shardSizes = new long[shardStatses.size()];
            for (ShardStats shardStats : shardStatses) {
                shardSizes[shardStats.getShardRouting().id()] = shardStats.getStats().getStore().sizeInBytes();
            }

            final long minShardSize = Arrays.stream(shardSizes).min().orElseThrow(() -> new AssertionError("no shards"));
            if (minShardSize > WATERMARK_BYTES) {
                return minShardSize;
            }
        }
    }

    private void refreshDiskUsage() throws ExecutionException, InterruptedException {
        ((InternalClusterInfoService) cluster().getCurrentMasterNodeInstance(ClusterInfoService.class)).refresh();
        // if the nodes were all under the low watermark already (but unbalanced) then a change in the disk usage doesn't trigger a reroute
        // even though it's now possible to achieve better balance, so we have to do an explicit reroute. TODO fix this?
        final ClusterInfo clusterInfo = cluster().getMasterNodeInstance(ClusterInfoService.class).getClusterInfo();
        if (StreamSupport.stream(clusterInfo.getNodeMostAvailableDiskUsages().values().spliterator(), false)
            .allMatch(cur -> cur.value.getFreeBytes() > WATERMARK_BYTES)) {

            var clusterRerouteResponse = client().admin().cluster()
                .execute(ClusterRerouteAction.INSTANCE, new ClusterRerouteRequest()).get();
            assertThat(clusterRerouteResponse.isAcknowledged()).isTrue();
        }

        var clusterHealthRequest = new ClusterHealthRequest()
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(true);
        ClusterHealthResponse response = FutureUtils.get(client().admin().cluster().health(clusterHealthRequest), REQUEST_TIMEOUT);
        assertThat(response.isTimedOut()).isFalse();
    }

    private static class TestFileStore extends FilterFileStore {

        private final Path path;

        private volatile long totalSpace = -1;

        TestFileStore(FileStore delegate, String scheme, Path path) {
            super(delegate, scheme);
            this.path = path;
        }

        @Override
        public String name() {
            return "fake"; // Lucene's is-spinning-disk check expects the device name here
        }

        @Override
        public long getTotalSpace() throws IOException {
            final long totalSpace = this.totalSpace;
            if (totalSpace == -1) {
                return super.getTotalSpace();
            } else {
                return totalSpace;
            }
        }

        public void setTotalSpace(long totalSpace) {
            assertThat(totalSpace).satisfiesAnyOf(l -> assertThat(l).isEqualTo(-1L), l -> assertThat(l).isGreaterThan(0L));
            this.totalSpace = totalSpace;
        }

        @Override
        public long getUsableSpace() throws IOException {
            final long totalSpace = this.totalSpace;
            if (totalSpace == -1) {
                return super.getUsableSpace();
            } else {
                return Math.max(0L, totalSpace - getTotalFileSize(path));
            }
        }

        @Override
        public long getUnallocatedSpace() throws IOException {
            final long totalSpace = this.totalSpace;
            if (totalSpace == -1) {
                return super.getUnallocatedSpace();
            } else {
                return Math.max(0L, totalSpace - getTotalFileSize(path));
            }
        }

        private static long getTotalFileSize(Path path) throws IOException {
            if (Files.isRegularFile(path)) {
                try {
                    return Files.size(path);
                } catch (NoSuchFileException | FileNotFoundException e) {
                    // probably removed
                    return 0L;
                }
            } else if (path.getFileName().toString().equals("_state") || path.getFileName().toString().equals("translog")) {
                // ignore metadata and translog, since the disk threshold decider only cares about the store size
                return 0L;
            } else {
                try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path)) {
                    long total = 0L;
                    for (Path subpath : directoryStream) {
                        total += getTotalFileSize(subpath);
                    }
                    return total;
                } catch (NotDirectoryException | NoSuchFileException | FileNotFoundException e) {
                    // probably removed
                    return 0L;
                }
            }
        }
    }

    private static class TestFileSystemProvider extends FilterFileSystemProvider {
        private final Map<Path, TestFileStore> trackedPaths = new ConcurrentHashMap<>();
        private final Path rootDir;

        TestFileSystemProvider(FileSystem delegateInstance, Path rootDir) {
            super("diskthreshold://", delegateInstance);
            this.rootDir = new FilterPath(rootDir, fileSystem);
        }

        Path getRootDir() {
            return rootDir;
        }

        void addTrackedPath(Path path) {
            assertThat(path.startsWith(rootDir)).withFailMessage("path + \" starts with \" + rootDir").isTrue();
            final FileStore fileStore;
            try {
                fileStore = super.getFileStore(path);
            } catch (IOException e) {
                throw new AssertionError("unexpected", e);
            }
            assertThat(trackedPaths.put(path, new TestFileStore(fileStore, getScheme(), path))).isNull();
        }

        @Override
        public FileStore getFileStore(Path path) {
            return getTestFileStore(path);
        }

        TestFileStore getTestFileStore(Path path) {
            if (path.endsWith(path.getFileSystem().getPath("nodes", "0"))) {
                path = path.getParent().getParent();
            }
            final TestFileStore fileStore = trackedPaths.get(path);
            if (fileStore != null) {
                return fileStore;
            }

            // On Linux, and only Linux, Lucene obtains a filestore for the index in order to determine whether it's on a spinning disk or
            // not so it can configure the merge scheduler accordingly
            assertThat(Constants.LINUX).withFailMessage(path + " not tracked and not on Linux").isTrue();
            final Set<Path> containingPaths = trackedPaths.keySet().stream().filter(path::startsWith).collect(Collectors.toSet());
            assertThat(containingPaths).withFailMessage(path + " not contained in a unique tracked path").hasSize(1);
            return trackedPaths.get(containingPaths.iterator().next());
        }
    }
}
