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

package org.elasticsearch.monitor.fs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.lucene.util.Constants;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeEnvironment.NodePath;
import org.elasticsearch.test.ESTestCase;

import io.crate.common.collections.Tuple;


public class FsProbeTests extends ESTestCase {

    public void testFsInfo() throws IOException {

        try (NodeEnvironment env = newNodeEnvironment()) {
            FsProbe probe = new FsProbe(env);

            FsInfo stats = probe.stats(null);
            assertThat(stats).isNotNull();
            assertThat(stats.getTimestamp()).isGreaterThan(0L);

            if (Constants.LINUX) {
                assertThat(stats.getIoStats()).isNotNull();
                assertThat(stats.getIoStats().devicesStats).isNotNull();
                for (int i = 0; i < stats.getIoStats().devicesStats.length; i++) {
                    final FsInfo.DeviceStats deviceStats = stats.getIoStats().devicesStats[i];
                    assertThat(deviceStats).isNotNull();
                    assertThat(deviceStats.currentReadsCompleted).isGreaterThanOrEqualTo(0L);
                    assertThat(deviceStats.previousReadsCompleted).isEqualTo(-1L);
                    assertThat(deviceStats.currentSectorsRead).isGreaterThanOrEqualTo(0L);
                    assertThat(deviceStats.previousSectorsRead).isEqualTo(-1L);
                    assertThat(deviceStats.currentWritesCompleted).isGreaterThanOrEqualTo(0L);
                    assertThat(deviceStats.previousWritesCompleted).isEqualTo(-1L);
                    assertThat(deviceStats.currentSectorsWritten).isGreaterThanOrEqualTo(0L);
                    assertThat(deviceStats.previousSectorsWritten).isEqualTo(-1L);
                }
            } else {
                assertThat(stats.getIoStats()).isNull();
            }

            FsInfo.Path total = stats.getTotal();
            assertThat(total).isNotNull();
            assertThat(total.total).isGreaterThan(0L);
            assertThat(total.free).isGreaterThan(0L);
            assertThat(total.available).isGreaterThan(0L);

            for (FsInfo.Path path : stats) {
                assertThat(path).isNotNull();
                assertThat(path.getPath()).isNotEmpty();
                assertThat(path.getMount()).isNotEmpty();
                assertThat(path.getType()).isNotEmpty();
                assertThat(path.total).isGreaterThan(0L);
                assertThat(path.free).isGreaterThan(0L);
                assertThat(path.available).isGreaterThan(0L);
            }
        }
    }

    public void testFsInfoOverflow() throws Exception {
        final FsInfo.Path pathStats =
            new FsInfo.Path(
                "/foo/bar",
                null,
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong());

        addUntilOverflow(
            pathStats,
            p -> p.total,
            "total",
            () -> new FsInfo.Path("/foo/baz", null, randomNonNegativeLong(), 0, 0));

        addUntilOverflow(
            pathStats,
            p -> p.free,
            "free",
            () -> new FsInfo.Path("/foo/baz", null, 0, randomNonNegativeLong(), 0));

        addUntilOverflow(
            pathStats,
            p -> p.available,
            "available",
            () -> new FsInfo.Path("/foo/baz", null, 0, 0, randomNonNegativeLong()));

        // even after overflowing these should not be negative
        assertThat(pathStats.total).isGreaterThan(0L);
        assertThat(pathStats.free).isGreaterThan(0L);
        assertThat(pathStats.available).isGreaterThan(0L);
    }

    private void addUntilOverflow(
        final FsInfo.Path pathStats,
        final Function<FsInfo.Path, Long> getter,
        final String field,
        final Supplier<FsInfo.Path> supplier) {
        FsInfo.Path pathToAdd = supplier.get();
        while ((getter.apply(pathStats) + getter.apply(pathToAdd)) > 0) {
            // add a path to increase the total bytes until it overflows
            logger.info(
                "--> adding {} bytes to {}, {} will be: {}",
                getter.apply(pathToAdd),
                getter.apply(pathStats),
                field,
                getter.apply(pathStats) + getter.apply(pathToAdd));
            pathStats.add(pathToAdd);
            pathToAdd = supplier.get();
        }
        // this overflows
        logger.info(
            "--> adding {} bytes to {}, {} will be: {}",
            getter.apply(pathToAdd),
            getter.apply(pathStats),
            field,
            getter.apply(pathStats) + getter.apply(pathToAdd));
        assertThat(getter.apply(pathStats) + getter.apply(pathToAdd)).isLessThan(0L);
        pathStats.add(pathToAdd);
    }

    public void testIoStats() {
        final AtomicReference<List<String>> diskStats = new AtomicReference<>();
        diskStats.set(Arrays.asList(
            " 259       0 nvme0n1 336609 0 7923613 82813 10264051 0 182983933 52451441 0 2970886 52536260",
            " 259       1 nvme0n1p1 602 0 9919 131 1 0 1 0 0 19 131",
            " 259       2 nvme0n1p2 186 0 8626 18 24 0 60 20 0 34 38",
            " 259       3 nvme0n1p3 335733 0 7901620 82658 9592875 0 182983872 50843431 0 1737726 50926087",
            " 253       0 dm-0 287716 0 7184666 33457 8398869 0 118857776 18730966 0 1918440 18767169",
            " 253       1 dm-1 112 0 4624 13 0 0 0 0 0 5 13",
            " 253       2 dm-2 47802 0 710658 49312 1371977 0 64126096 33730596 0 1058193 33781827"));

        final FsProbe probe = new FsProbe(null) {
            @Override
            List<String> readProcDiskStats() throws IOException {
                return diskStats.get();
            }
        };

        final Set<Tuple<Integer, Integer>> devicesNumbers = new HashSet<>();
        devicesNumbers.add(new Tuple<>(253, 0));
        devicesNumbers.add(new Tuple<>(253, 2));
        final FsInfo.IoStats first = probe.ioStats(devicesNumbers, null);
        assertThat(first).isNotNull();
        assertThat(first.devicesStats[0].majorDeviceNumber).isEqualTo(253);
        assertThat(first.devicesStats[0].minorDeviceNumber).isEqualTo(0);
        assertThat(first.devicesStats[0].deviceName).isEqualTo("dm-0");
        assertThat(first.devicesStats[0].currentReadsCompleted).isEqualTo(287716L);
        assertThat(first.devicesStats[0].previousReadsCompleted).isEqualTo(-1L);
        assertThat(first.devicesStats[0].currentSectorsRead).isEqualTo(7184666L);
        assertThat(first.devicesStats[0].previousSectorsRead).isEqualTo(-1L);
        assertThat(first.devicesStats[0].currentWritesCompleted).isEqualTo(8398869L);
        assertThat(first.devicesStats[0].previousWritesCompleted).isEqualTo(-1L);
        assertThat(first.devicesStats[0].currentSectorsWritten).isEqualTo(118857776L);
        assertThat(first.devicesStats[0].previousSectorsWritten).isEqualTo(-1L);
        assertThat(first.devicesStats[1].majorDeviceNumber).isEqualTo(253);
        assertThat(first.devicesStats[1].minorDeviceNumber).isEqualTo(2);
        assertThat(first.devicesStats[1].deviceName).isEqualTo("dm-2");
        assertThat(first.devicesStats[1].currentReadsCompleted).isEqualTo(47802L);
        assertThat(first.devicesStats[1].previousReadsCompleted).isEqualTo(-1L);
        assertThat(first.devicesStats[1].currentSectorsRead).isEqualTo(710658L);
        assertThat(first.devicesStats[1].previousSectorsRead).isEqualTo(-1L);
        assertThat(first.devicesStats[1].currentWritesCompleted).isEqualTo(1371977L);
        assertThat(first.devicesStats[1].previousWritesCompleted).isEqualTo(-1L);
        assertThat(first.devicesStats[1].currentSectorsWritten).isEqualTo(64126096L);
        assertThat(first.devicesStats[1].previousSectorsWritten).isEqualTo(-1L);

        diskStats.set(Arrays.asList(
            " 259       0 nvme0n1 336870 0 7928397 82876 10264393 0 182986405 52451610 0 2971042 52536492",
            " 259       1 nvme0n1p1 602 0 9919 131 1 0 1 0 0 19 131",
            " 259       2 nvme0n1p2 186 0 8626 18 24 0 60 20 0 34 38",
            " 259       3 nvme0n1p3 335994 0 7906404 82721 9593184 0 182986344 50843529 0 1737840 50926248",
            " 253       0 dm-0 287734 0 7185242 33464 8398869 0 118857776 18730966 0 1918444 18767176",
            " 253       1 dm-1 112 0 4624 13 0 0 0 0 0 5 13",
            " 253       2 dm-2 48045 0 714866 49369 1372291 0 64128568 33730766 0 1058347 33782056"));

        final FsInfo previous = new FsInfo(System.currentTimeMillis(), first, new FsInfo.Path[0]);
        final FsInfo.IoStats second = probe.ioStats(devicesNumbers, previous);
        assertThat(second).isNotNull();
        assertThat(second.devicesStats[0].majorDeviceNumber).isEqualTo(253);
        assertThat(second.devicesStats[0].minorDeviceNumber).isEqualTo(0);
        assertThat(second.devicesStats[0].deviceName).isEqualTo("dm-0");
        assertThat(second.devicesStats[0].currentReadsCompleted).isEqualTo(287734L);
        assertThat(second.devicesStats[0].previousReadsCompleted).isEqualTo(287716L);
        assertThat(second.devicesStats[0].currentSectorsRead).isEqualTo(7185242L);
        assertThat(second.devicesStats[0].previousSectorsRead).isEqualTo(7184666L);
        assertThat(second.devicesStats[0].currentWritesCompleted).isEqualTo(8398869L);
        assertThat(second.devicesStats[0].previousWritesCompleted).isEqualTo(8398869L);
        assertThat(second.devicesStats[0].currentSectorsWritten).isEqualTo(118857776L);
        assertThat(second.devicesStats[0].previousSectorsWritten).isEqualTo(118857776L);
        assertThat(second.devicesStats[1].majorDeviceNumber).isEqualTo(253);
        assertThat(second.devicesStats[1].minorDeviceNumber).isEqualTo(2);
        assertThat(second.devicesStats[1].deviceName).isEqualTo("dm-2");
        assertThat(second.devicesStats[1].currentReadsCompleted).isEqualTo(48045L);
        assertThat(second.devicesStats[1].previousReadsCompleted).isEqualTo(47802L);
        assertThat(second.devicesStats[1].currentSectorsRead).isEqualTo(714866L);
        assertThat(second.devicesStats[1].previousSectorsRead).isEqualTo(710658L);
        assertThat(second.devicesStats[1].currentWritesCompleted).isEqualTo(1372291L);
        assertThat(second.devicesStats[1].previousWritesCompleted).isEqualTo(1371977L);
        assertThat(second.devicesStats[1].currentSectorsWritten).isEqualTo(64128568L);
        assertThat(second.devicesStats[1].previousSectorsWritten).isEqualTo(64126096L);

        assertThat(second.totalOperations).isEqualTo(575L);
        assertThat(second.totalReadOperations).isEqualTo(261L);
        assertThat(second.totalWriteOperations).isEqualTo(314L);
        assertThat(second.totalReadKilobytes).isEqualTo(2392L);
        assertThat(second.totalWriteKilobytes).isEqualTo(1236L);
    }

    public void testAdjustForHugeFilesystems() throws Exception {
        NodePath np = new FakeNodePath(createTempDir());
        assertThat(FsProbe.getFSInfo(np).total).isGreaterThanOrEqualTo(0L);
        assertThat(FsProbe.getFSInfo(np).free).isGreaterThanOrEqualTo(0L);
        assertThat(FsProbe.getFSInfo(np).available).isGreaterThanOrEqualTo(0L);
    }

    static class FakeNodePath extends NodeEnvironment.NodePath {
        public final FileStore fileStore;

        FakeNodePath(Path path) throws IOException {
            super(path);
            this.fileStore = new HugeFileStore();
        }
    }

    /**
     * Randomly returns negative values for disk space to simulate https://bugs.openjdk.java.net/browse/JDK-8162520
     */
    static class HugeFileStore extends FileStore {

        @Override
        public String name() {
            return "myHugeFS";
        }

        @Override
        public String type() {
            return "bigFS";
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public long getTotalSpace() throws IOException {
            return randomIntBetween(-1000, 1000);
        }

        @Override
        public long getUsableSpace() throws IOException {
            return randomIntBetween(-1000, 1000);
        }

        @Override
        public long getUnallocatedSpace() throws IOException {
            return randomIntBetween(-1000, 1000);
        }

        @Override
        public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
            return false;
        }

        @Override
        public boolean supportsFileAttributeView(String name) {
            return false;
        }

        @Override
        public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
            throw new UnsupportedOperationException("don't call me");
        }

        @Override
        public Object getAttribute(String attribute) throws IOException {
            throw new UnsupportedOperationException("don't call me");
        }

    }
}
