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

package org.elasticsearch.snapshots.mockstore;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsRepository;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import io.crate.analyze.repositories.TypeSettings;

public class MockRepository extends FsRepository {

    private static final Logger logger = LogManager.getLogger(MockRepository.class);

    public static class Plugin extends org.elasticsearch.plugins.Plugin implements RepositoryPlugin {

        public static final Setting<String> USERNAME_SETTING = Setting.simpleString("secret.mock.username", Property.NodeScope);
        public static final Setting<String> PASSWORD_SETTING =
            Setting.simpleString("secret.mock.password", Property.NodeScope);


        @Override
        public Map<String, Repository.Factory> getRepositories(Environment env,
                                                               NamedXContentRegistry namedXContentRegistry,
                                                               ClusterService clusterService,
                                                               RecoverySettings recoverySettings) {
            Repository.Factory factory = new Repository.Factory() {

                @Override
                public Repository create(RepositoryMetadata metadata) throws Exception {
                    return new MockRepository(
                        metadata,
                        env,
                        namedXContentRegistry,
                        clusterService,
                        recoverySettings
                    );
                }

                @Override
                public TypeSettings settings() {
                    return new TypeSettings(mandatorySettings(), optionalSettings());
                }
            };
            return Map.of("mock", factory);
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(USERNAME_SETTING, PASSWORD_SETTING);
        }
    }

    private final AtomicLong failureCounter = new AtomicLong();

    public long getFailureCount() {
        return failureCounter.get();
    }

    private final double randomControlIOExceptionRate;

    private final double randomDataFileIOExceptionRate;

    private final boolean useLuceneCorruptionException;

    private final long maximumNumberOfFailures;

    private final long waitAfterUnblock;

    private final String randomPrefix;

    private final Environment env;

    private volatile boolean blockOnAnyFiles;

    private volatile boolean blockOnDataFiles;

    private volatile boolean blockOnDeleteIndexN;

    /**
     * Allows blocking on writing the index-N blob and subsequently failing it on unblock.
     * This is a way to enforce blocking the finalization of a snapshot, while permitting other IO operations to proceed unblocked.
     */
    private volatile boolean blockAndFailOnWriteIndexFile;

    /**
     * Same as {@link #blockAndFailOnWriteIndexFile} but proceeds without error after unblock.
     */
    private volatile boolean blockOnWriteIndexFile;

    /** Allows blocking on writing the snapshot file at the end of snapshot creation to simulate a died master node */
    private volatile boolean blockAndFailOnWriteSnapFile;


    /**
     * Reading blobs will fail with an {@link AssertionError} once the repository has been blocked once.
     */
    private volatile boolean failReadsAfterUnblock;
    private volatile boolean throwReadErrorAfterUnblock = false;

    private volatile boolean blocked = false;

    public MockRepository(RepositoryMetadata metadata, Environment environment,
                          NamedXContentRegistry namedXContentRegistry, ClusterService clusterService,
                          RecoverySettings recoverySettings) {
        super(overrideSettings(metadata, environment), environment, namedXContentRegistry, clusterService, recoverySettings);
        randomControlIOExceptionRate = metadata.settings().getAsDouble("random_control_io_exception_rate", 0.0);
        randomDataFileIOExceptionRate = metadata.settings().getAsDouble("random_data_file_io_exception_rate", 0.0);
        useLuceneCorruptionException = metadata.settings().getAsBoolean("use_lucene_corruption", false);
        maximumNumberOfFailures = metadata.settings().getAsLong("max_failure_number", 100L);
        blockOnAnyFiles = metadata.settings().getAsBoolean("block_on_control", false);
        blockOnDataFiles = metadata.settings().getAsBoolean("block_on_data", false);
        blockAndFailOnWriteSnapFile = metadata.settings().getAsBoolean("block_on_snap", false);
        randomPrefix = metadata.settings().get("random", "default");
        waitAfterUnblock = metadata.settings().getAsLong("wait_after_unblock", 0L);
        env = environment;
        logger.info("starting mock repository with random prefix {}", randomPrefix);
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return overrideSettings(super.getMetadata(), env);
    }

    private static RepositoryMetadata overrideSettings(RepositoryMetadata metadata, Environment environment) {
        // TODO: use another method of testing not being able to read the test file written by the master...
        // this is super duper hacky
        if (metadata.settings().getAsBoolean("localize_location", false)) {
            Path location = PathUtils.get(metadata.settings().get("location"));
            location = location.resolve(Integer.toString(environment.hashCode()));
            return new RepositoryMetadata(metadata.name(), metadata.type(),
                                          Settings.builder().put(metadata.settings()).put("location", location.toAbsolutePath()).build());
        } else {
            return metadata;
        }
    }

    private long incrementAndGetFailureCount() {
        return failureCounter.incrementAndGet();
    }

    @Override
    protected void doStop() {
        unblock();
        super.doStop();
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return new MockBlobStore(super.createBlobStore());
    }

    public synchronized void unblock() {
        blocked = false;
        // Clean blocking flags, so we wouldn't try to block again
        blockOnDataFiles = false;
        blockOnAnyFiles = false;
        blockAndFailOnWriteIndexFile = false;
        blockOnWriteIndexFile = false;
        blockAndFailOnWriteSnapFile = false;
        blockOnDeleteIndexN = false;
        this.notifyAll();
    }

    public void blockOnDataFiles(boolean blocked) {
        blockOnDataFiles = blocked;
    }

    public void setBlockOnAnyFiles(boolean blocked) {
        blockOnAnyFiles = blocked;
    }

    public void setBlockAndFailOnWriteSnapFiles(boolean blocked) {
        blockAndFailOnWriteSnapFile = blocked;
    }

    public void setBlockAndFailOnWriteIndexFile() {
        assert blockOnWriteIndexFile == false : "Either fail or wait after blocking on index-N not both";
        blockAndFailOnWriteIndexFile = true;
    }

    public void setBlockOnWriteIndexFile() {
        assert blockAndFailOnWriteIndexFile == false : "Either fail or wait after blocking on index-N not both";
        blockOnWriteIndexFile = true;
    }

    public void setBlockOnDeleteIndexFile() {
        blockOnDeleteIndexN = true;
    }

    public void setFailReadsAfterUnblock(boolean failReadsAfterUnblock) {
        this.failReadsAfterUnblock = failReadsAfterUnblock;
    }

    public boolean blocked() {
        return blocked;
    }

    private synchronized boolean blockExecution() {
        logger.debug("[{}] Blocking execution", metadata.name());
        boolean wasBlocked = false;
        try {
            while (blockOnDataFiles || blockOnAnyFiles || blockAndFailOnWriteIndexFile || blockOnWriteIndexFile ||
                    blockAndFailOnWriteSnapFile || blockOnDeleteIndexN) {
                blocked = true;
                this.wait();
                wasBlocked = true;
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        logger.debug("[{}] Unblocking execution", metadata.name());
        if (wasBlocked && failReadsAfterUnblock) {
            logger.debug("[{}] Next read operations will fail", metadata.name());
            this.throwReadErrorAfterUnblock = true;
        }
        return wasBlocked;
    }

    public class MockBlobStore extends BlobStoreWrapper {
        ConcurrentMap<String, AtomicLong> accessCounts = new ConcurrentHashMap<>();

        private long incrementAndGet(String path) {
            AtomicLong value = accessCounts.get(path);
            if (value == null) {
                value = accessCounts.putIfAbsent(path, new AtomicLong(1));
            }
            if (value != null) {
                return value.incrementAndGet();
            }
            return 1;
        }

        public MockBlobStore(BlobStore delegate) {
            super(delegate);
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new MockBlobContainer(super.blobContainer(path));
        }

        private class MockBlobContainer extends BlobContainerWrapper {

            private boolean shouldFail(String blobName, double probability) {
                if (probability > 0.0) {
                    String path = path().add(blobName).buildAsString() + randomPrefix;
                    path += "/" + incrementAndGet(path);
                    logger.info("checking [{}] [{}]", path, Math.abs(hashCode(path)) < Integer.MAX_VALUE * probability);
                    return Math.abs(hashCode(path)) < Integer.MAX_VALUE * probability;
                } else {
                    return false;
                }
            }

            private int hashCode(String path) {
                try {
                    MessageDigest digest = MessageDigest.getInstance("MD5");
                    byte[] bytes = digest.digest(path.getBytes("UTF-8"));
                    int i = 0;
                    return ((bytes[i++] & 0xFF) << 24) | ((bytes[i++] & 0xFF) << 16)
                           | ((bytes[i++] & 0xFF) << 8) | (bytes[i++] & 0xFF);
                } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
                    throw new ElasticsearchException("cannot calculate hashcode", ex);
                }
            }

            private void maybeIOExceptionOrBlock(String blobName) throws IOException {
                if (blobName.startsWith("__")) {
                    if (shouldFail(blobName, randomDataFileIOExceptionRate) && (incrementAndGetFailureCount() < maximumNumberOfFailures)) {
                        logger.info("throwing random IOException for file [{}] at path [{}]", blobName, path());
                        if (useLuceneCorruptionException) {
                            throw new CorruptIndexException("Random corruption", "random file");
                        } else {
                            throw new IOException("Random IOException");
                        }
                    } else if (blockOnDataFiles) {
                        blockExecutionAndMaybeWait(blobName);
                    }
                } else {
                    if (shouldFail(blobName, randomControlIOExceptionRate) && (incrementAndGetFailureCount() < maximumNumberOfFailures)) {
                        logger.info("throwing random IOException for file [{}] at path [{}]", blobName, path());
                        throw new IOException("Random IOException");
                    } else if (blockOnAnyFiles) {
                        blockExecutionAndMaybeWait(blobName);
                    } else if (blobName.startsWith("snap-") && blockAndFailOnWriteSnapFile) {
                        blockExecutionAndFail(blobName);
                    }
                }
            }

            private void blockExecutionAndMaybeWait(final String blobName) throws IOException {
                logger.info("[{}] blocking I/O operation for file [{}] at path [{}]", metadata.name(), blobName, path());
                final boolean wasBlocked = blockExecution();
                if (wasBlocked && lifecycle.stoppedOrClosed()) {
                    throw new IOException("already closed");
                }
                if (wasBlocked && waitAfterUnblock > 0) {
                    try {
                        // Delay operation after unblocking
                        // So, we can start node shutdown while this operation is still running.
                        Thread.sleep(waitAfterUnblock);
                    } catch (InterruptedException ex) {
                        //
                    }
                }
            }

            /**
             * Blocks an I/O operation on the blob fails and throws an exception when unblocked
             */
            private void blockExecutionAndFail(final String blobName) throws IOException {
                logger.info("blocking I/O operation for file [{}] at path [{}]", blobName, path());
                blockExecution();
                throw new IOException("exception after block");
            }

            private void maybeReadErrorAfterBlock(final String blobName) {
                if (throwReadErrorAfterUnblock) {
                    throw new AssertionError("Read operation are not allowed anymore at this point [blob=" + blobName + "]");
                }
            }

            MockBlobContainer(BlobContainer delegate) {
                super(delegate);
            }

            @Override
            public InputStream readBlob(String name) throws IOException {
                maybeReadErrorAfterBlock(name);
                maybeIOExceptionOrBlock(name);
                return super.readBlob(name);
            }

            @Override
            public InputStream readBlob(String name, long position, long length) throws IOException {
                maybeReadErrorAfterBlock(name);
                maybeIOExceptionOrBlock(name);
                return super.readBlob(name, position, length);
            }

            @Override
            public void delete() throws IOException {
                for (BlobContainer child : children().values()) {
                    child.delete();
                }
                final Map<String, BlobMetadata> blobs = listBlobs();
                for (String blob : blobs.values().stream().map(BlobMetadata::name).collect(Collectors.toList())) {
                    maybeIOExceptionOrBlock(blob);
                    deleteBlobsIgnoringIfNotExists(Collections.singletonList(blob));
                }
                blobStore().blobContainer(path().parent()).deleteBlobsIgnoringIfNotExists(
                    List.of(path().toArray()[path().toArray().length - 1]));
            }

            @Override
            public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
                if (blockOnDeleteIndexN && blobNames.stream().anyMatch(
                    name -> name.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX))) {
                    blockExecutionAndMaybeWait("index-{N}");
                }
                super.deleteBlobsIgnoringIfNotExists(blobNames);
            }

            @Override
            public Map<String, BlobMetadata> listBlobs() throws IOException {
                maybeIOExceptionOrBlock("");
                return super.listBlobs();
            }

            @Override
            public Map<String, BlobContainer> children() throws IOException {
                final Map<String, BlobContainer> res = new HashMap<>();
                for (Map.Entry<String, BlobContainer> entry : super.children().entrySet()) {
                    res.put(entry.getKey(), new MockBlobContainer(entry.getValue()));
                }
                return res;
            }

            @Override
            public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
                maybeIOExceptionOrBlock(blobNamePrefix);
                return super.listBlobsByPrefix(blobNamePrefix);
            }

            @Override
            public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                throws IOException {
                maybeIOExceptionOrBlock(blobName);
                super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
                if (RandomizedContext.current().getRandom().nextBoolean()) {
                    // for network based repositories, the blob may have been written but we may still
                    // get an error with the client connection, so an IOException here simulates this
                    maybeIOExceptionOrBlock(blobName);
                }
            }

            @Override
            public void writeBlobAtomic(final String blobName, final InputStream inputStream, final long blobSize,
                                        final boolean failIfAlreadyExists) throws IOException {
                final Random random = RandomizedContext.current().getRandom();
                if (blobName.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX)) {
                    if (blockAndFailOnWriteIndexFile) {
                        blockExecutionAndFail(blobName);
                    } else if (blockOnWriteIndexFile) {
                        blockExecutionAndMaybeWait(blobName);
                    }
                }
                if ((delegate() instanceof FsBlobContainer) && (random.nextBoolean())) {
                    // Simulate a failure between the write and move operation in FsBlobContainer
                    final String tempBlobName = FsBlobContainer.tempBlobName(blobName);
                    super.writeBlob(tempBlobName, inputStream, blobSize, failIfAlreadyExists);
                    maybeIOExceptionOrBlock(blobName);
                    final FsBlobContainer fsBlobContainer = (FsBlobContainer) delegate();
                    fsBlobContainer.moveBlobAtomic(tempBlobName, blobName, failIfAlreadyExists);
                } else {
                    // Atomic write since it is potentially supported
                    // by the delegating blob container
                    maybeIOExceptionOrBlock(blobName);
                    super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
                }
            }
        }
    }
}
