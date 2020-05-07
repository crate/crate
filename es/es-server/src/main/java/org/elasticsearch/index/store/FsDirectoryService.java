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

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import io.crate.common.io.IOUtils;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class FsDirectoryService extends DirectoryService {

    public static final Setting<LockFactory> INDEX_LOCK_FACTOR_SETTING = new Setting<>("index.store.fs.fs_lock", "native", (s) -> {
        switch (s) {
            case "native":
                return NativeFSLockFactory.INSTANCE;
            case "simple":
                return SimpleFSLockFactory.INSTANCE;
            default:
                throw new IllegalArgumentException("unrecognized [index.store.fs.fs_lock] \"" + s + "\": must be native or simple");
        } // can we set on both - node and index level, some nodes might be running on NFS so they might need simple rather than native
    }, Property.IndexScope, Property.NodeScope);

    private final ShardPath path;

    @Inject
    public FsDirectoryService(IndexSettings indexSettings, ShardPath path) {
        super(path.getShardId(), indexSettings);
        this.path = path;
    }

    @Override
    public Directory newDirectory() throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        Directory wrapped = newFSDirectory(location, lockFactory);
        Set<String> preLoadExtensions = new HashSet<>(
                indexSettings.getValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING));
        wrapped = setPreload(wrapped, location, lockFactory, preLoadExtensions);
        return wrapped;
    }

    protected Directory newFSDirectory(Path location, LockFactory lockFactory) throws IOException {
        final String storeType = indexSettings.getSettings()
            .get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey());
        IndexModule.Type type;
        if (IndexModule.Type.FS.match(storeType)) {
            type = IndexModule.defaultStoreType(
                IndexModule.NODE_STORE_ALLOW_MMAP.getWithFallback(indexSettings.getNodeSettings()));
        } else {
            type = IndexModule.Type.fromSettingsKey(storeType);
        }
        switch (type) {
            case HYBRIDFS:
                // Use Lucene defaults
                final FSDirectory primaryDirectory = FSDirectory.open(location, lockFactory);
                if (primaryDirectory instanceof MMapDirectory) {
                    MMapDirectory mMapDirectory = (MMapDirectory) primaryDirectory;
                    return new HybridDirectory(lockFactory, mMapDirectory);
                } else {
                    return primaryDirectory;
                }
            case MMAPFS:
                return new MMapDirectory(location, lockFactory);
            case SIMPLEFS:
                return new SimpleFSDirectory(location, lockFactory);
            case NIOFS:
                return new NIOFSDirectory(location, lockFactory);
            default:
                throw new AssertionError("unexpected built-in store type [" + type + "]");
        }
    }

    private static Directory setPreload(Directory directory, Path location, LockFactory lockFactory,
            Set<String> preLoadExtensions) throws IOException {
        if (preLoadExtensions.isEmpty() == false
                && directory instanceof MMapDirectory
                && ((MMapDirectory) directory).getPreload() == false) {
            if (preLoadExtensions.contains("*")) {
                ((MMapDirectory) directory).setPreload(true);
                return directory;
            }
            MMapDirectory primary = new MMapDirectory(location, lockFactory);
            primary.setPreload(true);
            return new FileSwitchDirectory(preLoadExtensions, primary, directory, true) {
                @Override
                public String[] listAll() throws IOException {
                    // avoid listing twice
                    return primary.listAll();
                }
            };
        }
        return directory;
    }

    public static boolean isHybridFs(Directory directory) {
        Directory unwrap = FilterDirectory.unwrap(directory);
        return unwrap instanceof HybridDirectory;
    }

    static final class HybridDirectory extends NIOFSDirectory {

        private final MMapDirectory delegate;


        HybridDirectory(LockFactory lockFactory, MMapDirectory delegate) throws IOException {
            super(delegate.getDirectory(), lockFactory);
            this.delegate = delegate;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (useDelegate(name)) {
                // we need to do these checks on the outer directory since the inner doesn't know about pending deletes
                ensureOpen();
                ensureCanRead(name);
                // we only use the mmap to open inputs. Everything else is managed by the NIOFSDirectory otherwise
                // we might run into trouble with files that are pendingDelete in one directory but still
                // listed in listAll() from the other. We on the other hand don't want to list files from both dirs
                // and intersect for perf reasons.
                return delegate.openInput(name, context);
            } else {
                return super.openInput(name, context);
            }
        }

        boolean useDelegate(String name) {
            String extension = FileSwitchDirectory.getExtension(name);
            switch (extension) {
                // Norms, doc values and term dictionaries are typically performance-sensitive and hot in the page
                // cache, so we use mmap, which provides better performance.
                case "nvd":
                case "dvd":
                case "tim":
                // We want to open the terms index and KD-tree index off-heap to save memory, but this only performs
                // well if using mmap.
                case "tip":
                case "dim":
                // Compound files are tricky because they store all the information for the segment. Benchmarks
                // suggested that not mapping them hurts performance.
                case "cfs":
                // MMapDirectory has special logic to read long[] arrays in little-endian order that helps speed
                // up the decoding of postings. The same logic applies to positions (.pos) of offsets (.pay) but we
                // are not mmaping them as queries that leverage positions are more costly and the decoding of postings
                // tends to be less a bottleneck.
                case "doc":
                    return true;
                // Other files are either less performance-sensitive (e.g. stored field index, norms metadata)
                // or are large and have a random access pattern and mmap leads to page cache trashing
                // (e.g. stored fields and term vectors).
                default:
                    return false;
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(super::close, delegate);
        }

        MMapDirectory getDelegate() {
            return delegate;
        }
    }
}
