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
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;

public class IndexStoreTests extends ESTestCase {

    @Test
    public void testStoreDirectory() throws IOException {
        Index index = new Index("foo", "fooUUID");
        final Path tempDir = createTempDir().resolve(index.getUUID()).resolve("0");
        // default
        doTestStoreDirectory(index, tempDir, null, IndexModule.Type.FS);
        // explicit directory impls
        for (IndexModule.Type type : IndexModule.Type.values()) {
            doTestStoreDirectory(index, tempDir, type.name().toLowerCase(Locale.ROOT), type);
        }
    }

    private void doTestStoreDirectory(Index index,
                                      Path tempDir,
                                      String typeSettingValue,
                                      IndexModule.Type type) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT);
        if (typeSettingValue != null) {
            settingsBuilder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), typeSettingValue);
        }
        Settings settings = settingsBuilder.build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        FsDirectoryService service = new FsDirectoryService(
            indexSettings, new ShardPath(false, tempDir, tempDir, new ShardId(index, 0)));
        try (Directory directory = service.newFSDirectory(tempDir, NoLockFactory.INSTANCE)) {
            switch (type) {
                case HYBRIDFS:
                    assertHybridDirectory(directory);
                    break;
                case NIOFS:
                    assertTrue(type + " " + directory.toString(), directory instanceof NIOFSDirectory);
                    break;
                case MMAPFS:
                    assertTrue(type + " " + directory.toString(), directory instanceof MMapDirectory);
                    break;
                case SIMPLEFS:
                    assertTrue(type + " " + directory.toString(), directory instanceof SimpleFSDirectory);
                    break;
                case FS:
                    if (Constants.JRE_IS_64BIT && MMapDirectory.UNMAP_SUPPORTED) {
                        assertHybridDirectory(directory);
                    } else if (Constants.WINDOWS) {
                        assertTrue(directory.toString(), directory instanceof SimpleFSDirectory);
                    } else {
                        assertTrue(directory.toString(), directory instanceof NIOFSDirectory);
                    }
                    break;
                default:
                    fail();
            }
        }
    }

    private void assertHybridDirectory(Directory directory) {
        assertTrue(directory.toString(), directory instanceof FsDirectoryService.HybridDirectory);
        Directory randomAccessDirectory = ((FsDirectoryService.HybridDirectory) directory).getDelegate();
        assertTrue("randomAccessDirectory:  " +  randomAccessDirectory.toString(), randomAccessDirectory instanceof MMapDirectory);
    }
}
