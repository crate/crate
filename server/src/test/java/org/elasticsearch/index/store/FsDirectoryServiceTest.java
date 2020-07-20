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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import org.apache.lucene.store.Directory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Test;

public class FsDirectoryServiceTest extends ESTestCase {

    @Test
    public void testPreload() throws IOException {
        Settings build = Settings.builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.HYBRIDFS.name().toLowerCase(Locale.ROOT))
            .putList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey(), "dvd", "bar")
            .build();
        try (Directory directory = newDirectory(build)) {
            assertTrue(FsDirectoryService.isHybridFs(directory));
            try (FsDirectoryService.HybridDirectory hybridDirectory = (FsDirectoryService.HybridDirectory) directory) {
                assertTrue(hybridDirectory.useDelegate("foo.dvd"));
                assertTrue(hybridDirectory.useDelegate("foo.nvd"));
                assertTrue(hybridDirectory.useDelegate("foo.tim"));
                assertTrue(hybridDirectory.useDelegate("foo.tip"));
                assertTrue(hybridDirectory.useDelegate("foo.cfs"));
                assertTrue(hybridDirectory.useDelegate("foo.dim"));
                assertTrue(hybridDirectory.useDelegate("foo.kdd"));
                assertTrue(hybridDirectory.useDelegate("foo.kdi"));
                assertFalse(hybridDirectory.useDelegate("foo.bar"));
            }
        }
    }

     private Directory newDirectory(Settings settings) throws IOException {
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        Path tempDir = createTempDir().resolve(idxSettings.getUUID()).resolve("0");
        Files.createDirectories(tempDir);
        ShardPath path = new ShardPath(false, tempDir, tempDir, new ShardId(idxSettings.getIndex(), 0));
        return new FsDirectoryService(idxSettings, path).newDirectory();
    }

}
