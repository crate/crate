/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.migration;

import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.is;

public class IndexMetaDataUtilTest extends CrateUnitTest {

    @Test
    public void testNoUpgradeRequired() throws IOException {
        Path zippedIndexDir = getDataPath("/indices/index_noupgrade_required-crate1.0.3-es2.4.2.zip");
        Path dataDir = prepareBackwardsDataDir(zippedIndexDir);
        IndexMetaData indexMetaData = IndexMetaDataUtil.loadIndexESMetadata(dataDir);
        assertThat(IndexMetaDataUtil.checkIndexIsUpgraded(indexMetaData), is(true));
        assertThat(IndexMetaDataUtil.checkReindexIsRequired(indexMetaData), is(false));
        for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
            Directory shardDir = FSDirectory.open(dataDir.resolve(String.valueOf(i)).resolve("index"));
            assertThat(IndexMetaDataUtil.checkAlreadyMigrated(shardDir), is(true));
            assertThat(IndexMetaDataUtil.checkValidShard(shardDir), is(true));
        }
    }

    @Test
    public void testReindexRequired() throws IOException {
        Path zippedIndexDir = getDataPath("/indices/index_reindex_required-crate0.45.8-es1.3.5.zip");
        Path dataDir = prepareBackwardsDataDir(zippedIndexDir);
        IndexMetaData indexMetaData = IndexMetaDataUtil.loadIndexESMetadata(dataDir);
        assertThat(IndexMetaDataUtil.checkReindexIsRequired(indexMetaData), is(true));
        assertThat(IndexMetaDataUtil.checkIndexIsUpgraded(indexMetaData), is(false));
        for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
            Directory shardDir = FSDirectory.open(dataDir.resolve(String.valueOf(i)).resolve("index"));
            assertThat(IndexMetaDataUtil.checkValidShard(shardDir), is(true));
            assertThat(IndexMetaDataUtil.checkAlreadyMigrated(shardDir), is(false));
        }
    }

    @Test
    public void testIndexAlreadyMigrated() throws IOException {
        Path zippedIndexDir = getDataPath("/indices/index_already_migrated-crate0.53.0-es1.7.3.zip");
        Path dataDir = prepareBackwardsDataDir(zippedIndexDir);
        IndexMetaData indexMetaData = IndexMetaDataUtil.loadIndexESMetadata(dataDir);
        assertThat(IndexMetaDataUtil.checkReindexIsRequired(indexMetaData), is(false));
        assertThat(IndexMetaDataUtil.checkIndexIsUpgraded(indexMetaData), is(false));
        for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
            Directory shardDir = FSDirectory.open(dataDir.resolve(String.valueOf(i)).resolve("index"));
            assertThat(IndexMetaDataUtil.checkValidShard(shardDir), is(true));
            assertThat(IndexMetaDataUtil.checkAlreadyMigrated(shardDir), is(true));
        }
    }

    @Test
    public void testInvalidShard() throws IOException {
        Path indexDir = createTempDir();
        Directory shardDir = FSDirectory.open(indexDir);
        assertThat(IndexMetaDataUtil.checkValidShard(shardDir), is(false));
    }

    private Path prepareBackwardsDataDir(Path backwardsIndex) throws IOException {
        Path indexDir = createTempDir();
        Path dataDir = indexDir.resolve("test");
        try (InputStream stream = Files.newInputStream(backwardsIndex)) {
            TestUtil.unzip(stream, indexDir);
        }
        assertTrue(Files.exists(dataDir));
        return dataDir;
    }
}
