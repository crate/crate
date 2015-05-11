/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.blob.BlobEnvironment;
import io.crate.blob.v2.BlobIndices;
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.hamcrest.CoreMatchers.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.TEST, numNodes = 2)
public class CustomBlobPathTest extends CrateIntegrationTest {

    private static File globalBlobPath;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        builder.put(BlobEnvironment.SETTING_BLOBS_PATH, globalBlobPath.getAbsolutePath());
        return builder.build();
    }

    @BeforeClass
    public static void setup() throws Exception {
        globalBlobPath = Files.createTempDirectory(null).toFile();
    }

    @AfterClass
    public static void cleanUpDirectories() throws IOException {
        FileSystemUtils.deleteRecursively(globalBlobPath);
    }

    @Test
    public void testGlobalBlobPath() throws Exception {
        BlobIndices blobIndices = cluster().getInstance(BlobIndices.class, "node_0");
        BlobEnvironment blobEnvironment = cluster().getInstance(BlobEnvironment.class, "node_0");
        BlobEnvironment blobEnvironment2 = cluster().getInstance(BlobEnvironment.class, "node_1");
        assertThat(blobEnvironment.blobsPath().getAbsolutePath(), is(globalBlobPath.getAbsolutePath()));

        Settings indexSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
                .build();
        blobIndices.createBlobTable("test", indexSettings).get();
        ensureGreen();
        assertTrue(blobEnvironment.shardLocation(new ShardId(".blob_test", 0)).exists()
                || blobEnvironment.shardLocation(new ShardId(".blob_test", 1)).exists());
        assertTrue(blobEnvironment2.shardLocation(new ShardId(".blob_test", 0)).exists()
                || blobEnvironment2.shardLocation(new ShardId(".blob_test", 1)).exists());

        blobIndices.dropBlobTable("test").get();

        File loc1 = blobEnvironment.indexLocation(new Index(".blob_test"));
        File loc2 = blobEnvironment2.indexLocation(new Index(".blob_test"));
        assertFalse(loc1.exists());
        assertFalse(loc2.exists());
    }

    @Test
    public void testPerTableBlobPath() throws Exception {
        BlobIndices blobIndices = cluster().getInstance(BlobIndices.class, "node_0");
        BlobEnvironment blobEnvironment = cluster().getInstance(BlobEnvironment.class, "node_0");
        BlobEnvironment blobEnvironment2 = cluster().getInstance(BlobEnvironment.class, "node_1");
        assertThat(blobEnvironment.blobsPath().getAbsolutePath(), is(globalBlobPath.getAbsolutePath()));

        File tempBlobPath = Files.createTempDirectory(null).toFile();
        Settings indexSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
                .put(BlobIndices.SETTING_INDEX_BLOBS_PATH, tempBlobPath.getAbsolutePath())
                .build();
        blobIndices.createBlobTable("test", indexSettings).get();
        ensureGreen();
        assertTrue(blobEnvironment.shardLocation(new ShardId(".blob_test", 0), tempBlobPath).exists()
                || blobEnvironment.shardLocation(new ShardId(".blob_test", 1), tempBlobPath).exists());
        assertTrue(blobEnvironment2.shardLocation(new ShardId(".blob_test", 0), tempBlobPath).exists()
                || blobEnvironment2.shardLocation(new ShardId(".blob_test", 1), tempBlobPath).exists());

        blobIndices.createBlobTable("test2", indexSettings).get();
        ensureGreen();
        assertTrue(blobEnvironment.shardLocation(new ShardId(".blob_test2", 0), tempBlobPath).exists()
                || blobEnvironment.shardLocation(new ShardId(".blob_test2", 1), tempBlobPath).exists());
        assertTrue(blobEnvironment2.shardLocation(new ShardId(".blob_test2", 0), tempBlobPath).exists()
                || blobEnvironment2.shardLocation(new ShardId(".blob_test2", 1), tempBlobPath).exists());

        blobIndices.dropBlobTable("test").get();

        File loc1 = blobEnvironment.indexLocation(new Index(".blob_test"));
        File loc2 = blobEnvironment2.indexLocation(new Index(".blob_test"));
        assertFalse(loc1.exists());
        assertFalse(loc2.exists());

        // blobs path still exists because other index is using it
        assertTrue(tempBlobPath.exists());

        blobIndices.dropBlobTable("test2").get();
        loc1 = blobEnvironment.indexLocation(new Index(".blob_test2"));
        loc2 = blobEnvironment2.indexLocation(new Index(".blob_test2"));
        assertFalse(loc1.exists());
        assertFalse(loc2.exists());
        // no index using the blobs path anymore, should be deleted
        assertFalse(tempBlobPath.exists());

        blobIndices.createBlobTable("test", indexSettings).get();
        ensureGreen();

        File customFile = new File(tempBlobPath, "test_file");
        customFile.createNewFile();

        blobIndices.dropBlobTable("test").get();

        // blobs path still exists because a user defined file exists at the path
        assertTrue(tempBlobPath.exists());
        assertTrue(customFile.exists());

        FileSystemUtils.deleteRecursively(tempBlobPath);
    }

}
