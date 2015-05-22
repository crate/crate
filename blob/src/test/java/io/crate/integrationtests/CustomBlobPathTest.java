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
import io.crate.plugin.CrateCorePlugin;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.hamcrest.CoreMatchers.is;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 2, scope = ElasticsearchIntegrationTest.Scope.TEST)
public class CustomBlobPathTest extends ElasticsearchIntegrationTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static File globalBlobPath;
    private String node1;
    private String node2;

    @BeforeClass
    public static void pathSetup() throws Exception {
        globalBlobPath = temporaryFolder.newFolder();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", CrateCorePlugin.class.getName())
                .put(BlobEnvironment.SETTING_BLOBS_PATH, globalBlobPath.getAbsolutePath())
                .put(InternalNode.HTTP_ENABLED, true)
                .build();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        String[] nodeNames = internalCluster().getNodeNames();
        node1 = nodeNames[0];
        node2 = nodeNames[1];
    }

    @Test
    public void testGlobalBlobPath() throws Exception {
        BlobIndices blobIndices = internalCluster().getInstance(BlobIndices.class, node1);
        BlobEnvironment blobEnvironment = internalCluster().getInstance(BlobEnvironment.class, node1);
        BlobEnvironment blobEnvironment2 = internalCluster().getInstance(BlobEnvironment.class, node2);
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
        BlobIndices blobIndices = internalCluster().getInstance(BlobIndices.class, node1);
        BlobEnvironment blobEnvironment = internalCluster().getInstance(BlobEnvironment.class, node1);
        BlobEnvironment blobEnvironment2 = internalCluster().getInstance(BlobEnvironment.class, node2);
        assertThat(blobEnvironment.blobsPath().getAbsolutePath(), is(globalBlobPath.getAbsolutePath()));

        File tempBlobPath = temporaryFolder.newFolder();
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

        assertThat(tempBlobPath.exists(), is(true));
        assertThat(tempBlobPath.listFiles().length, is(0));

        blobIndices.createBlobTable("test", indexSettings).get();
        ensureGreen();

        File customFile = new File(tempBlobPath, "test_file");
        customFile.createNewFile();

        blobIndices.dropBlobTable("test").get();

        // blobs path still exists because a user defined file exists at the path
        assertTrue(tempBlobPath.exists());
        assertTrue(customFile.exists());
    }
}
