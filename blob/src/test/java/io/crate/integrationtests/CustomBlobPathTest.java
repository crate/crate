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
import io.crate.blob.v2.BlobIndicesService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, scope = ESIntegTestCase.Scope.TEST)
public class CustomBlobPathTest extends BlobIntegrationTestBase {

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
        return Settings.settingsBuilder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(BlobEnvironment.SETTING_BLOBS_PATH, globalBlobPath.getAbsolutePath())
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
        BlobIndicesService blobIndicesService = internalCluster().getInstance(BlobIndicesService.class, node1);
        BlobEnvironment blobEnvironment = internalCluster().getInstance(BlobEnvironment.class, node1);
        BlobEnvironment blobEnvironment2 = internalCluster().getInstance(BlobEnvironment.class, node2);
        assertThat(blobEnvironment.blobsPath().toString(), is(globalBlobPath.getAbsolutePath()));

        Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
            .build();
        blobIndicesService.createBlobTable("test", indexSettings).get();
        ensureGreen();
        assertTrue(Files.exists(blobEnvironment.shardLocation(new ShardId(".blob_test", 0)))
                   || Files.exists(blobEnvironment.shardLocation(new ShardId(".blob_test", 1))));
        assertTrue(Files.exists(blobEnvironment2.shardLocation(new ShardId(".blob_test", 0)))
                   || Files.exists(blobEnvironment2.shardLocation(new ShardId(".blob_test", 1))));

        blobIndicesService.dropBlobTable("test").get();

        Path loc1 = blobEnvironment.indexLocation(new Index(".blob_test"));
        Path loc2 = blobEnvironment2.indexLocation(new Index(".blob_test"));
        assertFalse(Files.exists(loc1));
        assertFalse(Files.exists(loc2));
    }

    @Test
    public void testPerTableBlobPath() throws Exception {
        BlobIndicesService blobIndicesService = internalCluster().getInstance(BlobIndicesService.class, node1);
        BlobEnvironment blobEnvironment = internalCluster().getInstance(BlobEnvironment.class, node1);
        BlobEnvironment blobEnvironment2 = internalCluster().getInstance(BlobEnvironment.class, node2);
        assertThat(blobEnvironment.blobsPath().toString(), is(globalBlobPath.getAbsolutePath()));

        Path tempBlobPath = PathUtils.get(temporaryFolder.newFolder().toURI());
        Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
            .put(BlobIndicesService.SETTING_INDEX_BLOBS_PATH, tempBlobPath.toString())
            .build();
        blobIndicesService.createBlobTable("test", indexSettings).get();
        ensureGreen();
        assertTrue(Files.exists(blobEnvironment.shardLocation(new ShardId(".blob_test", 0), tempBlobPath))
                   || Files.exists(blobEnvironment.shardLocation(new ShardId(".blob_test", 1), tempBlobPath)));
        assertTrue(Files.exists(blobEnvironment2.shardLocation(new ShardId(".blob_test", 0), tempBlobPath))
                   || Files.exists(blobEnvironment2.shardLocation(new ShardId(".blob_test", 1), tempBlobPath)));

        blobIndicesService.createBlobTable("test2", indexSettings).get();
        ensureGreen();
        assertTrue(Files.exists(blobEnvironment.shardLocation(new ShardId(".blob_test2", 0), tempBlobPath))
                   || Files.exists(blobEnvironment.shardLocation(new ShardId(".blob_test2", 1), tempBlobPath)));
        assertTrue(Files.exists(blobEnvironment2.shardLocation(new ShardId(".blob_test2", 0), tempBlobPath))
                   || Files.exists(blobEnvironment2.shardLocation(new ShardId(".blob_test2", 1), tempBlobPath)));

        blobIndicesService.dropBlobTable("test").get();

        Path loc1 = blobEnvironment.indexLocation(new Index(".blob_test"));
        Path loc2 = blobEnvironment2.indexLocation(new Index(".blob_test"));
        assertFalse(Files.exists(loc1));
        assertFalse(Files.exists(loc2));

        // blobs path still exists because other index is using it
        assertTrue(Files.exists(tempBlobPath));

        blobIndicesService.dropBlobTable("test2").get();
        loc1 = blobEnvironment.indexLocation(new Index(".blob_test2"));
        loc2 = blobEnvironment2.indexLocation(new Index(".blob_test2"));
        assertFalse(Files.exists(loc1));
        assertFalse(Files.exists(loc2));

        assertThat(Files.exists(tempBlobPath), is(true));
        try (Stream<Path> list = Files.list(tempBlobPath)) {
            assertThat(list.count(), is(0L));
        }

        blobIndicesService.createBlobTable("test", indexSettings).get();
        ensureGreen();

        Path customFile = Files.createTempFile(tempBlobPath, "", "test_file");

        blobIndicesService.dropBlobTable("test").get();

        // blobs path still exists because a user defined file exists at the path
        assertTrue(Files.exists(tempBlobPath));
        assertTrue(Files.exists(customFile));
    }
}
