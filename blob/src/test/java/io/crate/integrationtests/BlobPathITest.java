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

package io.crate.integrationtests;

import io.crate.blob.v2.BlobAdminClient;
import io.crate.blob.v2.BlobIndicesService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class BlobPathITest extends BlobIntegrationTestBase {

    private BlobHttpClient client;
    private BlobAdminClient blobAdminClient;
    private Path globalBlobPath;

    private Settings configureGlobalBlobPath() {
        globalBlobPath = createTempDir("globalBlobPath");
        return Settings.builder()
            .put(nodeSettings(0))
            .put("blobs.path", globalBlobPath.toString())
            .build();
    }

    private Settings oneShardAndZeroReplicas() {
        return Settings.builder()
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .build();
    }

    private void launchNodeAndInitClient(Settings settings) throws Exception {
        // using numDataNodes = 1 to launch the node doesn't work:
        // if globalBlobPath is created within nodeSetting it is sometimes not available for the tests
        internalCluster().startNode(settings);
        blobAdminClient = internalCluster().getInstance(BlobAdminClient.class);

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        InetSocketAddress address = ((InetSocketTransportAddress) httpServerTransport
            .boundAddress().publishAddress()).address();
        client = new BlobHttpClient(address);
    }

    @Test
    public void testDataIsStoredInGlobalBlobPath() throws Exception {
        launchNodeAndInitClient(configureGlobalBlobPath());

        Settings indexSettings = oneShardAndZeroReplicas();
        blobAdminClient.createBlobTable("test", indexSettings).get();

        client.put("test", "abcdefg");
        String digest = "2fb5e13419fc89246865e7a324f476ec624e8740";
        try (Stream<Path> files = Files.walk(globalBlobPath)) {
            assertThat(files.anyMatch(i -> digest.equals(i.getFileName().toString())), is(true));
        }
    }
    @Test
    public void testDataIsStoredInTableSpecificBlobPath() throws Exception {
        launchNodeAndInitClient(configureGlobalBlobPath());

        Path tableBlobPath = createTempDir("tableBlobPath");
        Settings indexSettings = Settings.builder()
            .put(oneShardAndZeroReplicas())
            .put(BlobIndicesService.SETTING_INDEX_BLOBS_PATH.getKey(), tableBlobPath.toString())
            .build();
        blobAdminClient.createBlobTable("test", indexSettings).get();

        client.put("test", "abcdefg");
        String digest = "2fb5e13419fc89246865e7a324f476ec624e8740";
        try (Stream<Path> files = Files.walk(tableBlobPath)) {
            assertThat(files.anyMatch(i -> digest.equals(i.getFileName().toString())), is(true));
        }
    }

    @Test
    public void testDataStorageWithMultipleDataPaths() throws Exception {
        Path data1 = createTempDir("data1");
        Path data2 = createTempDir("data2");
        Settings settings = Settings.builder()
            .put(nodeSettings(0))
            .put("path.data", data1.toString() + "," + data2.toString())
            .build();
        launchNodeAndInitClient(settings);
        Settings indexSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .build();
        blobAdminClient.createBlobTable("test", indexSettings).get();

        for (int i = 0; i < 10; i++) {
            client.put("test", "body" + i);
        }
        List<String> data1Files = gatherDigests(data1);
        List<String> data2Files = gatherDigests(data2);

        assertThat(data1Files.size(), Matchers.allOf(lessThan(10), greaterThan(0)));
        assertThat(data2Files.size(), Matchers.allOf(lessThan(10), greaterThan(0)));
        assertThat(data1Files.size() + data2Files.size(), is(10));
    }

    private List<String> gatherDigests(Path data1) throws IOException {
        try (Stream<Path> files = Files.walk(data1)) {
            return files
                .map(Path::getFileName)
                .map(Path::toString)
                .filter(i -> i.length() == 40)
                .collect(Collectors.toList());
        }
    }
}
