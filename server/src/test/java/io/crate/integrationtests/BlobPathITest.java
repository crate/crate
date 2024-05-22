/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;


@IntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
@WindowsIncompatible
public class BlobPathITest extends BlobIntegrationTestBase {

    private BlobHttpClient client;
    private Path globalBlobPath;

    private Settings configureGlobalBlobPath() {
        globalBlobPath = createTempDir("globalBlobPath");
        return Settings.builder()
            .put(nodeSettings(0))
            .put("blobs.path", globalBlobPath.toString())
            .build();
    }

    private void launchNodeAndInitClient(Settings settings) throws Exception {
        // using numDataNodes = 1 to launch the node doesn't work:
        // if globalBlobPath is created within nodeSetting it is sometimes not available for the tests
        cluster().startNode(settings);

        HttpServerTransport httpServerTransport = cluster().getInstance(HttpServerTransport.class);
        InetSocketAddress address = httpServerTransport.boundAddress().publishAddress().address();
        client = new BlobHttpClient(address);
    }

    @Test
    public void testDataIsNotDeletedOnNodeShutdown() throws Exception {
        Path data1 = createTempDir("data1");
        launchNodeAndInitClient(Settings.builder()
            .put(nodeSettings(0))
            .put("path.data", data1.toString())
            .build()
        );
        execute("create blob table b1 clustered into 1 shards with (number_of_replicas = 0)");

        client.put("b1", "abcdefg");
        assertThat(gatherDigests(data1)).hasSize(1);
        cluster().stopRandomDataNode();
        assertThat(gatherDigests(data1)).hasSize(1);
    }

    @Test
    public void testDataIsStoredInGlobalBlobPath() throws Exception {
        launchNodeAndInitClient(configureGlobalBlobPath());

        execute("create blob table test clustered into 1 shards with (number_of_replicas = 0)");

        client.put("test", "abcdefg");
        String digest = "2fb5e13419fc89246865e7a324f476ec624e8740";
        try (Stream<Path> files = Files.walk(globalBlobPath)) {
            assertThat(files.anyMatch(i -> digest.equals(i.getFileName().toString()))).isTrue();
        }
    }

    @Test
    public void testDataIsStoredInTableSpecificBlobPath() throws Exception {
        launchNodeAndInitClient(configureGlobalBlobPath());

        Path tableBlobPath = createTempDir("tableBlobPath");
        execute(
            "create blob table test clustered into 1 shards with (number_of_replicas = 0, blobs_path = ?)",
            new Object[] { tableBlobPath.toString() }
        );

        client.put("test", "abcdefg");
        String digest = "2fb5e13419fc89246865e7a324f476ec624e8740";
        try (Stream<Path> files = Files.walk(tableBlobPath)) {
            assertThat(files.anyMatch(i -> digest.equals(i.getFileName().toString()))).isTrue();
        }
    }

    @Test
    public void testDataIsDeletedSpecificIndexBlobPath() throws Exception {
        launchNodeAndInitClient(configureGlobalBlobPath());

        Path tableBlobPath = createTempDir("tableBlobPath");
        execute(
            "create blob table test clustered into 1 shards with (number_of_replicas = 0, blobs_path = ?)",
            new Object[] { tableBlobPath.toString() }
        );
        client.put("test", "abcdefg");
        execute("drop blob table test");
        String blobRootPath = String.format("%s/nodes/0/indices/.blob_test", tableBlobPath.toString());

        assertBusy(() -> assertThat(Files.exists(Paths.get(blobRootPath))).isFalse(), 5, TimeUnit.SECONDS);
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
        execute("create blob table test clustered into 2 shards with (number_of_replicas = 0)");

        for (int i = 0; i < 10; i++) {
            client.put("test", "body" + i);
        }
        List<String> data1Files = gatherDigests(data1);
        List<String> data2Files = gatherDigests(data2);

        assertThat(data1Files.size()).isGreaterThan(0).isLessThan(10);
        assertThat(data2Files.size()).isGreaterThan(0).isLessThan(10);
        assertThat(data1Files.size() + data2Files.size()).isEqualTo(10);
    }

    private List<String> gatherDigests(Path data1) throws IOException {
        try (Stream<Path> files = Files.walk(data1)) {
            return files
                .map(Path::getFileName)
                .map(Path::toString)
                .filter(i -> i.length() == 40)
                .toList();
        }
    }
}
