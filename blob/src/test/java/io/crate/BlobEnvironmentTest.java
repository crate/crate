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

package io.crate;

import io.crate.blob.BlobEnvironment;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.os.OsUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.is;

public class BlobEnvironmentTest extends CrateUnitTest {

    private static Path dataPath;
    private static File testFile;
    private BlobEnvironment blobEnvironment;
    private NodeEnvironment nodeEnvironment;

    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        dataPath = Files.createTempDirectory(null);
        Settings settings = ImmutableSettings.builder()
                .put("path.data", dataPath.toAbsolutePath()).build();
        Environment environment = new Environment(settings);
        nodeEnvironment = new NodeEnvironment(settings, environment);
        blobEnvironment = new BlobEnvironment(nodeEnvironment, new ClusterName("test"));
    }

    @After
    public void cleanup() throws Exception {
        if (testFile != null) {
            testFile.delete();
        }
        if (dataPath != null) {
            FileSystemUtils.deleteRecursively(dataPath.toAbsolutePath().toFile());
        }
    }

    @Test
    public void testShardLocation() throws Exception {
        File blobsPath = new File("/tmp/crate_blobs");
        File shardLocation = blobEnvironment.shardLocation(new ShardId(".blob_test", 0), blobsPath);
        assertThat(shardLocation.getAbsolutePath().substring(0, blobsPath.getAbsolutePath().length()),
                is(blobsPath.getAbsolutePath()));
    }

    @Test
    public void testShardLocationWithoutCustomPath() throws Exception {
        File shardLocation = blobEnvironment.shardLocation(new ShardId(".blob_test", 0));
        File nodeShardLocation = nodeEnvironment.shardLocations(new ShardId(".blob_test", 0))[0];
        assertThat(shardLocation.getAbsolutePath().substring(nodeShardLocation.getAbsolutePath().length()),
                is(File.separator + BlobEnvironment.BLOBS_SUB_PATH));
    }

    @Test
    public void testIndexLocation() throws Exception {
        File blobsPath = new File("/tmp/crate_blobs");
        File indexLocation = blobEnvironment.indexLocation(new Index(".blob_test"), blobsPath);
        assertThat(indexLocation.getAbsolutePath().substring(0, blobsPath.getAbsolutePath().length()),
                is(blobsPath.getAbsolutePath()));
    }

    @Test
    public void testValidateIsFile() throws Exception {
        testFile = File.createTempFile("test_blob_file", ".txt");

        expectedException.expect(SettingsException.class);
        expectedException.expectMessage(String.format("blobs path '%s' is a file, must be a directory",
                testFile.getAbsolutePath()));
        blobEnvironment.validateBlobsPath(testFile);
    }

    @Test
    public void testValidateNotCreatable() throws Exception {
        Assume.assumeFalse(OsUtils.WINDOWS);

        File tmpDir = folder.newFolder();
        assertThat(tmpDir.setReadable(false), is(true));
        assertThat(tmpDir.setWritable(false), is(true));
        assertThat(tmpDir.setExecutable(false), is(true));
        File file = new File(tmpDir, "crate_blobs");

        expectedException.expect(SettingsException.class);
        expectedException.expectMessage(String.format("blobs path '%s' could not be created",
                file.getAbsolutePath()));
        blobEnvironment.validateBlobsPath(file);
    }

}
