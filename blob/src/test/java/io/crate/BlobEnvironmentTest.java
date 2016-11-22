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
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import static org.hamcrest.Matchers.is;

public class BlobEnvironmentTest extends CrateUnitTest {

    private BlobEnvironment blobEnvironment;
    private NodeEnvironment nodeEnvironment;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        Path home = folder.newFolder("home").toPath();
        Path dataPath = folder.newFolder("data").toPath();
        Settings settings = Settings.builder()
            .put("path.home", home.toAbsolutePath())
            .put("path.data", dataPath.toAbsolutePath()).build();
        Environment environment = new Environment(settings);
        nodeEnvironment = new NodeEnvironment(settings, environment);
        blobEnvironment = new BlobEnvironment(settings, nodeEnvironment);
    }

    @After
    public void cleanup() throws Exception {
        nodeEnvironment.close();
        nodeEnvironment = null;
    }

    @Test
    public void testShardLocation() throws Exception {
        Path blobsPath = PathUtils.get("/tmp/crate_blobs");
        Path shardLocation = blobEnvironment.shardLocation(new ShardId(".blob_test", 0), blobsPath);
        assertThat(shardLocation.toString().substring(0, blobsPath.toString().length()),
            is(blobsPath.toString()));
    }

    @Test
    public void testShardLocationWithoutCustomPath() throws Exception {
        Path shardLocation = blobEnvironment.shardLocation(new ShardId(".blob_test", 0));
        Path nodeShardPaths = nodeEnvironment.availableShardPaths(new ShardId(".blob_test", 0))[0];
        assertThat(shardLocation.toString().substring(nodeShardPaths.toAbsolutePath().toString().length()),
            is(File.separator + BlobEnvironment.BLOBS_SUB_PATH));
    }

    @Test
    public void testIndexLocation() throws Exception {
        Path blobsPath = PathUtils.get("/tmp/crate_blobs");
        Path indexLocation = blobEnvironment.indexLocation(new Index(".blob_test"), blobsPath);
        assertThat(indexLocation.toString().substring(0, blobsPath.toString().length()),
            is(blobsPath.toString()));
    }

    @Test
    public void testValidateIsFile() throws Exception {
        Path testFile = folder.newFile("test_blob_file.txt").toPath();

        expectedException.expect(SettingsException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "blobs path '%s' is a file, must be a directory",
            testFile.toString()));
        blobEnvironment.ensureExistsAndWritable(testFile);
    }

    @Test
    public void testValidateNotCreatable() throws Exception {
        Assume.assumeFalse(org.apache.lucene.util.Constants.WINDOWS);

        File tmpDir = folder.newFolder();
        assertThat(tmpDir.setReadable(false), is(true));
        assertThat(tmpDir.setWritable(false), is(true));
        assertThat(tmpDir.setExecutable(false), is(true));
        File file = new File(tmpDir, "crate_blobs");

        expectedException.expect(SettingsException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "blobs path '%s' could not be created",
            file.getAbsolutePath()));
        blobEnvironment.ensureExistsAndWritable(file.toPath());
    }
}
