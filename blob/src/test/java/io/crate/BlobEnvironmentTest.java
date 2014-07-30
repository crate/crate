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
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class BlobEnvironmentTest {

    private static Path dataPath;
    private static File testFile;
    private BlobEnvironment blobEnvironment;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        dataPath = Files.createTempDirectory(null);
        Settings settings = ImmutableSettings.builder()
                .put("path.data", dataPath.toAbsolutePath()).build();
        Environment environment = new Environment(settings);
        NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, environment);
        blobEnvironment = new BlobEnvironment(settings, nodeEnvironment);
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
        File shardLocation = blobEnvironment.shardLocations(new ShardId(".blob_test", 0), blobsPath);
        assertThat(shardLocation.getAbsolutePath().substring(0, blobsPath.getAbsolutePath().length()),
                is(blobsPath.getAbsolutePath()));
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
        File file = new File("/crate_blobs");

        expectedException.expect(SettingsException.class);
        expectedException.expectMessage(String.format("blobs path '%s' could not be created",
                file.getAbsolutePath()));
        blobEnvironment.validateBlobsPath(file);
    }

}
