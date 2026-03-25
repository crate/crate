/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.opendal;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.opendal.AsyncExecutor;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OpenDALBlobContainerTest extends ESTestCase {

    private AsyncExecutor executor;

    @Before
    public void setup() {
        executor = AsyncExecutor.createTokioExecutor(1);
    }

    @After
    public void teardown() {
        executor.close();
    }

    @Test
    public void test_listBlobsByPrefix_excludes_dirs() throws Exception {
        Path tempDir = createTempDir("test");
        var config = new ServiceConfig() {
            @Override
            public String scheme() {
                return "fs";
            }

            @Override
            public Map<String, String> configMap() {
                return Map.of("root", tempDir.toString());
            }
        };
        try (Operator operator = AsyncOperator.of(config, executor).blocking()) {
            BlobPath blobPath = new BlobPath();
            OpenDALBlobContainer container = new OpenDALBlobContainer(blobPath, operator, 1024);
            String blobPrefix = "prefix";

            operator.createDir(blobPrefix + "/dir/");
            try (var out = operator.createOutputStream(blobPrefix + "file1.txt", 1024)) {
                out.write("content".getBytes());
            }

            List<String> blobs = container.listBlobsByPrefix(blobPrefix);
            // Without dir filtering would be ["prefix/", "prefixfile1.txt"]
            assertThat(blobs).containsExactly("prefixfile1.txt");
        }
    }

}
