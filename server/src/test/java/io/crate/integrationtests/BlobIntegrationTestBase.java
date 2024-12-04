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
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;

import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;

public abstract class BlobIntegrationTestBase extends IntegTestCase {

    private Field indicesField;
    private Field shardsField;

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Before
    public void initFields() throws Exception {
        indicesField = BlobIndicesService.class.getDeclaredField("indices");
        indicesField.setAccessible(true);
        shardsField = BlobIndex.class.getDeclaredField("shards");
        shardsField.setAccessible(true);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
            .put(SETTING_HTTP_COMPRESSION.getKey(), false)
            .build();
    }

    @After
    public void assertNoTmpFilesAndNoIndicesRemaining() throws Exception {
        assertBusy(() -> forEachIndicesMap(i -> {
            for (BlobIndex blobIndex : i.values()) {
                try {
                    Map<Integer, BlobShard> o = (Map<Integer, BlobShard>) shardsField.get(blobIndex);
                    for (BlobShard blobShard : o.values()) {
                        Path tmpDir = blobShard.blobContainer().getTmpDirectory();
                        try (Stream<Path> files = Files.list(tmpDir)) {
                            assertThat(files.count()).isEqualTo(0L);
                        } catch (NoSuchFileException ignored) {
                        }
                    }
                } catch (IOException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }));

        cluster().wipeAllTables();
        assertBusy(() -> forEachIndicesMap(i -> assertThat(i.keySet()).isEmpty()));
    }

    private void forEachIndicesMap(Consumer<Map<String, BlobIndex>> consumer) {
        Iterable<BlobIndicesService> blobIndicesServices = cluster().getInstances(BlobIndicesService.class);
        for (BlobIndicesService blobIndicesService : blobIndicesServices) {
            try {
                Map<String, BlobIndex> indices = (Map<String, BlobIndex>) indicesField.get(blobIndicesService);
                consumer.accept(indices);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
