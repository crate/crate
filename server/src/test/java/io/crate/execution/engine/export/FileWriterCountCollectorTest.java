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

package io.crate.execution.engine.export;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerator;

import io.crate.data.Row;
import io.crate.exceptions.UnhandledServerException;
import io.crate.execution.dsl.projection.WriterProjection;

public class FileWriterCountCollectorTest extends ESTestCase {

    @Test
    public void testJsonBuilderDoesNotPassFlushToStream() throws Exception {
        Path file = createTempFile("out", "json");
        try (OutputStream os = new FileOutputStream(file.toFile())) {
            XContentBuilder xContentBuilder = FileWriterCountCollector.createJsonBuilder(os);
            assertThat(xContentBuilder.generator().isEnabled(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM)).isFalse();
        }
    }

    @Test
    public void test_query_or_fragment_in_directory_uri_or_file_name_does_not_truncate_existing_file() throws Exception {
        Path dir = createTempDir();
        Path file = Files.writeString(dir.resolve("tbl"), "content");
        String dirUri = dir.toUri().toString();

        // '?' or '#' in the directory URI, the file name is still appended to the path, which isn't a directory
        for (String uri : List.of(dirUri + "tbl?", dirUri + "tbl#")) {
            assertThatThrownBy(() -> newCollector(FileWriterProjector.resolveOutputUri(uri, "tbl_0_.json")))
                .isExactlyInstanceOf(UnhandledServerException.class)
                .hasMessageStartingWith("Failed to open output")
                .hasMessageEndingWith("/tbl/tbl_0_.json (Not a directory)'");
        }

        // '?' or '#' in the file name (e.g. table name), they remain part of the file name.
        for (String fileName : List.of("tbl?_0_.json", "tbl#_0_.json")) {
            FileWriterCountCollector collector = newCollector(FileWriterProjector.resolveOutputUri(dirUri, fileName));
            Row result = collector.finisher().apply(collector.supplier().get()).iterator().next();
            assertThat(result.get(0)).isEqualTo(0L);
            assertThat(new File(dir.toString(), fileName)).exists();
        }
        assertThat(Files.readString(file)).isEqualTo("content");
    }

    private static FileWriterCountCollector newCollector(URI uri) {
        return new FileWriterCountCollector(
            mock(Executor.class),
            uri,
            null,
            List.of(),
            List.of(),
            null,
            WriterProjection.OutputFormat.JSON_OBJECT,
            Map.of(LocalFsFileOutputFactory.NAME, new LocalFsFileOutputFactory()),
            Settings.EMPTY);
    }
}
