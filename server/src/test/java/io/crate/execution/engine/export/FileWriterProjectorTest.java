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

import static io.crate.data.SentinelRow.SENTINEL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.testing.RowGenerator;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.exceptions.UnhandledServerException;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.testing.TestingHelpers;

public class FileWriterProjectorTest extends ESTestCase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final Supplier<BatchIterator<Row>> sourceSupplier = () -> InMemoryBatchIterator.of(RowGenerator.fromSingleColValues(
        IntStream.range(0, 5)
            .mapToObj(i -> String.format(Locale.ENGLISH, "input line %02d", i))
            .collect(Collectors.toList())), SENTINEL, true);

    @Test
    public void testWriteRawToFile() throws Exception {
        Path file = createTempFile("out", "json");

        FileWriterProjector fileWriterProjector = new FileWriterProjector(executorService, file.toUri().toString(),
            null, null, Set.of(),
            null, WriterProjection.OutputFormat.JSON_OBJECT,
            Map.of(LocalFsFileOutputFactory.NAME, new LocalFsFileOutputFactory()), Settings.EMPTY);

        new TestingRowConsumer().accept(fileWriterProjector.apply(sourceSupplier.get()), null);

        assertThat(TestingHelpers.readFile(file.toAbsolutePath().toString())).isEqualTo("""
            input line 00
            input line 01
            input line 02
            input line 03
            input line 04""");
    }

    @Test
    public void testDirectoryAsFile() throws Exception {
        Path directory = createTempDir();

        FileWriterProjector fileWriterProjector = new FileWriterProjector(
                executorService, directory.toUri().toString(),
                null, null, Set.of(),
                null, WriterProjection.OutputFormat.JSON_OBJECT,
                Map.of(LocalFsFileOutputFactory.NAME, new LocalFsFileOutputFactory()), Settings.EMPTY);
        assertThatThrownBy(() -> new TestingRowConsumer().accept(fileWriterProjector.apply(sourceSupplier.get()), null))
            .isExactlyInstanceOf(UnhandledServerException.class)
            .hasMessageStartingWith("Failed to open output: 'Output path is a directory: ");
    }

    @Test
    public void testFileAsDirectory() throws Exception {
        String uri = Paths.get(folder.newFile().toURI()).resolve("out.json").toUri().toString();

        FileWriterProjector fileWriterProjector = new FileWriterProjector(executorService, uri,
                null, null, Set.of(),
                null, WriterProjection.OutputFormat.JSON_OBJECT,
                Map.of(LocalFsFileOutputFactory.NAME, new LocalFsFileOutputFactory()), Settings.EMPTY);

        assertThatThrownBy(() -> new TestingRowConsumer().accept(fileWriterProjector.apply(sourceSupplier.get()), null))
            .isExactlyInstanceOf(UnhandledServerException.class)
            .hasMessageStartingWith("Failed to open output");
    }
}
