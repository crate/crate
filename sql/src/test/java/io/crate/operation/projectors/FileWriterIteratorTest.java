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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableSet;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.projection.WriterProjection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.CollectingBatchConsumer;
import io.crate.testing.SingleColumnBatchIterator;
import io.crate.testing.TestingHelpers;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;

public class FileWriterIteratorTest extends CrateUnitTest {

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    Supplier<BatchIterator> sourceSupplier = () -> SingleColumnBatchIterator.fromIterable(
        () -> IntStream.range(0, 5)
            .mapToObj(i -> new BytesRef(String.format(Locale.ENGLISH, "input line %02d", i)))
            .iterator()
    );

    @Test
    public void testFileWriterIterator() throws Exception {
        Path file = createTempFile("out", "json");

        BatchIteratorTester tester = new BatchIteratorTester(() ->
            FileWriterIterator.newInstance(sourceSupplier.get(),
                executorService, file.toUri().toString(), null, null, ImmutableSet.<CollectExpression<Row, ?>>of(),
                new HashMap<>(), null, WriterProjection.OutputFormat.JSON_OBJECT),
            Collections.singletonList(new Object[]{5L}));
        tester.run();
    }

    @Test
    public void testToNestedStringObjectMap() throws Exception {
        Map<ColumnIdent, Object> columnIdentMap = new HashMap<>();
        columnIdentMap.put(new ColumnIdent("some", Arrays.asList("nested", "column")), "foo");
        Map<String, Object> convertedMap = FileWriterIterator.toNestedStringObjectMap(columnIdentMap);

        Map someMap = (Map) convertedMap.get("some");
        Map nestedMap = (Map) someMap.get("nested");
        assertThat(nestedMap.get("column"), is("foo"));
    }

    @Test
    public void testWriteRawToFile() throws Exception {
        Path file = createTempFile("out", "json");

        BatchIterator batchIterator = FileWriterIterator.newInstance(sourceSupplier.get(),
            executorService, file.toUri().toString(), null, null, ImmutableSet.<CollectExpression<Row, ?>>of(),
            new HashMap<>(), null, WriterProjection.OutputFormat.JSON_OBJECT);

        new CollectingBatchConsumer().accept(batchIterator, null);

        assertEquals("input line 00\n" +
                     "input line 01\n" +
                     "input line 02\n" +
                     "input line 03\n" +
                     "input line 04\n", TestingHelpers.readFile(file.toAbsolutePath().toString()));
    }

    @Test
    public void testDirectoryAsFile() throws Exception {
        expectedException.expect(UnhandledServerException.class);
        expectedException.expectMessage("Failed to open output: 'Output path is a directory: ");

        Path directory = createTempDir();

        BatchIterator batchIterator = FileWriterIterator.newInstance(sourceSupplier.get(),
            executorService, directory.toUri().toString(), null, null, ImmutableSet.<CollectExpression<Row, ?>>of(),
            new HashMap<>(), null, WriterProjection.OutputFormat.JSON_OBJECT);

        new CollectingBatchConsumer().accept(batchIterator, null);
    }

    @Test
    public void testFileAsDirectory() throws Exception {
        expectedException.expect(UnhandledServerException.class);
        expectedException.expectMessage("Failed to open output");

        String uri = Paths.get(folder.newFile().toURI()).resolve("out.json").toUri().toString();

        BatchIterator batchIterator = FileWriterIterator.newInstance(sourceSupplier.get(),
            executorService, uri, null, null, ImmutableSet.<CollectExpression<Row, ?>>of(),
            new HashMap<>(), null, WriterProjection.OutputFormat.JSON_OBJECT);
    }
}
