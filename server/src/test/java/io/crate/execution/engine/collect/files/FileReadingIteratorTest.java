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

package io.crate.execution.engine.collect.files;

import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.CSV;
import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.JSON;
import static io.crate.execution.engine.collect.files.FileReadingIterator.MAX_SOCKET_TIMEOUT_RETRIES;
import static io.crate.testing.TestingHelpers.createReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.crate.data.testing.BatchIteratorTester;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.file.FileLineReferenceResolver;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;

public class FileReadingIteratorTest extends ESTestCase {

    private static final Object[] FIRST_JSON_LINE_IN_TARGET_ORDER = createJsonLine("Arthur", 4, 38);
    private static final Object[] SECOND_JSON_LINE_IN_TARGET_ORDER = createJsonLine("Trillian", 5, 33);

    private static final Object[] FIRST_CSV_LINE_IN_TARGET_ORDER = new Object[]{"Arthur", 4, 38};
    private static final Object[] SECOND_CSV_LINE_IN_TARGET_ORDER = new Object[]{"Trillian", 5, 33};

    private static final TransactionContext TXN_CTX = CoordinatorTxnCtx.systemTransactionContext();
    private static ThreadPool THREAD_POOL;

    private InputFactory inputFactory;

    @BeforeClass
    public static void setupThreadPool() {
        THREAD_POOL = new TestThreadPool(Thread.currentThread().getName());
    }

    @AfterClass
    public static void shutdownThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
    }

    @Before
    public void prepare() {
        NodeContext nodeCtx = new NodeContext(new Functions(Map.of()), null);
        inputFactory = new InputFactory(nodeCtx);
    }

    @Test
    public void testIteratorContract_givenJSONInputFormat_AndNoRelevantFileExtension_thenWritesAsArray() throws Exception {
        Path tempFile = createTempFile("tempfile", ".any-suffix");
        Files.write(tempFile, List.of(
            "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}",
            "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
        var fileUri = tempFile.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), JSON, true
        );

        List<Object[]> expectedResult = Arrays.asList(
            FIRST_JSON_LINE_IN_TARGET_ORDER,
            SECOND_JSON_LINE_IN_TARGET_ORDER
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testIteratorContract_givenCSVInputFormat__AndNoRelevantFileExtension_thenWritesAsArray() throws Exception {
        Path tempFile = createTempFile("tempfile", ".any-suffix");
        Files.write(tempFile, List.of("name,id,age", "Arthur,4,38", "Trillian,5,33"));
        var fileUri = tempFile.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), CSV, false
        );

        List<Object[]> expectedResult = Arrays.asList(
            FIRST_CSV_LINE_IN_TARGET_ORDER,
            SECOND_CSV_LINE_IN_TARGET_ORDER
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testIteratorContract_givenDefaultJsonInputFormat_AndJSONExtension_thenWritesAsArray() throws Exception {
        Path tempFile = createTempFile("tempfile", ".json");
        Files.write(tempFile, List.of(
            "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}",
            "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
        var fileUri = tempFile.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), JSON, true
        );

        List<Object[]> expectedResult = Arrays.asList(
            FIRST_JSON_LINE_IN_TARGET_ORDER,
            SECOND_JSON_LINE_IN_TARGET_ORDER
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testIteratorContract_givenDefaultJsonInputFormat_AndCSVExtension_thenWritesAsArray() throws Exception {
        Path tempFile = createTempFile("tempfile", ".csv");
        Files.write(tempFile, List.of("name,id,age", "Arthur,4,38", "Trillian,5,33"));
        var fileUri = tempFile.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), JSON, false
        );

        List<Object[]> expectedResult = Arrays.asList(
            FIRST_CSV_LINE_IN_TARGET_ORDER,
            SECOND_CSV_LINE_IN_TARGET_ORDER
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    /**
     * Tests a regression resulting in an infinitive loop as the reader wasn't closed on IO errors
     */
    @Test
    public void test_iterator_closes_current_reader_on_io_error() throws Exception {
        Path tempFile1 = createTempFile("tempfile1", ".csv");
        Files.write(tempFile1, List.of("name,id,age",
                                       "Arthur,4,38",
                                       "Douglas,6,42"  // <--- reader will fail on this line, so it is not part of the expected results
        ));
        Path tempFile2 = createTempFile("tempfile2", ".csv");
        Files.write(tempFile2, List.of("name,id,age", "Trillian,5,33"));
        List<String> fileUris = List.of(tempFile1.toUri().toString(), tempFile2.toUri().toString());

        Reference name = createReference("name", DataTypes.STRING);
        Reference id = createReference("id", DataTypes.INTEGER);
        Reference age = createReference("age", DataTypes.INTEGER);
        List<Reference> targetColumns = new ArrayList<>();
        targetColumns.add(name);
        targetColumns.add(id);
        targetColumns.add(age);

        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation);

        ctx.add(targetColumns);

        ColumnIndexWriterProjection projection = mock(ColumnIndexWriterProjection.class);
        when(projection.allTargetColumns()).thenReturn(targetColumns);

        Supplier<BatchIterator<Row>> batchIteratorSupplier =
            () -> new FileReadingIterator(
                fileUris,
                ctx,
                null,
                Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
                false,
                1,
                0,
                List.of("name", "id", "age"),
                CopyFromParserProperties.DEFAULT,
                JSON,
                Settings.EMPTY,
                projection,
                THREAD_POOL.scheduler()
            ) {

                @Override
                BufferedReader createBufferedReader(InputStream inputStream) throws IOException {
                    return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {

                        private int currentLineNumber = 0;

                        @Override
                        public String readLine() throws IOException {
                            var line = super.readLine();
                            currentLineNumber++;
                            if (line != null && currentLineNumber > 2) {      // fail on 3rd line, succeed on header and first row
                                throw new IOException("dummy");
                            }
                            return line;
                        }
                    };
                }
            };

        List<Object[]> expectedResult = Arrays.asList(
            FIRST_CSV_LINE_IN_TARGET_ORDER,
            SECOND_CSV_LINE_IN_TARGET_ORDER
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    /**
     * Validates a bug which was resulting in duplicate reads of the same line when consecutive retries happen.
     * https://github.com/crate/crate/pull/13261
     */
    @Test
    public void test_consecutive_retries_will_not_result_in_duplicate_reads() throws Exception {
        Path tempFile = createTempFile("tempfile1", ".csv");
        Files.write(tempFile, List.of("id", "1", "2", "3", "4", "5"));
        List<String> fileUris = List.of(tempFile.toUri().toString());

        Reference id = createReference("id", DataTypes.INTEGER);
        List<Reference> targetColumns = new ArrayList<>();
        targetColumns.add(id);

        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation);

        ctx.add(targetColumns);

        ColumnIndexWriterProjection projection = mock(ColumnIndexWriterProjection.class);
        when(projection.allTargetColumns()).thenReturn(targetColumns);

        Supplier<BatchIterator<Row>> batchIteratorSupplier =
            () -> new FileReadingIterator(
                fileUris,
                ctx,
                null,
                Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
                false,
                1,
                0,
                List.of("id"),
                new CopyFromParserProperties(true, true, ',', 0),
                CSV,
                Settings.EMPTY,
                projection,
                THREAD_POOL.scheduler()
            ) {
                int retry = 0;

                @Override
                BufferedReader createBufferedReader(InputStream inputStream) throws IOException {
                    return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {

                        private int currentLineNumber = 0;

                        @Override
                        public String readLine() throws IOException {
                            var line = super.readLine();
                            // current implementation does not handle SocketTimeoutException thrown when parsing header so skip it here as well.
                            if (currentLineNumber++ > 0 && retry++ < MAX_SOCKET_TIMEOUT_RETRIES) {
                                throw new SocketTimeoutException("dummy");
                            }
                            return line;
                        }
                    };
                }
            };

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5}
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void test_skipping_csv_headers_and_rows_combined_with_retry_logic() throws Exception {
        final int skipNumLines = 2;
        Path tempFile = createTempFile("tempfile1", ".csv");
        Files.write(tempFile, List.of("id", "1", "2", "3", "4", "5"));
        List<String> fileUris = List.of(tempFile.toUri().toString());

        Reference id = createReference("id", DataTypes.INTEGER);
        List<Reference> targetColumns = new ArrayList<>();
        targetColumns.add(id);

        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation);

        ctx.add(targetColumns);

        ColumnIndexWriterProjection projection = mock(ColumnIndexWriterProjection.class);
        when(projection.allTargetColumns()).thenReturn(targetColumns);
        Supplier<BatchIterator<Row>> batchIteratorSupplier =
            () -> new FileReadingIterator(
                fileUris,
                ctx,
                null,
                Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
                false,
                1,
                0,
                List.of("id"),
                new CopyFromParserProperties(true, false, ',', skipNumLines),
                CSV,
                Settings.EMPTY,
                projection,
                THREAD_POOL.scheduler()
            ) {
                int retry = 0;
                final List<String> linesToThrow = List.of("3", "2", "3", "5", "2");
                int linesToThrowIndex = 0;

                @Override
                BufferedReader createBufferedReader(InputStream inputStream) throws IOException {
                    return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {

                        @Override
                        public String readLine() throws IOException {
                            var line = super.readLine();
                            if (linesToThrow.get(linesToThrowIndex).equals(line) && retry++ < MAX_SOCKET_TIMEOUT_RETRIES) {
                                linesToThrowIndex = (linesToThrowIndex + 1) % linesToThrow.size();
                                throw new SocketTimeoutException("dummy");
                            }
                            return line;
                        }
                    };
                }
            };

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
            new Object[]{5}
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void test_loadNextBatch_implements_retry_with_backoff() throws IOException {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        var fi = new FileReadingIterator(
            List.of(),
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation),
            null,
            Map.of(),
            false,
            1,
            0,
            List.of(),
            null,
            CSV,
            Settings.EMPTY,
            null,
            scheduler
        );
        ArgumentCaptor<Long> delays = ArgumentCaptor.forClass(Long.class);

        for (int i = 0; i < MAX_SOCKET_TIMEOUT_RETRIES; i++) {
            fi.loadNextBatch().complete(null);
        }

        verify(scheduler, times(MAX_SOCKET_TIMEOUT_RETRIES))
            .schedule(any(Runnable.class), delays.capture(), eq(TimeUnit.MILLISECONDS));
        final List<Long> actualDelays = delays.getAllValues();
        assertThat(actualDelays).isEqualTo(Arrays.asList(0L, 10L, 30L, 100L, 230L));

        // retry fails if MAX_SOCKET_TIMEOUT_RETRIES is exceeded
        assertThatThrownBy(fi::loadNextBatch)
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("All batches already loaded");
    }

    @Test
    public void test_retry_from_one_uri_does_not_affect_reading_next_uri() throws Exception {
        Path tempFile = createTempFile("tempfile1", ".csv");
        Files.write(tempFile, List.of("1", "2", "3"));
        Path tempFile2 = createTempFile("tempfile2", ".csv");
        Files.write(tempFile2, List.of("4", "5", "6"));
        List<String> fileUris = List.of(tempFile.toUri().toString(), tempFile2.toUri().toString());

        Reference id = createReference("id", DataTypes.INTEGER);
        List<Reference> targetColumns = new ArrayList<>();
        targetColumns.add(id);

        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation);

        ctx.add(targetColumns);

        ColumnIndexWriterProjection projection = mock(ColumnIndexWriterProjection.class);
        when(projection.allTargetColumns()).thenReturn(targetColumns);

        var fi = new FileReadingIterator(
            fileUris,
            ctx,
            null,
            Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0,
            List.of("id"),
            new CopyFromParserProperties(true, false, ',', 0),
            CSV,
            Settings.EMPTY,
            projection,
            THREAD_POOL.scheduler()
        ) {
            private boolean isThrownOnce = false;
            final int lineToThrow = 2;

            @Override
            BufferedReader createBufferedReader(InputStream inputStream) throws IOException {
                return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {

                    private int currentLineNumber = 0;
                    @Override
                    public String readLine() throws IOException {
                        var line = super.readLine();
                        if (!isThrownOnce && currentLineNumber++ == lineToThrow) {
                            isThrownOnce = true;
                            throw new SocketTimeoutException("dummy");
                        }
                        return line;
                    }
                };
            }
        };

        assertThat(fi.moveNext()).isEqualTo(true);
        assertThat(fi.currentElement().get(0)).isEqualTo(1);
        assertThat(fi.moveNext()).isEqualTo(true);
        assertThat(fi.currentElement().get(0)).isEqualTo(2);
        assertThat(fi.moveNext()).isEqualTo(false);
        assertThat(fi.allLoaded()).isEqualTo(false);
        var backoff = fi.loadNextBatch();
        backoff.thenRun(
            () -> {
                assertThat(fi.currentElement().get(0)).isEqualTo(2);
                assertThat(fi.watermark).isEqualTo(3);
                assertThat(fi.moveNext()).isEqualTo(true);
                // the watermark helped 'fi' to recover the state right before the exception then cleared
                assertThat(fi.watermark).isEqualTo(0);
                assertThat(fi.currentElement().get(0)).isEqualTo(3);

                // verify the exception did not prevent reading the next URI
                assertThat(fi.moveNext()).isEqualTo(true);
                assertThat(fi.currentElement().get(0)).isEqualTo(4);
            }
        ).join();
    }

    private BatchIterator<Row> createBatchIterator(Collection<String> fileUris,
                                                   FileUriCollectPhase.InputFormat format,
                                                   boolean ageInObject) {
        Reference name = createReference("name", DataTypes.STRING);
        Reference id = createReference("id", DataTypes.INTEGER);
        Reference age = createReference("age", DataTypes.INTEGER);
        Reference details = createReference("details", DataTypes.UNTYPED_OBJECT);
        List<Reference> targetColumns = new ArrayList<>();
        targetColumns.add(name);
        targetColumns.add(id);
        if (ageInObject) {
            targetColumns.add(details);
        } else {
            targetColumns.add(age);
        }

        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation);

        ctx.add(targetColumns);

        ColumnIndexWriterProjection projection = mock(ColumnIndexWriterProjection.class);
        when(projection.allTargetColumns()).thenReturn(targetColumns);

        return FileReadingIterator.newInstance(
            fileUris,
            ctx,
            null,
            Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0,
            targetColumns.stream().map(Reference::toString).collect(Collectors.toList()),
            CopyFromParserProperties.DEFAULT,
            format,
            Settings.EMPTY,
            projection,
            THREAD_POOL.scheduler());
    }

    private static Object[] createJsonLine(String name, int id, int age) {
        LinkedHashMap<String, Integer> details = new LinkedHashMap();
        details.put("age", age);
        return new Object[]{name, id, details};
    }
}
