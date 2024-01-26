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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.testing.BatchIteratorTester;
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

    private static final String JSON_AS_MAP_FIRST_LINE = "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}";
    private static final String JSON_AS_MAP_SECOND_LINE = "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}";
    private static final String CSV_AS_MAP_FIRST_LINE = "{\"name\":\"Arthur\",\"id\":\"4\",\"age\":\"38\"}";
    private static final String CSV_AS_MAP_SECOND_LINE = "{\"name\":\"Trillian\",\"id\":\"5\",\"age\":\"33\"}";
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
    public void testIteratorContract_givenJSONInputFormat_AndNoRelevantFileExtension_thenWritesAsMap() throws Exception {
        Path tempFile = createTempFile("tempfile", ".any-suffix");
        Files.write(tempFile, List.of(
            "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}",
            "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
        var fileUri = tempFile.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), JSON
        );

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{JSON_AS_MAP_FIRST_LINE},
            new Object[]{JSON_AS_MAP_SECOND_LINE});
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testIteratorContract_givenCSVInputFormat__AndNoRelevantFileExtension_thenWritesAsMap() throws Exception {
        Path tempFile = createTempFile("tempfile", ".any-suffix");
        Files.write(tempFile, List.of("name,id,age", "Arthur,4,38", "Trillian,5,33"));
        var fileUri = tempFile.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), CSV
        );

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{CSV_AS_MAP_FIRST_LINE},
            new Object[]{CSV_AS_MAP_SECOND_LINE});
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testIteratorContract_givenDefaultJsonInputFormat_AndJSONExtension_thenWritesAsMap() throws Exception {
        Path tempFile = createTempFile("tempfile", ".json");
        Files.write(tempFile, List.of(
            "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}",
            "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
        var fileUri = tempFile.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), JSON
        );

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{JSON_AS_MAP_FIRST_LINE},
            new Object[]{JSON_AS_MAP_SECOND_LINE});
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testIteratorContract_givenDefaultJsonInputFormat_AndCSVExtension_thenWritesAsMap() throws Exception {
        Path tempFile = createTempFile("tempfile", ".csv");
        Files.write(tempFile, List.of("name,id,age", "Arthur,4,38", "Trillian,5,33"));
        var fileUri = tempFile.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), JSON
        );

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{CSV_AS_MAP_FIRST_LINE},
            new Object[]{CSV_AS_MAP_SECOND_LINE});
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

        Reference raw = createReference("_raw", DataTypes.STRING);
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation);
        List<Input<?>> inputs = Collections.singletonList(ctx.add(raw));


        Supplier<BatchIterator<Row>> batchIteratorSupplier =
            () -> new FileReadingIterator(
                fileUris.stream().map(FileReadingIterator::toURI).toList(),
                inputs,
                ctx.expressions(),
                null,
                Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
                false,
                1,
                0,
                List.of("name", "id", "age"),
                CopyFromParserProperties.DEFAULT,
                JSON,
                Settings.EMPTY,
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
            new Object[]{CSV_AS_MAP_FIRST_LINE},
            new Object[]{CSV_AS_MAP_SECOND_LINE});
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

        Reference raw = createReference("_raw", DataTypes.STRING);
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation);
        List<Input<?>> inputs = Collections.singletonList(ctx.add(raw));


        Supplier<BatchIterator<Row>> batchIteratorSupplier =
            () -> new FileReadingIterator(
                fileUris.stream().map(FileReadingIterator::toURI).toList(),
                inputs,
                ctx.expressions(),
                null,
                Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
                false,
                1,
                0,
                List.of("id"),
                new CopyFromParserProperties(true, true, ',', 0),
                CSV,
                Settings.EMPTY,
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
            new Object[]{"{\"id\":\"1\"}"},
            new Object[]{"{\"id\":\"2\"}"},
            new Object[]{"{\"id\":\"3\"}"},
            new Object[]{"{\"id\":\"4\"}"},
            new Object[]{"{\"id\":\"5\"}"}
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


        Reference raw = createReference("_raw", DataTypes.STRING);
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation);
        List<Input<?>> inputs = Collections.singletonList(ctx.add(raw));


        Supplier<BatchIterator<Row>> batchIteratorSupplier =
            () -> new FileReadingIterator(
                fileUris.stream().map(FileReadingIterator::toURI).toList(),
                inputs,
                ctx.expressions(),
                null,
                Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
                false,
                1,
                0,
                List.of("id"),
                new CopyFromParserProperties(true, false, ',', skipNumLines),
                CSV,
                Settings.EMPTY,
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
            new Object[]{"{\"id\":\"2\"}"},
            new Object[]{"{\"id\":\"3\"}"},
            new Object[]{"{\"id\":\"4\"}"},
            new Object[]{"{\"id\":\"5\"}"}
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void test_loadNextBatch_implements_retry_with_backoff() throws IOException {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        var fi = new FileReadingIterator(
            List.of(),
            List.of(),
            List.of(),
            null,
            Map.of(),
            false,
            1,
            0,
            List.of(),
            null,
            CSV,
            Settings.EMPTY,
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

        Reference raw = createReference("_raw", DataTypes.STRING);
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation);
        List<Input<?>> inputs = Collections.singletonList(ctx.add(raw));

        var fi = new FileReadingIterator(
            fileUris.stream().map(FileReadingIterator::toURI).toList(),
            inputs,
            ctx.expressions(),
            null,
            Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0,
            List.of("id"),
            new CopyFromParserProperties(true, false, ',', 0),
            CSV,
            Settings.EMPTY,
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
        assertThat(fi.currentElement().get(0)).isEqualTo("{\"id\":\"1\"}");
        assertThat(fi.moveNext()).isEqualTo(true);
        assertThat(fi.currentElement().get(0)).isEqualTo("{\"id\":\"2\"}");
        assertThat(fi.moveNext()).isEqualTo(false);
        assertThat(fi.allLoaded()).isEqualTo(false);
        var backoff = fi.loadNextBatch();
        backoff.thenRun(
            () -> {
                assertThat(fi.currentElement().get(0)).isEqualTo("{\"id\":\"2\"}");
                assertThat(fi.watermark).isEqualTo(3);
                assertThat(fi.moveNext()).isEqualTo(true);
                // the watermark helped 'fi' to recover the state right before the exception then cleared
                assertThat(fi.watermark).isEqualTo(0);
                assertThat(fi.currentElement().get(0)).isEqualTo("{\"id\":\"3\"}");

                // verify the exception did not prevent reading the next URI
                assertThat(fi.moveNext()).isEqualTo(true);
                assertThat(fi.currentElement().get(0)).isEqualTo("{\"id\":\"4\"}");
            }
        ).join();
    }

    private BatchIterator<Row> createBatchIterator(Collection<String> fileUris,
                                                   FileUriCollectPhase.InputFormat format) {
        Reference raw = createReference("_raw", DataTypes.STRING);
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation);

        List<Input<?>> inputs = Collections.singletonList(ctx.add(raw));
        return FileReadingIterator.newInstance(
            fileUris.stream().map(FileReadingIterator::toURI).toList(),
            inputs,
            ctx.expressions(),
            null,
            Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0,
            List.of("name", "id", "age"),
            CopyFromParserProperties.DEFAULT,
            format,
            Settings.EMPTY,
            THREAD_POOL.scheduler());
    }
}
