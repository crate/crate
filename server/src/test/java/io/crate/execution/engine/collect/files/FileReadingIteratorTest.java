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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
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
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockMakers;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import io.crate.data.BatchIterator;
import io.crate.data.testing.BatchIteratorTester;
import io.crate.execution.engine.collect.files.FileReadingIterator.LineCursor;

public class FileReadingIteratorTest extends ESTestCase {

    @Rule
    public MockitoRule initRule = MockitoJUnit.rule();

    private static ThreadPool THREAD_POOL;

    @BeforeClass
    public static void setupThreadPool() {
        THREAD_POOL = new TestThreadPool(Thread.currentThread().getName());
    }

    @AfterClass
    public static void shutdownThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
    }

    /**
     * Tests a regression resulting in an infinitive loop as the reader wasn't closed on IO errors
     */
    @Test
    public void test_iterator_closes_current_reader_on_io_error() throws Exception {
        Path tempFile1 = createTempFile("tempfile1", ".csv");
        List<String> lines1 = List.of(
            "name,id,age",
            "Arthur,4,38",
            "Douglas,6,42"  // <--- reader will fail on this line, so it is not part of the expected results
        );
        Files.write(tempFile1, lines1);
        Path tempFile2 = createTempFile("tempfile2", ".csv");
        List<String> lines2 = List.of("name,id,age", "Trillian,5,33");
        Files.write(tempFile2, lines2);
        List<String> fileUris = List.of(tempFile1.toUri().toString(), tempFile2.toUri().toString());

        Supplier<BatchIterator<LineCursor>> batchIteratorSupplier =
            () -> new FileReadingIterator(
                fileUris,
                null,
                Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
                false,
                1,
                0,
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

        List<String> expectedResult = Arrays.asList(
            "name,id,age",
            "Arthur,4,38",
            "name,id,age",
            "Trillian,5,33"
        );
        var tester = new BatchIteratorTester<>(() -> batchIteratorSupplier.get().map(LineCursor::line));
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    /**
     * Validates a bug which was resulting in duplicate reads of the same line when consecutive retries happen.
     * https://github.com/crate/crate/pull/13261
     */
    @Test
    public void test_consecutive_retries_will_not_result_in_duplicate_reads() throws Exception {
        Path tempFile = createTempFile("tempfile1", ".csv");
        List<String> lines = List.of("id", "1", "2", "3", "4", "5");
        Files.write(tempFile, lines);
        List<String> fileUris = List.of(tempFile.toUri().toString());

        Supplier<BatchIterator<LineCursor>> batchIteratorSupplier =
            () -> new FileReadingIterator(
                fileUris,
                null,
                Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
                false,
                1,
                0,
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

        var tester = new BatchIteratorTester<>(() -> batchIteratorSupplier.get().map(LineCursor::line));
        tester.verifyResultAndEdgeCaseBehaviour(lines);
    }

    @Test
    public void test_loadNextBatch_implements_retry_with_backoff() throws IOException {
        ScheduledExecutorService scheduler =
            mock(ScheduledExecutorService.class, withSettings().mockMaker(MockMakers.SUBCLASS));
        var fi = new FileReadingIterator(
            List.of(),
            null,
            Map.of(),
            false,
            1,
            0,
            Settings.EMPTY,
            scheduler
        );
        ArgumentCaptor<Long> delays = ArgumentCaptor.forClass(Long.class);

        for (int i = 0; i < FileReadingIterator.MAX_SOCKET_TIMEOUT_RETRIES; i++) {
            fi.loadNextBatch().complete(null);
        }

        verify(scheduler, times(FileReadingIterator.MAX_SOCKET_TIMEOUT_RETRIES))
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

        var fi = new FileReadingIterator(
            fileUris,
            null,
            Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0,
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
        assertThat(fi.currentElement().line()).isEqualTo("1");
        assertThat(fi.moveNext()).isEqualTo(true);
        assertThat(fi.currentElement().line()).isEqualTo("2");
        assertThat(fi.moveNext()).isEqualTo(false);
        assertThat(fi.allLoaded()).isEqualTo(false);
        assertThat(fi.loadNextBatch()).succeedsWithin(5, TimeUnit.SECONDS)
            .satisfies(x -> {
                assertThat(fi.currentElement().line()).isEqualTo("2");
                assertThat(fi.watermark).isEqualTo(3);
                assertThat(fi.moveNext()).isEqualTo(true);
                // the watermark helped 'fi' to recover the state right before the exception then cleared
                assertThat(fi.watermark).isEqualTo(0);
                assertThat(fi.currentElement().line()).isEqualTo("3");

                // verify the exception did not prevent reading the next URI
                assertThat(fi.moveNext()).isEqualTo(true);
                assertThat(fi.currentElement().line()).isEqualTo("4");
            });
    }
}
