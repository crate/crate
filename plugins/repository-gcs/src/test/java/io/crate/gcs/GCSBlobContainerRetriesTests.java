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

package io.crate.gcs;

import static io.crate.gcs.GCSClientSettings.CLIENT_EMAIL_SETTING;
import static io.crate.gcs.GCSClientSettings.CLIENT_ID_SETTING;
import static io.crate.gcs.GCSClientSettings.ENDPOINT_SETTING;
import static io.crate.gcs.GCSClientSettings.PRIVATE_KEY_ID_SETTING;
import static io.crate.gcs.GCSClientSettings.PRIVATE_KEY_SETTING;
import static io.crate.gcs.GCSClientSettings.PROJECT_ID_SETTING;
import static io.crate.gcs.GCSClientSettings.READ_TIMEOUT_SETTING;
import static io.crate.gcs.GCSClientSettings.TOKEN_URI_SETTING;
import static io.crate.gcs.GCSHttpHandler.decodeQueryString;
import static io.crate.gcs.GCSHttpHandler.getContentRangeEnd;
import static io.crate.gcs.GCSHttpHandler.getContentRangeLimit;
import static io.crate.gcs.GCSHttpHandler.getContentRangeStart;
import static io.crate.gcs.GCSHttpHandler.parseMultipartRequestBody;
import static io.crate.gcs.GCSSnapshotIntegrationTest.PKCS8_PRIVATE_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.lucene.tests.util.LuceneTestCase.expectThrows;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.elasticsearch.repositories.ESBlobStoreTestCase.randomBytes;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threeten.bp.Duration;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.crate.common.SuppressForbidden;
import io.crate.common.collections.Tuple;
import io.crate.common.unit.TimeValue;

@SuppressForbidden(reason = "use a http server")
public class GCSBlobContainerRetriesTests extends IntegTestCase {

    static final long MAX_RANGE_VAL = Long.MAX_VALUE - 1;

    HttpServer httpServer;

    @Before
    public void setUp() throws Exception {
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        httpServer.stop(0);
        super.tearDown();
    }

    @Test
    public void testReadNonexistentBlobThrowsNoSuchFileException() {
        final BlobContainer blobContainer = createBlobContainer(between(1, 5), null);
        final long position = randomLongBetween(0, MAX_RANGE_VAL);
        final int length = randomIntBetween(1, Math.toIntExact(Math.min(Integer.MAX_VALUE, MAX_RANGE_VAL - position)));
        final Exception exception = expectThrows(NoSuchFileException.class, () -> {
            if (randomBoolean()) {
                Streams.readFully(blobContainer.readBlob("read_nonexistent_blob"));
            } else {
                Streams.readFully(blobContainer.readBlob("read_nonexistent_blob", 0, 1));
            }
        });
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT)).contains("blob object [read_nonexistent_blob] not found");

        assertThat(
            expectThrows(
                NoSuchFileException.class,
                () -> Streams.readFully(blobContainer.readBlob("read_nonexistent_blob", position, length))
            ).getMessage().toLowerCase(Locale.ROOT))
            .contains("blob object [read_nonexistent_blob] not found");
    }

    @Test
    public void testReadBlobWithRetries() throws Exception {
        final int maxRetries = randomInt(5);
        final CountDown countDown = new CountDown(maxRetries + 1);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext(downloadStorageEndpoint("read_blob_max_retries"), exchange -> {
            Streams.readFully(exchange.getRequestBody());
            if (countDown.countDown()) {
                final int rangeStart = getRangeStart(exchange);
                assertThat(rangeStart).isLessThan(bytes.length);
                exchange.getResponseHeaders().add("Content-Type", bytesContentType());
                exchange.sendResponseHeaders(HttpStatus.SC_OK, bytes.length - rangeStart);
                exchange.getResponseBody().write(bytes, rangeStart, bytes.length - rangeStart);
                exchange.close();
                return;
            }
            if (randomBoolean()) {
                exchange.sendResponseHeaders(
                    randomFrom(
                        HttpStatus.SC_INTERNAL_SERVER_ERROR,
                        HttpStatus.SC_BAD_GATEWAY,
                        HttpStatus.SC_SERVICE_UNAVAILABLE,
                        HttpStatus.SC_GATEWAY_TIMEOUT
                    ),
                    -1
                );
            } else if (randomBoolean()) {
                sendIncompleteContent(exchange, bytes);
            }
            if (randomBoolean()) {
                exchange.close();
            }
        });

        final TimeValue readTimeout = TimeValue.timeValueSeconds(between(1, 3));
        final BlobContainer blobContainer = createBlobContainer(maxRetries, readTimeout);
        try (InputStream inputStream = blobContainer.readBlob("read_blob_max_retries")) {
            final int readLimit;
            final InputStream wrappedStream;
            if (randomBoolean()) {
                // read stream only partly
                readLimit = randomIntBetween(0, bytes.length);
                wrappedStream = Streams.limitStream(inputStream, readLimit);
            } else {
                readLimit = bytes.length;
                wrappedStream = inputStream;
            }
            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(wrappedStream));
            logger.info("maxRetries={}, readLimit={}, byteSize={}, bytesRead={}", maxRetries, readLimit, bytes.length, bytesRead.length);
            assertThat(Arrays.copyOfRange(bytes, 0, readLimit)).isEqualTo(bytesRead);
            if (readLimit < bytes.length) {
                // we might have completed things based on an incomplete response, and we're happy with that
            } else {
                assertThat(countDown.isCountedDown()).isTrue();
            }
        }
    }

    @Test
    public void testReadRangeBlobWithRetries() throws Exception {
        final int maxRetries = randomInt(5);
        final CountDown countDown = new CountDown(maxRetries + 1);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext(downloadStorageEndpoint("read_range_blob_max_retries"), exchange -> {
            Streams.readFully(exchange.getRequestBody());
            if (countDown.countDown()) {
                final int rangeStart = getRangeStart(exchange);
                assertThat(rangeStart).isLessThan(bytes.length);
                assertThat(getRangeEnd(exchange).isPresent()).isTrue();
                final int rangeEnd = getRangeEnd(exchange).get();
                assertThat(rangeEnd).isGreaterThan(rangeStart);
                // adapt range end to be compliant to https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
                final int effectiveRangeEnd = Math.min(bytes.length - 1, rangeEnd);
                final int length = (effectiveRangeEnd - rangeStart) + 1;
                exchange.getResponseHeaders().add("Content-Type", bytesContentType());
                exchange.sendResponseHeaders(HttpStatus.SC_OK, length);
                exchange.getResponseBody().write(bytes, rangeStart, length);
                exchange.close();
                return;
            } else {
                exchange.sendResponseHeaders(
                    randomFrom(
                        HttpStatus.SC_INTERNAL_SERVER_ERROR,
                        HttpStatus.SC_BAD_GATEWAY,
                        HttpStatus.SC_SERVICE_UNAVAILABLE,
                        HttpStatus.SC_GATEWAY_TIMEOUT
                    ),
                    -1
                );
            }
            if (randomBoolean()) {
                exchange.close();
            }
        });

        final TimeValue readTimeout = TimeValue.timeValueMillis(between(100, 500));
        final BlobContainer blobContainer = createBlobContainer(maxRetries, readTimeout);
        final int position = randomIntBetween(0, bytes.length - 1);
        final int length = randomIntBetween(0, randomBoolean() ? bytes.length : Integer.MAX_VALUE);
        try (InputStream inputStream = blobContainer.readBlob("read_range_blob_max_retries", position, length)) {
            final int readLimit;
            final InputStream wrappedStream;
            if (randomBoolean()) {
                // read stream only partly
                readLimit = randomIntBetween(0, length);
                wrappedStream = Streams.limitStream(inputStream, readLimit);
            } else {
                readLimit = length;
                wrappedStream = inputStream;
            }
            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(wrappedStream));
            logger.info(
                "maxRetries={}, position={}, length={}, readLimit={}, byteSize={}, bytesRead={}",
                maxRetries,
                position,
                length,
                readLimit,
                bytes.length,
                bytesRead.length
            );
            assertThat(Arrays.copyOfRange(bytes, position, Math.min(bytes.length, position + readLimit))).isEqualTo(bytesRead);
            if (readLimit == 0 || (readLimit < length && readLimit == bytesRead.length)) {
                // we might have completed things based on an incomplete response, and we're happy with that
            } else {
                assertThat(countDown.isCountedDown()).isTrue();
            }
        }
    }

    @Test
    public void testReadBlobWithReadTimeouts() {
        final int maxRetries = randomInt(5);
        final TimeValue readTimeout = TimeValue.timeValueMillis(between(100, 200));
        final BlobContainer blobContainer = createBlobContainer(maxRetries, readTimeout);

        // HTTP server does not send a response
        httpServer.createContext(downloadStorageEndpoint("read_blob_unresponsive"), exchange -> {
        });

        Exception exception = expectThrows(
            unresponsiveExceptionType(),
            () -> Streams.readFully(blobContainer.readBlob("read_blob_unresponsive"))
        );
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT)).contains("read timed out");
        assertThat(exception.getCause()).isInstanceOfAny(SocketTimeoutException.class);

        // HTTP server sends a partial response
        final byte[] bytes = randomBlobContent();
        httpServer.createContext(downloadStorageEndpoint("read_blob_incomplete"), exchange -> sendIncompleteContent(exchange, bytes));

        final int position = randomIntBetween(0, bytes.length - 1);
        final int length = randomIntBetween(1, randomBoolean() ? bytes.length : Integer.MAX_VALUE);
        exception = expectThrows(Exception.class, () -> {
            try (
                InputStream stream = randomBoolean()
                    ? blobContainer.readBlob("read_blob_incomplete")
                    : blobContainer.readBlob("read_blob_incomplete", position, length)
            ) {
                Streams.readFully(stream);
            }
        });
        assertThat(exception).isInstanceOfAny(SocketTimeoutException.class, ConnectionClosedException.class, RuntimeException.class);
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT))
            .containsAnyOf("read timed out",
                "premature end of chunk coded message body: closing chunk expected",
                "Read timed out",
                "unexpected end of file from server");
        assertThat(exception.getSuppressed().length).isEqualTo(maxRetries);
    }

    @Test
    public void testReadBlobWithNoHttpResponse() {
        final TimeValue readTimeout = TimeValue.timeValueMillis(between(100, 200));
        final BlobContainer blobContainer = createBlobContainer(randomInt(5), readTimeout);

        // HTTP server closes connection immediately
        httpServer.createContext(downloadStorageEndpoint("read_blob_no_response"), HttpExchange::close);

        Exception exception = expectThrows(unresponsiveExceptionType(), () -> {
            if (randomBoolean()) {
                Streams.readFully(blobContainer.readBlob("read_blob_no_response"));
            } else {
                Streams.readFully(blobContainer.readBlob("read_blob_no_response", 0, 1));
            }
        });
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT))
            .containsAnyOf("the target server failed to respond", "unexpected end of file from server");
    }

    @Test
    public void testReadBlobWithPrematureConnectionClose() {
        final int maxRetries = randomInt(20);
        final BlobContainer blobContainer = createBlobContainer(maxRetries, null);

        // HTTP server sends a partial response
        final byte[] bytes = randomBlobContent(1);
        httpServer.createContext(downloadStorageEndpoint("read_blob_incomplete"), exchange -> {
            sendIncompleteContent(exchange, bytes);
            exchange.close();
        });

        final Exception exception = expectThrows(Exception.class, () -> {
            try (
                InputStream stream = randomBoolean()
                    ? blobContainer.readBlob("read_blob_incomplete", 0, 1)
                    : blobContainer.readBlob("read_blob_incomplete")
            ) {
                Streams.readFully(stream);
            }
        });
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT))
            .containsAnyOf(
                "premature end of chunk coded message body: closing chunk expected",
                "premature end of content-length delimited message body", "connection closed prematurely"
            );
        assertThat(exception.getSuppressed().length).isEqualTo(Math.min(10, maxRetries));
    }

    @Test
    public void testReadLargeBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(2, 10);
        final AtomicInteger countDown = new AtomicInteger(maxRetries);

        // SDK reads in 2 MB chunks so we use twice that to simulate 2 chunks
        final byte[] bytes = randomBytes(1 << 22);
        httpServer.createContext("/download/storage/v1/b/bucket/o/large_blob_retries", exchange -> {
            Streams.readFully(exchange.getRequestBody());
            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            final Tuple<Long, Long> range = getRange(exchange);
            final int offset = Math.toIntExact(range.v1());
            final byte[] chunk = Arrays.copyOfRange(bytes, offset, Math.toIntExact(Math.min(range.v2() + 1, bytes.length)));
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), chunk.length);
            if (randomBoolean() && countDown.decrementAndGet() >= 0) {
                exchange.getResponseBody().write(chunk, 0, chunk.length - 1);
                exchange.close();
                return;
            }
            exchange.getResponseBody().write(chunk);
            exchange.close();
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries, null);
        try (InputStream inputStream = blobContainer.readBlob("large_blob_retries")) {
            assertThat(bytes).isEqualTo(BytesReference.toBytes(Streams.readFully(inputStream)));
        }
    }

    @Test
    public void testWriteBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(2, 10);
        final CountDown countDown = new CountDown(maxRetries);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/upload/storage/v1/b/bucket/o", safeHandler(exchange -> {
            assertThat(exchange.getRequestURI().getQuery()).contains("uploadType=multipart");
            if (countDown.countDown()) {
                Optional<Tuple<String, BytesReference>> content = parseMultipartRequestBody(exchange.getRequestBody());
                assertThat(content.isPresent()).isTrue();
                assertThat(content.get().v1()).isEqualTo("write_blob_max_retries");
                if (Objects.deepEquals(bytes, BytesReference.toBytes(content.get().v2()))) {
                    byte[] response = ("{\"bucket\":\"bucket\",\"name\":\"" + content.get().v1() + "\"}").getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                } else {
                    exchange.sendResponseHeaders(HttpStatus.SC_BAD_REQUEST, -1);
                }
                return;
            }
            if (randomBoolean()) {
                if (randomBoolean()) {
                    Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, Math.max(1, bytes.length - 1))]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                    exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
                }
            }
        }));

        final BlobContainer blobContainer = createBlobContainer(maxRetries, null);
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
            blobContainer.writeBlob("write_blob_max_retries", stream, bytes.length, false);
        }
        assertThat(countDown.isCountedDown()).isTrue();
    }

    @Test
    public void testWriteBlobWithReadTimeouts() {
        final byte[] bytes = randomByteArrayOfLength(randomIntBetween(10, 128));
        final TimeValue readTimeout = TimeValue.timeValueMillis(randomIntBetween(100, 500));
        final BlobContainer blobContainer = createBlobContainer(1, readTimeout);

        // HTTP server does not send a response
        httpServer.createContext("/upload/storage/v1/b/bucket/o", exchange -> {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, bytes.length - 1)]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                }
            }
        });
        assertThatThrownBy(() -> {
            try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
                blobContainer.writeBlob("write_blob_timeout", stream, bytes.length, false);
            }
        }).isInstanceOfAny(StorageException.class).hasMessage("Read timed out");
    }

    @Test
    public void testWriteLargeBlob() throws IOException {
        // See {@link BaseWriteChannel#DEFAULT_CHUNK_SIZE}
        final int defaultChunkSize = 60 * 256 * 1024;
        final int nbChunks = randomIntBetween(3, 5);
        final int lastChunkSize = randomIntBetween(1, defaultChunkSize - 1);
        final int totalChunks = nbChunks + 1;
        final byte[] data = randomBytes(defaultChunkSize * nbChunks + lastChunkSize);
        assertThat(data.length).isGreaterThan(GCSBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE);

        final int nbErrors = 2; // we want all requests to fail at least once
        final AtomicInteger countInits = new AtomicInteger(nbErrors);
        final AtomicInteger countUploads = new AtomicInteger(nbErrors * totalChunks);
        final AtomicBoolean allow410Gone = new AtomicBoolean(randomBoolean());
        final AtomicBoolean allowReadTimeout = new AtomicBoolean(rarely());
        final int wrongChunk = randomIntBetween(1, totalChunks);

        final AtomicReference<String> sessionUploadId = new AtomicReference<>(UUIDs.randomBase64UUID());

        httpServer.createContext("/upload/storage/v1/b/bucket/o", safeHandler(exchange -> {
            final BytesReference requestBody = Streams.readFully(exchange.getRequestBody());

            final Map<String, String> params = decodeQueryString(exchange.getRequestURI());
            assertThat(params.get("uploadType")).isEqualTo("resumable");

            if ("POST".equals(exchange.getRequestMethod())) {
                assertThat(params.get("name")).isEqualTo("write_large_blob");
                if (countInits.decrementAndGet() <= 0) {
                    byte[] response = requestBody.utf8ToString().getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.getResponseHeaders()
                        .add(
                            "Location",
                            httpServerUrl() + "/upload/storage/v1/b/bucket/o?uploadType=resumable&upload_id=" + sessionUploadId.get()
                        );
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                    return;
                }
                if (allowReadTimeout.get()) {
                    assertThat(wrongChunk).isGreaterThan(0);
                    return;
                }

            } else if ("PUT".equals(exchange.getRequestMethod())) {
                final String uploadId = params.get("upload_id");
                if (uploadId.equals(sessionUploadId.get()) == false) {
                    assertThat(wrongChunk).isGreaterThan(0);
                    exchange.sendResponseHeaders(HttpStatus.SC_GONE, -1);
                    return;
                }

                if (countUploads.get() == (wrongChunk * nbErrors)) {
                    if (allowReadTimeout.compareAndSet(true, false)) {
                        assertThat(wrongChunk).isGreaterThan(0);
                        return;
                    }
                    if (allow410Gone.compareAndSet(true, false)) {
                        final String newUploadId = UUIDs.randomBase64UUID(random());
                        sessionUploadId.set(newUploadId);

                        // we must reset the counters because the whole object upload will be retried
                        countInits.set(nbErrors);
                        countUploads.set(nbErrors * totalChunks);

                        exchange.sendResponseHeaders(HttpStatus.SC_GONE, -1);
                        return;
                    }
                }

                final String range = exchange.getRequestHeaders().getFirst("Content-Range");
                assertThat(Strings.hasLength(range)).isTrue();

                if (countUploads.decrementAndGet() % 2 == 0) {
                    final int rangeStart = getContentRangeStart(range);
                    final int rangeEnd = getContentRangeEnd(range);
                    assertThat(rangeEnd + 1 - rangeStart).isEqualTo(Math.toIntExact(requestBody.length()));
                    assertThat(new BytesArray(data, rangeStart, rangeEnd - rangeStart + 1)).isEqualTo(requestBody);

                    final Integer limit = getContentRangeLimit(range);
                    if (limit != null) {
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                        return;
                    } else {
                        exchange.getResponseHeaders().add("Range", String.format(Locale.ROOT, "bytes=%d/%d", rangeStart, rangeEnd));
                        exchange.getResponseHeaders().add("Content-Length", "0");
                        exchange.sendResponseHeaders(308 /* Resume Incomplete */, -1);
                        return;
                    }
                }
            }

            if (randomBoolean()) {
                exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
            }
        }));

        final TimeValue readTimeout = allowReadTimeout.get() ? TimeValue.timeValueSeconds(3) : null;

        final BlobContainer blobContainer = createBlobContainer(nbErrors + 1, readTimeout);
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", data), data.length)) {
            blobContainer.writeBlob("write_large_blob", stream, data.length, false);
        }

        assertThat(countInits.get()).isEqualTo(0);
        assertThat(countUploads.get()).isEqualTo(0);
        assertThat(allow410Gone.get()).isFalse();
    }

    static byte[] randomBlobContent() {
        return randomBlobContent(1);
    }

    static byte[] randomBlobContent(int minSize) {
        return randomByteArrayOfLength(randomIntBetween(minSize, frequently() ? 512 : 1 << 20)); // rarely up to 1mb
    }

    static final Pattern RANGE_PATTERN = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$");

    static Tuple<Long, Long> getRange(HttpExchange exchange) {
        final String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
        if (rangeHeader == null) {
            return new Tuple<>(0L, MAX_RANGE_VAL);
        }

        final Matcher matcher = RANGE_PATTERN.matcher(rangeHeader);
        assertThat(matcher.matches()).isTrue();
        long rangeStart = Long.parseLong(matcher.group(1));
        long rangeEnd = Long.parseLong(matcher.group(2));
        assertThat(rangeStart).isLessThanOrEqualTo(rangeEnd);
        return new Tuple<>(rangeStart, rangeEnd);
    }

    static int getRangeStart(HttpExchange exchange) {
        return Math.toIntExact(getRange(exchange).v1());
    }

    static Optional<Integer> getRangeEnd(HttpExchange exchange) {
        final long rangeEnd = getRange(exchange).v2();
        if (rangeEnd == MAX_RANGE_VAL) {
            return Optional.empty();
        }
        return Optional.of(Math.toIntExact(rangeEnd));
    }

    void sendIncompleteContent(HttpExchange exchange, byte[] bytes) throws IOException {
        final int rangeStart = getRangeStart(exchange);
        assertThat(rangeStart).isLessThan(bytes.length);
        final Optional<Integer> rangeEnd = getRangeEnd(exchange);
        final int length;
        if (rangeEnd.isPresent()) {
            // adapt range end to be compliant to https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
            final int effectiveRangeEnd = Math.min(rangeEnd.get(), bytes.length - 1);
            length = effectiveRangeEnd - rangeStart + 1;
        } else {
            length = bytes.length - rangeStart;
        }
        exchange.getResponseHeaders().add("Content-Type", bytesContentType());
        exchange.sendResponseHeaders(HttpStatus.SC_OK, length);
        int minSend = Math.min(0, length - 1);
        final int bytesToSend = randomIntBetween(minSend, length - 1);
        if (bytesToSend > 0) {
            exchange.getResponseBody().write(bytes, rangeStart, bytesToSend);
        }
        if (randomBoolean()) {
            exchange.getResponseBody().flush();
        }
    }

    private String httpServerUrl() {
        assertThat(httpServer).isNotNull();
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    String downloadStorageEndpoint(String blob) {
        return "/download/storage/v1/b/bucket/o/" + blob;
    }

    String bytesContentType() {
        return "application/octet-stream";
    }

    Class<? extends Exception> unresponsiveExceptionType() {
        return StorageException.class;
    }

    BlobContainer createBlobContainer(
        final @Nullable Integer maxRetries,
        final @Nullable TimeValue readTimeout
    ) {
        final Settings.Builder clientSettings = Settings.builder();
        clientSettings.put(PROJECT_ID_SETTING.getKey(), "project_id");
        clientSettings.put(CLIENT_ID_SETTING.getKey(), "client_id");
        clientSettings.put(CLIENT_EMAIL_SETTING.getKey(), "mail@foobar.com");
        clientSettings.put(PRIVATE_KEY_ID_SETTING.getKey(), "id1234");
        clientSettings.put(PRIVATE_KEY_SETTING.getKey(), PKCS8_PRIVATE_KEY);
        clientSettings.put(ENDPOINT_SETTING.getKey(), httpServerUrl());
        clientSettings.put(TOKEN_URI_SETTING.getKey(), httpServerUrl() + "/token");
        if (readTimeout != null) {
            clientSettings.put(READ_TIMEOUT_SETTING.getKey(), readTimeout);
        }

        final GCSService service = new GCSService() {

            @Override
            StorageOptions createStorageOptions(
                final GCSClientSettings clientSettings,
                final HttpTransportOptions httpTransportOptions
            ) {
                StorageOptions options = super.createStorageOptions(clientSettings, httpTransportOptions);
                RetrySettings.Builder retrySettingsBuilder = RetrySettings.newBuilder()
                    .setTotalTimeout(options.getRetrySettings().getTotalTimeout())
                    .setInitialRetryDelay(Duration.ofMillis(10L))
                    .setRetryDelayMultiplier(1.0d)
                    .setMaxRetryDelay(Duration.ofSeconds(1L))
                    .setInitialRpcTimeout(Duration.ofSeconds(1))
                    .setRpcTimeoutMultiplier(options.getRetrySettings().getRpcTimeoutMultiplier())
                    .setMaxRpcTimeout(Duration.ofSeconds(1));
                if (maxRetries != null) {
                    retrySettingsBuilder.setMaxAttempts(maxRetries + 1);
                }
                return options.toBuilder()
                    .setHost(options.getHost())
                    .setCredentials(options.getCredentials())
                    .setRetrySettings(retrySettingsBuilder.build())
                    .build();
            }
        };

        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("test", "gcs", clientSettings.build());

        httpServer.createContext("/token", new FakeOAuth2HttpHandler());
        final GCSBlobStore blobStore = new GCSBlobStore(
            "bucket",
            service,
            repositoryMetadata,
            randomIntBetween(1, 8) * 1024
        );

        return new GCSBlobContainer(BlobPath.cleanPath(), blobStore);
    }

    private HttpHandler safeHandler(HttpHandler handler) {
        final HttpHandler loggingHandler = wrap(handler, logger);
        return exchange -> {
            try {
                loggingHandler.handle(exchange);
            } finally {
                exchange.close();
            }
        };
    }

    static HttpHandler wrap(final HttpHandler handler, final Logger logger) {
        return new ExceptionCatchingHttpHandler(handler, logger);
    }

    static class ExceptionCatchingHttpHandler implements HttpHandler {

        private final HttpHandler handler;
        private final Logger logger;

        ExceptionCatchingHttpHandler(HttpHandler handler, Logger logger) {
            this.handler = handler;
            this.logger = logger;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                handler.handle(exchange);
            } catch (Throwable t) {
                logger.error(
                    () -> new ParameterizedMessage(
                        "Exception when handling request {} {} {}",
                        exchange.getRemoteAddress(),
                        exchange.getRequestMethod(),
                        exchange.getRequestURI()
                    ),
                    t
                );
                throw t;
            }
        }
    }
}
