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

import static io.crate.common.exceptions.Exceptions.rethrowUnchecked;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.zip.GZIPInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.common.exceptions.Exceptions;
import io.crate.common.unit.TimeValue;
import io.crate.data.BatchIterator;

/**
 * BatchIterator to read lines from one or more {@link URI}s.
 *
 * <p>
 * URIs are opened using a {@link FileInputFactory}.
 * A map from Scheme -> FileInputFactory is parameterized in the constructor to support
 * arbitrary sources.
 * </p>
 *
 * <p>
 * The iterator automatically retries reading on
 * @{link {@link SocketException} or {@link SocketTimeoutException}
 * </p>
 *
 * <p>
 * The file content is exposed via a shared {@link LineCursor}
 * It's properties are mutated after each {@link #moveNext()} call.
 * Use {@link LineCursor#copy()} if you need an instance that's not shared.
 *
 * {@link #currentElement()} can be used in a "off-position", before the first {@link #moveNext()} call
 * to gain early access to the cursor.
 * </p>
 */
public class FileReadingIterator implements BatchIterator<FileReadingIterator.LineCursor> {

    private static final Logger LOGGER = LogManager.getLogger(FileReadingIterator.class);
    @VisibleForTesting
    static final int MAX_SOCKET_TIMEOUT_RETRIES = 5;

    private static final Predicate<URI> MATCH_ALL_PREDICATE = (URI input) -> true;

    private final Map<String, FileInputFactory> fileInputFactories;
    private final Boolean shared;
    private final int numReaders;
    private final int readerNumber;
    private final boolean compressed;
    private final List<FileInput> fileInputs;

    private volatile Throwable killed;

    private Iterator<FileInput> fileInputsIterator = null;
    private FileInput currentInput = null;
    private Iterator<URI> currentInputUriIterator = null;
    private BufferedReader currentReader = null;

    @VisibleForTesting
    long watermark;

    private final LineCursor cursor;
    private final ScheduledExecutorService scheduler;
    private final Iterator<TimeValue> backOffPolicy;

    public static class LineCursor {
        private URI uri;
        private long lineNumber;
        private String line;
        private IOException failure;

        public LineCursor() {
        }

        public LineCursor(URI uri, long lineNumber, @Nullable String line, @Nullable IOException failure) {
            this.uri = uri;
            this.lineNumber = lineNumber;
            this.line = line;
            this.failure = failure;
        }

        public URI uri() {
            return uri;
        }

        public long lineNumber() {
            return lineNumber;
        }

        @Nullable
        public String line() {
            return line;
        }

        @Nullable
        public IOException failure() {
            return failure;
        }

        @VisibleForTesting
        public LineCursor copy() {
            return new LineCursor(uri, lineNumber, line, failure);
        }

        @Override
        public String toString() {
            return "LineCursor{" + uri + ":" + lineNumber + ":line=" + line + ", failure=" + failure + "}";
        }

        @Override
        public int hashCode() {
            return Objects.hash(uri, lineNumber, line, failure);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            LineCursor other = (LineCursor) obj;
            return Objects.equals(uri, other.uri)
                && lineNumber == other.lineNumber
                && Objects.equals(line, other.line)
                && Objects.equals(failure, other.failure);
        }
    }

    public FileReadingIterator(Collection<URI> fileUris,
                               String compression,
                               Map<String, FileInputFactory> fileInputFactories,
                               Boolean shared,
                               int numReaders,
                               int readerNumber,
                               Settings withClauseOptions,
                               ScheduledExecutorService scheduler) {
        this.compressed = compression != null && compression.equalsIgnoreCase("gzip");
        this.fileInputFactories = fileInputFactories;
        this.cursor = new LineCursor();
        this.shared = shared;
        this.numReaders = numReaders;
        this.readerNumber = readerNumber;
        this.scheduler = scheduler;
        this.backOffPolicy = BackoffPolicy.exponentialBackoff(TimeValue.ZERO, MAX_SOCKET_TIMEOUT_RETRIES).iterator();

        this.fileInputs = fileUris.stream()
            .map(uri -> toFileInput(uri, withClauseOptions))
            .filter(Objects::nonNull)
            .toList();
        fileInputsIterator = fileInputs.iterator();
    }

    @Override
    public LineCursor currentElement() {
        return cursor;
    }

    @Override
    public void kill(@NotNull Throwable throwable) {
        killed = throwable;
    }

    @Override
    public void moveToStart() {
        raiseIfKilled();
        reset();
        watermark = 0;
        fileInputsIterator = fileInputs.iterator();
    }

    @Override
    public boolean moveNext() {
        raiseIfKilled();
        try {
            if (currentReader != null) {
                String line;
                try {
                    line = getLine(currentReader);
                } catch (SocketException | SocketTimeoutException e) {
                    if (backOffPolicy.hasNext()) {
                        return false;
                    }
                    throw e;
                }
                if (line == null) {
                    closeReader();
                    return moveNext();
                }
                cursor.line = line;
                cursor.failure = null;
                return true;
            } else if (currentInputUriIterator != null && currentInputUriIterator.hasNext()) {
                advanceToNextUri(currentInput);
                return moveNext();
            } else if (fileInputsIterator != null && fileInputsIterator.hasNext()) {
                advanceToNextFileInput();
                return moveNext();
            } else {
                reset();
                return false;
            }
        } catch (IOException e) {
            cursor.failure = e;
            closeReader();
            // If IOError happens on file opening, let consumers collect the error
            // This is mostly for RETURN SUMMARY of COPY FROM
            if (cursor.lineNumber == 0) {
                return true;
            }
            return moveNext();
        }
    }

    private void advanceToNextUri(FileInput fileInput) throws IOException {
        watermark = 0;
        createReader(fileInput, currentInputUriIterator.next());
    }

    private void advanceToNextFileInput() throws IOException {
        currentInput = fileInputsIterator.next();
        List<URI> uris = currentInput.expandUri().stream().filter(this::shouldBeReadByCurrentNode).toList();
        if (uris.size() > 0) {
            currentInputUriIterator = uris.iterator();
            advanceToNextUri(currentInput);
        } else if (currentInput.isGlobbed()) {
            URI uri = currentInput.uri();
            cursor.uri = uri;
            throw new IOException("Cannot find any URI matching: " + uri.toString());
        }
    }

    private boolean shouldBeReadByCurrentNode(URI uri) {
        boolean sharedStorage = Objects.requireNonNullElse(shared, currentInput.sharedStorageDefault());
        if (sharedStorage) {
            return moduloPredicateImpl(uri, this.readerNumber, this.numReaders);
        } else {
            return MATCH_ALL_PREDICATE.test(uri);
        }
    }

    private void createReader(FileInput fileInput, URI uri) throws IOException {
        cursor.uri = uri;
        cursor.lineNumber = 0;
        InputStream stream = fileInput.getStream(uri);
        currentReader = createBufferedReader(stream);
    }

    private void closeReader() {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (IOException e) {
                LOGGER.error("Unable to close reader for " + cursor.uri, e);
            }
            currentReader = null;
        }
    }

    private String getLine(BufferedReader reader) throws IOException {
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                cursor.lineNumber++;
                if (cursor.lineNumber < watermark) {
                    continue;
                } else {
                    watermark = 0;
                }
                if (line.length() == 0) {
                    continue;
                }
                break;
            }
        } catch (SocketException | SocketTimeoutException e) {
            if (backOffPolicy.hasNext()) {
                watermark = watermark == 0 ? cursor.lineNumber + 1 : watermark;
                closeReader();
                createReader(currentInput, cursor.uri);
            } else {
                URI uri = currentInput.uri();
                LOGGER.error("Timeout during COPY FROM '" + uri.toString() +
                             "' after " + MAX_SOCKET_TIMEOUT_RETRIES +
                             " retries", e);
            }
            throw e;
        } catch (Exception e) {
            URI uri = currentInput.uri();
            // it's nice to know which exact file/uri threw an error
            // when COPY FROM returns less rows than expected
            LOGGER.error("Error during COPY FROM '" + uri.toString() + "'", e);
            rethrowUnchecked(e);
        }
        return line;
    }

    @Override
    public void close() {
        closeReader();
        reset();
        killed = BatchIterator.CLOSED;
    }

    private void reset() {
        fileInputsIterator = null;
        currentInputUriIterator = null;
        currentInput = null;
        cursor.failure = null;
    }

    @Override
    public CompletableFuture<?> loadNextBatch() throws IOException {
        if (backOffPolicy.hasNext()) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            scheduler.schedule(
                (Runnable) () -> cf.complete(null), // cast to Runnable for enabling mockito tests
                backOffPolicy.next().millis(),
                TimeUnit.MILLISECONDS);
            return cf;
        } else {
            throw new IllegalStateException("All batches already loaded");
        }
    }

    @Override
    public boolean allLoaded() {
        return !backOffPolicy.hasNext();
    }

    @Override
    public boolean hasLazyResultSet() {
        return true;
    }

    @VisibleForTesting
    public static URI toURI(String fileUri) {
        if (fileUri.startsWith("/")) {
            // using Paths.get().toUri instead of new URI(...) as it also encodes umlauts and other special characters
            return Paths.get(fileUri).toUri();
        } else {
            URI uri = URI.create(fileUri);
            if (uri.getScheme() == null) {
                throw new IllegalArgumentException("relative fileURIs are not allowed");
            }
            if (uri.getScheme().equals("file") && !uri.getSchemeSpecificPart().startsWith("///")) {
                throw new IllegalArgumentException("Invalid fileURI");
            }
            return uri;
        }
    }

    @Nullable
    private FileInput toFileInput(URI uri, Settings withClauseOptions) {
        FileInputFactory fileInputFactory = fileInputFactories.get(uri.getScheme());
        if (fileInputFactory != null) {
            try {
                return fileInputFactory.create(uri, withClauseOptions);
            } catch (IOException e) {
                return null;
            }
        }
        return new URLFileInput(uri);
    }

    @VisibleForTesting
    BufferedReader createBufferedReader(InputStream inputStream) throws IOException {
        BufferedReader reader;
        if (compressed) {
            reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(inputStream),
                StandardCharsets.UTF_8));
        } else {
            reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        }
        return reader;
    }

    @VisibleForTesting
    public static boolean moduloPredicateImpl(URI input, int readerNumber, int numReaders) {
        int hash = input.hashCode();
        if (hash == Integer.MIN_VALUE) {
            hash = 0; // Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE
        }
        return Math.abs(hash) % numReaders == readerNumber;
    }

    private void raiseIfKilled() {
        if (killed != null) {
            Exceptions.rethrowUnchecked(killed);
        }
    }
}
