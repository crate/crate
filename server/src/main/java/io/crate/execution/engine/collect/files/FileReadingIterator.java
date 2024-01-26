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

import static io.crate.exceptions.Exceptions.rethrowUnchecked;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import java.util.function.Predicate;
import java.util.zip.GZIPInputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.exceptions.Exceptions;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.expression.InputRow;

public class FileReadingIterator implements BatchIterator<Row> {

    private static final Logger LOGGER = LogManager.getLogger(FileReadingIterator.class);
    @VisibleForTesting
    static final int MAX_SOCKET_TIMEOUT_RETRIES = 5;

    public static BatchIterator<Row> newInstance(Collection<URI> fileUris,
                                                 List<Input<?>> inputs,
                                                 Iterable<LineCollectorExpression<?>> collectorExpressions,
                                                 String compression,
                                                 Map<String, FileInputFactory> fileInputFactories,
                                                 Boolean shared,
                                                 int numReaders,
                                                 int readerNumber,
                                                 List<String> targetColumns,
                                                 CopyFromParserProperties parserProperties,
                                                 FileUriCollectPhase.InputFormat inputFormat,
                                                 Settings withClauseOptions) {
        return new FileReadingIterator(
            fileUris,
            inputs,
            collectorExpressions,
            compression,
            fileInputFactories,
            shared,
            numReaders,
            readerNumber,
            targetColumns,
            parserProperties,
            inputFormat,
            withClauseOptions);
    }


    private final Map<String, FileInputFactory> fileInputFactories;
    private final Boolean shared;
    private final int numReaders;
    private final int readerNumber;
    private final boolean compressed;
    private static final Predicate<URI> MATCH_ALL_PREDICATE = (URI input) -> true;
    private final List<FileInput> fileInputs;

    private final Iterable<LineCollectorExpression<?>> collectorExpressions;

    private volatile Throwable killed;
    private final List<String> targetColumns;
    private final CopyFromParserProperties parserProperties;
    private final FileUriCollectPhase.InputFormat inputFormat;
    private Iterator<FileInput> fileInputsIterator = null;
    private FileInput currentInput = null;
    private Iterator<URI> currentInputUriIterator = null;
    private URI currentUri;
    private BufferedReader currentReader = null;
    private long currentLineNumber;
    private final Row row;
    private LineProcessor lineProcessor;

    @VisibleForTesting
    FileReadingIterator(Collection<URI> fileUris,
                        List<? extends Input<?>> inputs,
                        Iterable<LineCollectorExpression<?>> collectorExpressions,
                        String compression,
                        Map<String, FileInputFactory> fileInputFactories,
                        Boolean shared,
                        int numReaders,
                        int readerNumber,
                        List<String> targetColumns,
                        CopyFromParserProperties parserProperties,
                        FileUriCollectPhase.InputFormat inputFormat,
                        Settings withClauseOptions) {
        this.compressed = compression != null && compression.equalsIgnoreCase("gzip");
        this.row = new InputRow(inputs);
        this.fileInputFactories = fileInputFactories;
        this.shared = shared;
        this.numReaders = numReaders;
        this.readerNumber = readerNumber;
        this.fileInputs = fileUris.stream().map(uri -> toFileInput(uri, withClauseOptions)).filter(Objects::nonNull).toList();
        this.collectorExpressions = collectorExpressions;
        this.targetColumns = targetColumns;
        this.parserProperties = parserProperties;
        this.inputFormat = inputFormat;
        initCollectorState();
    }

    @Override
    public Row currentElement() {
        return row;
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        killed = throwable;
    }

    private void initCollectorState() {
        lineProcessor = new LineProcessor(parserProperties, targetColumns);
        lineProcessor.startCollect(collectorExpressions);
        fileInputsIterator = fileInputs.iterator();
    }

    @Override
    public void moveToStart() {
        raiseIfKilled();
        initCollectorState();
    }

    @Override
    public boolean moveNext() {
        raiseIfKilled();
        try {
            if (currentReader != null) {
                String line = getLine(currentReader, currentLineNumber, 0);
                if (line == null) {
                    closeCurrentReader();
                    return moveNext();
                }
                lineProcessor.process(line);
                return true;
            } else if (currentInputUriIterator != null && currentInputUriIterator.hasNext()) {
                advanceToNextUri(currentInput);
                return moveNext();
            } else if (fileInputsIterator != null && fileInputsIterator.hasNext()) {
                advanceToNextFileInput();
                return moveNext();
            } else {
                releaseBatchIteratorState();
                return false;
            }
        } catch (IOException e) {
            lineProcessor.setUriFailure(e.getMessage());
            closeCurrentReader();
            // If the error happens on the first line, return true so that the error is collected by {@link #currentElement()}
            if (currentLineNumber == 0) {
                return true;
            }
            return moveNext();
        } catch (Exception e) {
            lineProcessor.setParsingFailure(e.getMessage());
            return true;
        }
    }

    private void advanceToNextUri(FileInput fileInput) throws IOException {
        currentUri = currentInputUriIterator.next();
        initCurrentReader(fileInput, currentUri);
    }

    private void advanceToNextFileInput() throws IOException {
        currentInput = fileInputsIterator.next();
        List<URI> uris = currentInput.expandUri().stream().filter(this::shouldBeReadByCurrentNode).toList();
        if (uris.size() > 0) {
            currentInputUriIterator = uris.iterator();
            advanceToNextUri(currentInput);
        } else if (currentInput.isGlobbed()) {
            lineProcessor.startWithUri(currentInput.uri());
            throw new IOException("Cannot find any URI matching: " + currentInput.uri().toString());
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

    private void initCurrentReader(FileInput fileInput, URI uri) throws IOException {
        lineProcessor.startWithUri(uri);
        InputStream stream = fileInput.getStream(uri);
        currentReader = createBufferedReader(stream);
        currentLineNumber = 0;
        lineProcessor.readFirstLine(currentUri, inputFormat, currentReader);
    }

    private void closeCurrentReader() {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (IOException e) {
                LOGGER.error("Unable to close reader for " + currentUri, e);
            }
            currentReader = null;
        }
    }

    private String getLine(BufferedReader reader, long startFrom, int retry) throws IOException {
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                currentLineNumber++;
                if (currentLineNumber < startFrom) {
                    continue;
                }
                if (line.length() == 0) {
                    continue;
                }
                break;
            }
        } catch (SocketTimeoutException e) {
            if (retry > MAX_SOCKET_TIMEOUT_RETRIES) {
                URI uri = currentInput.uri();
                LOGGER.error("Timeout during COPY FROM '" + uri.toString() + "' after " + retry + " retries", e);
                throw e;
            } else {
                long startLine = retry == 0 ? currentLineNumber + 1 : startFrom;
                closeCurrentReader();
                initCurrentReader(currentInput, currentUri);
                return getLine(currentReader, startLine, retry + 1);
            }
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
        closeCurrentReader();
        releaseBatchIteratorState();
        killed = BatchIterator.CLOSED;
    }

    private void releaseBatchIteratorState() {
        fileInputsIterator = null;
        currentInputUriIterator = null;
        currentInput = null;
        currentUri = null;
    }

    @Override
    public CompletableFuture<?> loadNextBatch() {
        throw new IllegalStateException("All batches already loaded");
    }

    @Override
    public boolean allLoaded() {
        return true;
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
