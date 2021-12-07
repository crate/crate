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

import io.crate.analyze.CopyFromParserProperties;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Tuple;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.exceptions.Exceptions;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.expression.InputRow;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static io.crate.exceptions.Exceptions.rethrowUnchecked;

public class FileReadingIterator implements BatchIterator<Row> {

    private static final Logger LOGGER = LogManager.getLogger(FileReadingIterator.class);
    private static final int MAX_SOCKET_TIMEOUT_RETRIES = 5;
    private final Map<String, FileInputFactory> fileInputFactories;
    private final Boolean shared;
    private final int numReaders;
    private final int readerNumber;
    private final boolean compressed;
    @Nullable
    private final String protocolSetting;
    private static final Pattern HAS_GLOBS_PATTERN = Pattern.compile("^((s3://|file://|/)[^\\*]*/)[^\\*]*\\*.*");
    private static final Predicate<URI> MATCH_ALL_PREDICATE = (URI input) -> true;

    private final Collection<URI> uris;
    @VisibleForTesting
    final List<Tuple<FileInput, UriWithGlob>> fileInputsToUriWithGlobs;
    private final Iterable<LineCollectorExpression<?>> collectorExpressions;

    private volatile Throwable killed;
    private final CopyFromParserProperties parserProperties;
    private final FileUriCollectPhase.InputFormat inputFormat;
    private Iterator<Tuple<FileInput, UriWithGlob>> fileInputsIterator = null;
    private Tuple<FileInput, UriWithGlob> currentInput = null;
    private Iterator<URI> currentInputIterator = null;
    private URI currentUri;
    private BufferedReader currentReader = null;
    private long currentLineNumber;
    private final Row row;
    private LineProcessor lineProcessor;

    private FileReadingIterator(Collection<String> fileUris,
                                List<? extends Input<?>> inputs,
                                Iterable<LineCollectorExpression<?>> collectorExpressions,
                                String compression,
                                Map<String, FileInputFactory> fileInputFactories,
                                Boolean shared,
                                int numReaders,
                                int readerNumber,
                                CopyFromParserProperties parserProperties,
                                FileUriCollectPhase.InputFormat inputFormat,
                                @Nullable String protocolSetting) {
        this.compressed = compression != null && compression.equalsIgnoreCase("gzip");
        this.row = new InputRow(inputs);
        this.fileInputFactories = fileInputFactories;
        this.shared = shared;
        this.numReaders = numReaders;
        this.readerNumber = readerNumber;
        this.uris = fileUris.stream().map(FileReadingIterator::toURI).collect(Collectors.toList());
        this.collectorExpressions = collectorExpressions;
        this.parserProperties = parserProperties;
        this.inputFormat = inputFormat;
        this.fileInputsToUriWithGlobs = new ArrayList<>();
        initFileInputs();
        initCollectorState();
        this.protocolSetting = protocolSetting;
    }

    @Override
    public Row currentElement() {
        return row;
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        killed = throwable;
    }

    public static BatchIterator<Row> newInstance(Collection<String> fileUris,
                                                 List<Input<?>> inputs,
                                                 Iterable<LineCollectorExpression<?>> collectorExpressions,
                                                 String compression,
                                                 Map<String, FileInputFactory> fileInputFactories,
                                                 Boolean shared,
                                                 int numReaders,
                                                 int readerNumber,
                                                 CopyFromParserProperties parserProperties,
                                                 FileUriCollectPhase.InputFormat inputFormat,
                                                 @Nullable String protocolSetting) {
        return new FileReadingIterator(
            fileUris,
            inputs,
            collectorExpressions,
            compression,
            fileInputFactories,
            shared,
            numReaders,
            readerNumber,
            parserProperties,
            inputFormat,
            protocolSetting);
    }

    private void initFileInputs() {
        for (URI uri : uris) {
            FileInput fileInput = getFileInput(uri);
            if (fileInput != null) {
                fileInputsToUriWithGlobs.add(new Tuple<>(fileInput, toUriWithGlob(uri, fileInput.uriFormatter())));
            }
        }
    }

    private void initCollectorState() {
        lineProcessor = new LineProcessor(parserProperties);
        lineProcessor.startCollect(collectorExpressions);
        fileInputsIterator = fileInputsToUriWithGlobs.iterator();
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
            } else if (currentInputIterator != null && currentInputIterator.hasNext()) {
                advanceToNextUri(currentInput.v1());
                return moveNext();
            } else if (fileInputsIterator != null && fileInputsIterator.hasNext()) {
                advanceToNextFileInput();
                return moveNext();
            } else {
                releaseBatchIteratorState();
                return false;
            }
        } catch (IOException e) {
            lineProcessor.setFailure(e.getMessage());
            return true;
        }
    }

    private void advanceToNextUri(FileInput fileInput) throws IOException {
        currentUri = currentInputIterator.next();
        initCurrentReader(fileInput, currentUri);
    }

    private void advanceToNextFileInput() throws IOException {
        currentInput = fileInputsIterator.next();
        FileInput fileInput = currentInput.v1();
        UriWithGlob fileUri = currentInput.v2();
        Predicate<URI> uriPredicate = generateUriPredicate(fileInput, fileUri.globPredicate);
        List<URI> uris = getUris(fileInput, fileUri.uri, fileUri.preGlobUri, uriPredicate);

        if (uris.size() > 0) {
            currentInputIterator = uris.iterator();
            advanceToNextUri(fileInput);
        } else if (fileUri.preGlobUri != null) {
            lineProcessor.startWithUri(fileUri.uri);
            throw new IOException("Cannot find any URI matching: " + fileUri.uri.toString());
        }
    }

    private void initCurrentReader(FileInput fileInput, URI uri) throws IOException {
        lineProcessor.startWithUri(uri);
        InputStream stream = fileInput.getStream(uri, protocolSetting);
        currentReader = createBufferedReader(stream);
        currentLineNumber = 0;
        lineProcessor.readFirstLine(currentUri, inputFormat, currentReader);
    }

    private void closeCurrentReader() {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (IOException e) {
                LOGGER.error("Unable to close reader for {}", e, currentUri);
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
                URI uri = currentInput.v2().uri;
                LOGGER.info("Timeout during COPY FROM '{}' after {} retries", e, uri.toString(), retry);
                throw e;
            } else {
                long startLine = currentLineNumber + 1;
                closeCurrentReader();
                initCurrentReader(currentInput.v1(), currentUri);
                return getLine(currentReader, startLine, retry + 1);
            }
        } catch (Exception e) {
            URI uri = currentInput.v2().uri;
            // it's nice to know which exact file/uri threw an error
            // when COPY FROM returns less rows than expected
            LOGGER.info("Error during COPY FROM '{}'", e, uri.toString());
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
        currentInputIterator = null;
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
    static class UriWithGlob {
        final URI uri;
        final URI preGlobUri;
        @Nullable
        final Predicate<URI> globPredicate;

        UriWithGlob(URI uri, URI preGlobUri, Predicate<URI> globPredicate) {
            this.uri = uri;
            this.preGlobUri = preGlobUri;
            this.globPredicate = globPredicate;
        }
    }

    @Nullable
    private static UriWithGlob toUriWithGlob(URI fileUri, @Nullable Function<String, URI> uriFormatter) {
        if (uriFormatter != null) {
            fileUri = uriFormatter.apply(fileUri.toString());
        } else {
            uriFormatter = FileReadingIterator::toURI;
        }
        String formattedUriStr = fileUri.toString();
        URI preGlobUri = null;
        Predicate<URI> globPredicate = null;
        Matcher hasGlobMatcher = HAS_GLOBS_PATTERN.matcher(formattedUriStr);
        /*
         * hasGlobMatcher.group(1) returns part of the path before the wildcards with a trailing backslash,
         * ex)
         *      'file:///bucket/prefix/*.json'                           -> 'file:///bucket/prefix/'
         *      's3://bucket/year=2020/month=12/day=*0/hour=12/*.json'   -> 's3://bucket/year=2020/month=12/'
         */
        if (hasGlobMatcher.matches()) {
            if (formattedUriStr.startsWith("/") || formattedUriStr.startsWith("file://")) {
                Path oldPath = Paths.get(uriFormatter.apply(hasGlobMatcher.group(1)));
                String oldPathAsString;
                String newPathAsString;
                try {
                    oldPathAsString = oldPath.toUri().toString();
                    newPathAsString = oldPath.toRealPath().toUri().toString();
                } catch (IOException e) {
                    return null;
                }
                //resolve any links
                String resolvedFileUrl = formattedUriStr.replace(oldPathAsString, newPathAsString);
                fileUri = uriFormatter.apply(resolvedFileUrl);
                preGlobUri = uriFormatter.apply(newPathAsString);
            } else {
                preGlobUri = URI.create(hasGlobMatcher.group(1));
            }
            globPredicate = new GlobPredicate(fileUri);
        }
        return new UriWithGlob(fileUri, preGlobUri, globPredicate);
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
    private FileInput getFileInput(URI fileUri) {
        FileInputFactory fileInputFactory = fileInputFactories.get(fileUri.getScheme());
        if (fileInputFactory != null) {
            return fileInputFactory.create();
        }
        return new URLFileInput(fileUri);
    }

    private BufferedReader createBufferedReader(InputStream inputStream) throws IOException {
        BufferedReader reader;
        if (compressed) {
            reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(inputStream),
                StandardCharsets.UTF_8));
        } else {
            reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        }
        return reader;
    }

    private List<URI> getUris(FileInput fileInput, URI fileUri, URI preGlobUri, Predicate<URI> uriPredicate) throws IOException {
        List<URI> uris;
        if (preGlobUri != null) {
            uris = fileInput.listUris(fileUri, preGlobUri, uriPredicate, protocolSetting);
        } else if (uriPredicate.test(fileUri)) {
            uris = List.of(fileUri);
        } else {
            uris = List.of();
        }
        return uris;
    }

    private Predicate<URI> generateUriPredicate(FileInput fileInput, @Nullable Predicate<URI> globPredicate) {
        Predicate<URI> moduloPredicate;
        boolean sharedStorage = Objects.requireNonNullElse(shared, fileInput.sharedStorageDefault());
        if (sharedStorage) {
            moduloPredicate = input -> moduloPredicateImpl(input, this.readerNumber, this.numReaders);
        } else {
            moduloPredicate = MATCH_ALL_PREDICATE;
        }

        if (globPredicate != null) {
            return moduloPredicate.and(globPredicate);
        }
        return moduloPredicate;
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

    @VisibleForTesting
    static class GlobPredicate implements Predicate<URI> {
        private final Pattern globPattern;

        GlobPredicate(URI fileUri) {
            this.globPattern = Pattern.compile(Globs.toUnixRegexPattern(fileUri.toString()));
        }

        @Override
        public boolean test(@Nullable URI input) {
            return input != null && globPattern.matcher(input.toString()).matches();
        }
    }
}
