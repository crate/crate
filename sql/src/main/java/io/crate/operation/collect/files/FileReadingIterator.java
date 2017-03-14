/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect.files;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchIterator;
import io.crate.data.CloseAssertingBatchIterator;
import io.crate.data.Columns;
import io.crate.data.Input;
import io.crate.operation.reference.file.LineContext;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static io.crate.exceptions.Exceptions.rethrowUnchecked;

public class FileReadingIterator implements BatchIterator {

    private static final ESLogger LOGGER = Loggers.getLogger(FileReadingIterator.class);
    private static final int MAX_SOCKET_TIMEOUT_RETRIES = 5;
    private final Map<String, FileInputFactory> fileInputFactories;
    private final Boolean shared;
    private final int numReaders;
    private final int readerNumber;
    private final boolean compressed;

    private static final Pattern HAS_GLOBS_PATTERN = Pattern.compile("(.*)[^\\\\]\\*.*");
    private static final Predicate<URI> MATCH_ALL_PREDICATE = (URI input) -> true;

    private final List<UriWithGlob> urisWithGlob;
    private final Iterable<LineCollectorExpression<?>> collectorExpressions;
    private Iterator<Tuple<FileInput, UriWithGlob>> fileInputsIterator = null;
    private Tuple<FileInput, UriWithGlob> currentInput = null;
    private Iterator<URI> currentInputIterator = null;
    private URI currentUri;
    private BufferedReader currentReader = null;
    private long currentLineNumber;
    private LineContext lineContext;
    private final Columns inputs;

    private volatile Throwable killed;

    private FileReadingIterator(Collection<String> fileUris,
                                List<? extends Input<?>> inputs,
                                Iterable<LineCollectorExpression<?>> collectorExpressions,
                                String compression,
                                Map<String, FileInputFactory> fileInputFactories,
                                Boolean shared,
                                int numReaders,
                                int readerNumber) {
        this.compressed = compression != null && compression.equalsIgnoreCase("gzip");
        this.inputs = Columns.wrap(inputs.stream().map(ExceptionHandlingInputProxy::new).collect(Collectors.toList()));
        this.fileInputFactories = fileInputFactories;
        this.shared = shared;
        this.numReaders = numReaders;
        this.readerNumber = readerNumber;
        this.urisWithGlob = getUrisWithGlob(fileUris);
        this.collectorExpressions = collectorExpressions;
        initCollectorState();
    }

    @Override
    public Columns rowData() {
        return inputs;
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        // handled by CloseAssertingBatchIterator
    }

    private final class ExceptionHandlingInputProxy<T> implements Input<T> {

        private final Input<T> input;

        ExceptionHandlingInputProxy(Input<T> input) {
            this.input = input;
        }

        @Override
        public T value() {
            try {
                return this.input.value();
            } catch (ElasticsearchParseException e) {
                throw new ElasticsearchParseException(String.format(Locale.ENGLISH,
                    "Failed to parse JSON in line: %d in file: \"%s\"%n" +
                    "Original error message: %s", currentLineNumber, currentUri, e.getMessage()), e);
            }
        }
    }

    public static BatchIterator newInstance(Collection<String> fileUris,
                                            List<Input<?>> inputs,
                                            Iterable<LineCollectorExpression<?>> collectorExpressions,
                                            String compression,
                                            Map<String, FileInputFactory> fileInputFactories,
                                            Boolean shared,
                                            int numReaders,
                                            int readerNumber) {
        return new CloseAssertingBatchIterator(new FileReadingIterator(fileUris, inputs, collectorExpressions,
            compression, fileInputFactories, shared, numReaders, readerNumber));
    }

    private void initCollectorState() {
        lineContext = new LineContext();
        for (LineCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(lineContext);
        }
        List<Tuple<FileInput, UriWithGlob>> fileInputs = new ArrayList<>(urisWithGlob.size());
        for (UriWithGlob fileUri : urisWithGlob) {
            try {
                FileInput fileInput = getFileInput(fileUri.uri);
                fileInputs.add(new Tuple<>(fileInput, fileUri));
            } catch (IOException e) {
                rethrowUnchecked(e);
            }
        }
        fileInputsIterator = fileInputs.iterator();
    }

    @Override
    public void moveToStart() {
        initCollectorState();
    }

    @Override
    public boolean moveNext() {
        try {
            if (currentReader != null) {
                String line = getLine(currentReader, currentLineNumber, 0);
                if (line == null) {
                    closeCurrentReader();
                    return moveNext();
                } else {
                    lineContext.rawSource(line.getBytes(StandardCharsets.UTF_8));
                    return true;
                }
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
            rethrowUnchecked(e);
        }
        return false;
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
        }
    }

    private void initCurrentReader(FileInput fileInput, URI uri) throws IOException {
        InputStream stream = fileInput.getStream(uri);
        if (stream != null) {
            currentReader = createBufferedReader(stream);
            currentLineNumber = 0;
        }
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
    }

    private void releaseBatchIteratorState() {
        fileInputsIterator = null;
        currentInputIterator = null;
        currentInput = null;
        currentUri = null;
    }

    @Override
    public CompletableFuture<?> loadNextBatch() {
        return CompletableFutures.failedFuture(new IllegalStateException("All batches already loaded"));
    }

    @Override
    public boolean allLoaded() {
        return true;
    }

    private static class UriWithGlob {
        final URI uri;
        final URI preGlobUri;
        @Nullable
        final Predicate<URI> globPredicate;

        public UriWithGlob(URI uri, URI preGlobUri, Predicate<URI> globPredicate) {
            this.uri = uri;
            this.preGlobUri = preGlobUri;
            this.globPredicate = globPredicate;
        }
    }

    private List<UriWithGlob> getUrisWithGlob(Collection<String> fileUris) {
        List<UriWithGlob> uris = new ArrayList<>(fileUris.size());
        for (String fileUri : fileUris) {
            URI uri = toURI(fileUri);

            URI preGlobUri = null;
            Predicate<URI> globPredicate = null;
            Matcher hasGlobMatcher = HAS_GLOBS_PATTERN.matcher(uri.toString());
            if (hasGlobMatcher.matches()) {
                if (fileUri.startsWith("/") || fileUri.startsWith("file://")) {
                    /*
                     * Substitute a symlink with the real path.
                     * The wildcard needs to be maintained, though, because it is used to generate the matcher.
                     * Take the part before the wildcard (*) and try to resolved the real path.
                     * If the part before the wildcard contains a part of the filename (e.g. /tmp/foo_*.json) then use the
                     * parent directory of this filename to resolved the real path.
                     * Then replace this part with the real path and generate the URI.
                     */
                    Path oldPath = Paths.get(toURI(hasGlobMatcher.group(1)));
                    if (!Files.isDirectory(oldPath)) {
                        oldPath = oldPath.getParent();
                    }
                    String oldPathAsString;
                    String newPathAsString;
                    try {
                        oldPathAsString = oldPath.toUri().toString();
                        newPathAsString = oldPath.toRealPath().toUri().toString();
                    } catch (IOException e) {
                        continue;
                    }
                    String resolvedFileUrl = uri.toString().replace(oldPathAsString, newPathAsString);
                    uri = toURI(resolvedFileUrl);
                    preGlobUri = toURI(newPathAsString);
                } else {
                    preGlobUri = URI.create(hasGlobMatcher.group(1));
                }
                globPredicate = new GlobPredicate(uri);
            }

            uris.add(new UriWithGlob(uri, preGlobUri, globPredicate));
        }
        return uris;
    }

    private URI toURI(String fileUri) {
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
    private FileInput getFileInput(URI fileUri) throws IOException {
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

    private static List<URI> getUris(FileInput fileInput, URI fileUri, URI preGlobUri, Predicate<URI> uriPredicate) throws IOException {
        List<URI> uris;
        if (preGlobUri != null) {
            uris = fileInput.listUris(preGlobUri, uriPredicate);
        } else if (uriPredicate.apply(fileUri)) {
            uris = ImmutableList.of(fileUri);
        } else {
            uris = ImmutableList.of();
        }
        return uris;
    }

    private Predicate<URI> generateUriPredicate(FileInput fileInput, @Nullable Predicate<URI> globPredicate) {
        Predicate<URI> moduloPredicate;
        boolean sharedStorage = MoreObjects.firstNonNull(shared, fileInput.sharedStorageDefault());
        if (sharedStorage) {
            moduloPredicate = input -> {
                int hash = input.hashCode();
                if (hash == Integer.MIN_VALUE) {
                    hash = 0; // Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE
                }
                return Math.abs(hash) % numReaders == readerNumber;
            };
        } else {
            moduloPredicate = MATCH_ALL_PREDICATE;
        }

        if (globPredicate != null) {
            return Predicates.and(moduloPredicate, globPredicate);
        }
        return moduloPredicate;
    }

    private static class GlobPredicate implements Predicate<URI> {
        private final Pattern globPattern;

        GlobPredicate(URI fileUri) {
            this.globPattern = Pattern.compile(Globs.toUnixRegexPattern(fileUri.toString()));
        }

        @Override
        public boolean apply(@Nullable URI input) {
            return input != null && globPattern.matcher(input.toString()).matches();
        }
    }
}
