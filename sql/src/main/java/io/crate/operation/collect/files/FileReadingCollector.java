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
import io.crate.exceptions.JobKilledException;
import io.crate.jobs.KeepAliveListener;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.RowUpstream;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class FileReadingCollector implements CrateCollector, RowUpstream {

    private static final ESLogger LOGGER = Loggers.getLogger(FileReadingCollector.class);
    public static final int MAX_SOCKET_TIMEOUT_RETRIES = 5;
    private final Map<String, FileInputFactory> fileInputFactories;
    private final Boolean shared;
    private final int numReaders;
    private final int readerNumber;
    private final InputRow row;
    private final KeepAliveListener keepAliveListener;
    private final RowReceiver downstream;
    private final boolean compressed;
    private final List<LineCollectorExpression<?>> collectorExpressions;

    private static final Pattern HAS_GLOBS_PATTERN = Pattern.compile("(.*)[^\\\\]\\*.*");
    private static final Predicate<URI> MATCH_ALL_PREDICATE = new Predicate<URI>() {
        @Override
        public boolean apply(@Nullable URI input) {
            return true;
        }
    };
    private final List<UriWithGlob> fileUris;
    private volatile boolean killed;

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }

    /**
     * tells the RowUpstream that it should push all rows again
     */
    @Override
    public void repeat() {
        throw new UnsupportedOperationException();
    }

    public enum FileFormat {
        JSON
    }

    public FileReadingCollector(Collection<String> fileUris,
                                List<Input<?>> inputs,
                                List<LineCollectorExpression<?>> collectorExpressions,
                                RowReceiver downstream,
                                FileFormat format,
                                String compression,
                                Map<String, FileInputFactory> fileInputFactories,
                                Boolean shared,
                                KeepAliveListener keepAliveListener,
                                int numReaders,
                                int readerNumber) {
        this.keepAliveListener = keepAliveListener;
        this.fileUris = getUrisWithGlob(fileUris);
        this.downstream = downstream;
        downstream.setUpstream(this);
        this.compressed = compression != null && compression.equalsIgnoreCase("gzip");
        this.row = new InputRow(inputs);
        this.collectorExpressions = collectorExpressions;
        this.fileInputFactories = fileInputFactories;
        this.shared = shared;
        this.numReaders = numReaders;
        this.readerNumber = readerNumber;
    }

    private static class UriWithGlob {
        final URI uri;
        final URI preGlobUri;
        @Nullable  final Predicate<URI> globPredicate;

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
                preGlobUri = URI.create(hasGlobMatcher.group(1));
                globPredicate = new GlobPredicate(uri);
            }

            uris.add(new UriWithGlob(uri, preGlobUri, globPredicate));
        }
        return uris;
    }

    private static URI toURI(String fileUri) {
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

    @Override
    public void doCollect() {
        CollectorContext collectorContext = new CollectorContext();
        for (LineCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }

        for (UriWithGlob fileUri : fileUris) {
            FileInput fileInput;
            try {
                fileInput = getFileInput(fileUri.uri);
            } catch (IOException e) {
                downstream.fail(e);
                return;
            }

            Predicate<URI> uriPredicate = generateUriPredicate(fileInput, fileUri.globPredicate);
            List<URI> uris;
            try {
                uris = getUris(fileInput, fileUri.uri, fileUri.preGlobUri, uriPredicate);
                for (URI uri : uris) {
                    readLines(fileInput, collectorContext, uri, 0, 0);
                }
            } catch (Throwable e) {
                downstream.fail(e);
                return;
            }
        }
        downstream.finish();
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        killed = true;
    }

    private void readLines(FileInput fileInput,
                           CollectorContext collectorContext,
                           URI uri,
                           long startLine,
                           int retry) throws IOException {
        InputStream inputStream = fileInput.getStream(uri);
        if (inputStream == null) {
            return;
        }

        String line;
        long linesRead = 0L;
        int keepAliveCount = 0;
        try (BufferedReader reader = createReader(inputStream)) {
            while ((line = reader.readLine()) != null) {
                if (killed) {
                    throw new CancellationException(JobKilledException.MESSAGE);
                }

                keepAliveCount++;
                if (keepAliveCount > 100_00) {
                    keepAliveListener.keepAlive();
                    keepAliveCount = 0;
                }

                linesRead++;
                if (linesRead < startLine) {
                    continue;
                }
                if (line.length() == 0) { // skip empty lines
                    continue;
                }
                collectorContext.lineContext().rawSource(line.getBytes(StandardCharsets.UTF_8));
                if (!downstream.setNextRow(row)) {
                    break;
                }
            }
        } catch (SocketTimeoutException e) {
            if (retry > MAX_SOCKET_TIMEOUT_RETRIES) {
                LOGGER.info("Timeout during COPY FROM '{}' after {} retries", e, uri.toString(), retry);
                throw e;
            } else {
                readLines(fileInput, collectorContext, uri, linesRead + 1, retry + 1);
            }
        } catch (Exception e) {
            // it's nice to know which exact file/uri threw an error
            // when COPY FROM returns less rows than expected
            LOGGER.info("Error during COPY FROM '{}'", e, uri.toString());
            throw e;
        }
    }

    private BufferedReader createReader(InputStream inputStream) throws IOException {
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
            moduloPredicate = new Predicate<URI>() {
                @Override
                public boolean apply(URI input) {
                    int hash = input.hashCode();
                    if (hash == Integer.MIN_VALUE) {
                        hash = 0; // Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE
                    }
                    return Math.abs(hash) % numReaders == readerNumber;
                }
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

        public GlobPredicate(URI fileUri) {
            this.globPattern = Pattern.compile(Globs.toUnixRegexPattern(fileUri.toString()));
        }

        @Override
        public boolean apply(@Nullable URI input) {
            return input != null && globPattern.matcher(input.toString()).matches();
        }
    }
}
