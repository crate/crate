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
import com.google.common.collect.ImmutableMap;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class FileReadingCollector implements CrateCollector, RowUpstream {

    private static final ESLogger LOGGER = Loggers.getLogger(FileReadingCollector.class);
    public static final int MAX_SOCKET_TIMEOUT_RETRIES = 5;
    private final Map<String, FileInputFactory> fileInputFactoryMap;
    private final URI fileUri;
    private final Predicate<URI> globPredicate;
    private final Boolean shared;
    private final int numReaders;
    private final int readerNumber;
    private final InputRow row;
    private final KeepAliveListener keepAliveListener;
    private URI preGlobUri;
    private RowReceiver downstream;
    private final boolean compressed;
    private final List<LineCollectorExpression<?>> collectorExpressions;

    private static final Pattern HAS_GLOBS_PATTERN = Pattern.compile("(.*)[^\\\\]\\*.*");
    private static final Predicate<URI> MATCH_ALL_PREDICATE = new Predicate<URI>() {
        @Override
        public boolean apply(@Nullable URI input) {
            return true;
        }
    };
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

    public FileReadingCollector(String fileUri,
                                List<Input<?>> inputs,
                                List<LineCollectorExpression<?>> collectorExpressions,
                                RowReceiver downstream,
                                FileFormat format,
                                String compression,
                                Map<String, FileInputFactory> additionalFileInputFactories,
                                Boolean shared,
                                KeepAliveListener keepAliveListener,
                                int numReaders,
                                int readerNumber) {
        this.keepAliveListener = keepAliveListener;
        if (fileUri.startsWith("/")) {
            // using Paths.get().toUri instead of new URI(...) as it also encodes umlauts and other special characters
            this.fileUri = Paths.get(fileUri).toUri();
        } else {
            this.fileUri = URI.create(fileUri);
            if (this.fileUri.getScheme() == null) {
                throw new IllegalArgumentException("relative fileURIs are not allowed");
            }
            if (this.fileUri.getScheme().equals("file") && !this.fileUri.getSchemeSpecificPart().startsWith("///")) {
                throw new IllegalArgumentException("Invalid fileURI");
            }
            if (!this.fileUri.getScheme().equals("file") && !this.fileUri.getScheme().equals("s3") ) {
                throw new IllegalArgumentException("URI scheme is not supported");
            }
        }
        this.downstream = downstream;
        downstream.setUpstream(this);
        this.compressed = compression != null && compression.equalsIgnoreCase("gzip");
        this.row = new InputRow(inputs);
        this.collectorExpressions = collectorExpressions;
        this.fileInputFactoryMap = new HashMap<>(ImmutableMap.of(
                "s3", new FileInputFactory() {
                    @Override
                    public FileInput create() throws IOException {
                        return new S3FileInput();
                    }
                },
                "file", new FileInputFactory() {

                    @Override
                    public FileInput create() throws IOException {
                        return new LocalFsFileInput();
                    }
                }
        ));
        this.fileInputFactoryMap.putAll(additionalFileInputFactories);
        this.shared = shared;
        this.numReaders = numReaders;
        this.readerNumber = readerNumber;
        Matcher hasGlobMatcher = HAS_GLOBS_PATTERN.matcher(this.fileUri.toString());
        if (!hasGlobMatcher.matches()) {
            globPredicate = null;
        } else {
            this.preGlobUri = URI.create(hasGlobMatcher.group(1));
            final Pattern globPattern = Pattern.compile(Globs.toUnixRegexPattern(this.fileUri.toString()));
            globPredicate = new Predicate<URI>() {
                @Override
                public boolean apply(URI input) {
                    return globPattern.matcher(input.toString()).matches();
                }
            };
        }
    }

    @Nullable
    private FileInput getFileInput() throws IOException {
        FileInputFactory fileInputFactory = fileInputFactoryMap.get(fileUri.getScheme());
        if (fileInputFactory != null) {
            return fileInputFactory.create();
        }
        return null;
    }

    @Override
    public void doCollect() {
        FileInput fileInput;
        try {
            fileInput = getFileInput();
        } catch (IOException e) {
            downstream.fail(e);
            return;
        }
        if (fileInput == null) {
            if (downstream != null) {
                downstream.finish();
            }
            return;
        }
        Predicate<URI> uriPredicate = generateUriPredicate(fileInput);

        CollectorContext collectorContext = new CollectorContext();
        for (LineCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }
        List<URI> uris;

        try {
            uris = getUris(fileInput, uriPredicate);
            for (URI uri : uris) {
                readLines(fileInput, collectorContext, uri, 0, 0);
            }
            downstream.finish();
        } catch (Throwable e) {
            downstream.fail(e);
        }
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
                    throw new CancellationException();
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

    private List<URI> getUris(FileInput fileInput, Predicate<URI> uriPredicate) throws IOException {
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

    private Predicate<URI> generateUriPredicate(FileInput fileInput) {
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

}
