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
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.collect.CollectionAbortedException;
import io.crate.operation.collect.CrateCollector;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class FileReadingCollector implements CrateCollector {

    private final Map<String, FileInputFactory> fileInputFactoryMap;
    private final URI fileUri;
    private final Predicate<URI> globPredicate;
    private final Boolean shared;
    private final int numReaders;
    private final int readerNumber;
    private final InputRow row;
    private URI preGlobUri;
    private RowDownstreamHandle downstream;
    private final boolean compressed;
    private final List<LineCollectorExpression<?>> collectorExpressions;

    private static final Pattern HAS_GLOBS_PATTERN = Pattern.compile("(.*)[^\\\\]\\*.*");
    private static final Predicate<URI> MATCH_ALL_PREDICATE = new Predicate<URI>() {
        @Override
        public boolean apply(@Nullable URI input) {
            return true;
        }
    };

    public enum FileFormat {
        JSON
    }

    public FileReadingCollector(String fileUri,
                                List<Input<?>> inputs,
                                List<LineCollectorExpression<?>> collectorExpressions,
                                RowDownstream downstream,
                                FileFormat format,
                                String compression,
                                Map<String, FileInputFactory> additionalFileInputFactories,
                                Boolean shared,
                                int numReaders,
                                int readerNumber) {
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
        this.downstream = downstream.registerUpstream(this);
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
    public void doCollect(RamAccountingContext ramAccountingContext) throws IOException {
        FileInput fileInput = getFileInput();
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
        String line;
        List<URI> uris;
        uris = getUris(fileInput, uriPredicate);
        try {
            for (URI uri : uris) {
                InputStream inputStream = fileInput.getStream(uri);
                if (inputStream == null) {
                    continue;
                }
                BufferedReader reader;
                reader = createReader(inputStream);

                try {
                    while ((line = reader.readLine()) != null) {
                        if (line.length() == 0) { // skip empty lines
                            continue;
                        }
                        collectorContext.lineContext().rawSource(line.getBytes(StandardCharsets.UTF_8));
                        if (!downstream.setNextRow(row)) {
                            throw new CollectionAbortedException();
                        }
                    }
                } finally {
                    reader.close();
                }
            }
        } finally {
            downstream.finish();
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
