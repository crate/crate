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


import org.jetbrains.annotations.VisibleForTesting;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemLoopException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.crate.execution.engine.collect.files.FileReadingIterator.toURI;
import io.crate.execution.engine.collect.files.Globs.GlobPredicate;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

public class LocalFsFileInput implements FileInput {

    private static final Pattern HAS_GLOBS_PATTERN = Pattern.compile("^((file://|/)[^\\*]*/)[^\\*]*\\*.*");

    @NotNull
    private final URI uri;
    @Nullable
    @VisibleForTesting
    final URI preGlobUri;
    @NotNull
    private final Predicate<String> uriPredicate;

    public LocalFsFileInput(URI uri) throws IOException {
        Matcher hasGlobMatcher = HAS_GLOBS_PATTERN.matcher(uri.toString());
        /*
         * hasGlobMatcher.group(1) returns part of the path before the wildcards with a trailing backslash,
         * ex)
         *      'file:///bucket/prefix/*.json'                           -> 'file:///bucket/prefix/'
         *      's3://bucket/year=2020/month=12/day=*0/hour=12/*.json'   -> 's3://bucket/year=2020/month=12/'
         */
        if (hasGlobMatcher.matches()) {
            Path oldPath = Paths.get(toURI(hasGlobMatcher.group(1)));
            String oldPathAsString = oldPath.toUri().toString();
            String newPathAsString = oldPath.toRealPath().toUri().toString();
            String resolvedFileUrl = uri.toString().replace(oldPathAsString, newPathAsString);
            this.uri = toURI(resolvedFileUrl);
            this.preGlobUri = toURI(newPathAsString);
        } else {
            this.uri = uri;
            this.preGlobUri = null;
        }
        this.uriPredicate = new GlobPredicate(this.uri.toString());
    }

    @Override
    public boolean isGlobbed() {
        return preGlobUri != null;
    }

    @Override
    public URI uri() {
        // returns a realPath if it was a symbolic link
        return uri;
    }

    @Override
    public List<URI> expandUri() throws IOException {
        if (preGlobUri == null) {
            return List.of(uri);
        }

        Path preGlobPath = Paths.get(preGlobUri);
        if (!Files.isDirectory(preGlobPath)) {
            preGlobPath = preGlobPath.getParent();
            if (preGlobPath == null) {
                return List.of();
            }
        }
        if (Files.notExists(preGlobPath)) {
            return List.of();
        }
        final int fileURIDepth = countOccurrences(uri.toString(), '/');
        final int maxDepth = fileURIDepth - countOccurrences(preGlobUri.toString(), '/') + 1;
        final List<URI> uris = new ArrayList<>();

        var fileVisitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                if (exc instanceof AccessDeniedException) {
                    return FileVisitResult.CONTINUE;
                }
                if (exc instanceof FileSystemLoopException) {
                    final int maxDepth = fileURIDepth - countOccurrences(file.toUri().toString(), '/') + 1;
                    if (maxDepth >= 0) {
                        Files.walkFileTree(file, EnumSet.of(FOLLOW_LINKS), maxDepth, this);
                    }
                    return FileVisitResult.CONTINUE;
                }
                throw exc;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                URI uri = file.toUri();
                if (uriPredicate.test(uri.toString())) {
                    uris.add(uri);
                }
                return FileVisitResult.CONTINUE;
            }
        };
        Files.walkFileTree(preGlobPath, EnumSet.of(FOLLOW_LINKS), maxDepth, fileVisitor);
        return uris;
    }

    @Override
    public InputStream getStream(URI uri) throws IOException {
        File file = new File(uri);
        return new FileInputStream(file);
    }

    @Override
    public boolean sharedStorageDefault() {
        return false;
    }

    private static int countOccurrences(String str, char c) throws IOException {
        try {
            return Math.toIntExact(str.chars().filter(ch -> ch == c).count());
        } catch (ArithmeticException e) {
            throw new IOException("Provided URI is too long");
        }
    }
}
