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


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class LocalFsFileInput implements FileInput {

    @Override
    public List<URI> listUris(final URI fileUri, final Predicate<URI> uriPredicate) throws IOException {
        assert fileUri != null : "fileUri must not be null";
        assert uriPredicate != null : "uriPredicate must not be null";

        Path path = Paths.get(fileUri);
        if (!Files.isDirectory(path)) {
            path = path.getParent();
            if (path == null) {
                return List.of();
            }
        }
        if (Files.notExists(path)) {
            return List.of();
        }

        final List<URI> uris = new ArrayList<>();
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                if (exc instanceof AccessDeniedException) {
                    return FileVisitResult.CONTINUE;
                }
                throw exc;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                URI uri = file.toUri();
                if (uriPredicate.test(uri)) {
                    uris.add(uri);
                }
                return FileVisitResult.CONTINUE;
            }
        });
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
}
