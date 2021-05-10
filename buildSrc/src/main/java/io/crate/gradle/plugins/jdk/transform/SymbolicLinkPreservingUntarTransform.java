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

package io.crate.gradle.plugins.jdk.transform;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.gradle.api.logging.Logging;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

public abstract class SymbolicLinkPreservingUntarTransform implements UnpackTransform {

    public void unpack(File tarFile, File targetDir) throws IOException {
        Logging.getLogger(SymbolicLinkPreservingUntarTransform.class)
            .info("Unpacking " + tarFile.getName() + " using " + SymbolicLinkPreservingUntarTransform.class.getSimpleName() + ".");

        TarArchiveInputStream tar = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(tarFile)));
        final Path destinationPath = targetDir.toPath();
        TarArchiveEntry entry = tar.getNextTarEntry();
        while (entry != null) {
            final Path relativePath = UnpackTransform.trimArchiveExtractPath(entry.getName());
            if (relativePath == null) {
                entry = tar.getNextTarEntry();
                continue;
            }

            final Path destination = destinationPath.resolve(relativePath);
            final Path parent = destination.getParent();
            if (Files.exists(parent) == false) {
                Files.createDirectories(parent);
            }
            if (entry.isDirectory()) {
                Files.createDirectory(destination);
            } else if (entry.isSymbolicLink()) {
                Files.createSymbolicLink(destination, Paths.get(entry.getLinkName()));
            } else {
                // copy the file from the archive using a small buffer to avoid heaping
                Files.createFile(destination);
                try (FileOutputStream fos = new FileOutputStream(destination.toFile())) {
                    tar.transferTo(fos);
                }
            }
            if (entry.isSymbolicLink() == false) {
                // check if the underlying file system supports POSIX permissions
                final PosixFileAttributeView view = Files.getFileAttributeView(destination, PosixFileAttributeView.class);
                if (view != null) {
                    final Set<PosixFilePermission> permissions = PosixFilePermissions.fromString(
                        permissions((entry.getMode() >> 6) & 07) + permissions((entry.getMode() >> 3) & 07) + permissions(
                            (entry.getMode() >> 0) & 07
                        )
                    );
                    Files.setPosixFilePermissions(destination, permissions);
                }
            }
            entry = tar.getNextTarEntry();
        }

    }

    private static String permissions(final int permissions) {
        if (permissions < 0 || permissions > 7) {
            throw new IllegalArgumentException("permissions [" + permissions + "] out of range");
        }
        final StringBuilder sb = new StringBuilder(3);
        if ((permissions & 4) == 4) {
            sb.append('r');
        } else {
            sb.append('-');
        }
        if ((permissions & 2) == 2) {
            sb.append('w');
        } else {
            sb.append('-');
        }
        if ((permissions & 1) == 1) {
            sb.append('x');
        } else {
            sb.append('-');
        }
        return sb.toString();
    }
}
