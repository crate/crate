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

package io.crate.blob;

import static io.crate.testing.Asserts.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.common.Hex;
import io.crate.test.utils.Blobs;

public class BlobContainerTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testGetFiles() throws Exception {
        File blobsPath = temporaryFolder.newFolder();
        BlobContainer blobContainer = new BlobContainer(blobsPath.toPath());
        blobContainer.getFile(digest("Content A")).createNewFile();
        blobContainer.getFile(digest("Content B")).createNewFile();
        blobContainer.getFile(digest("Content C")).createNewFile();

        Iterator<File> fileIterator = blobContainer.getFiles().iterator();

        assertThat(fileIterator.hasNext()).isTrue();
        assertThat(fileIterator.next().exists()).isTrue();
        assertThat(fileIterator.hasNext()).isTrue();
        assertThat(fileIterator.next().exists()).isTrue();
        assertThat(fileIterator.next().exists()).isTrue();
        assertThat(fileIterator.hasNext()).isFalse();
    }

    @Test
    public void testContainerVisitor() throws Exception {
        File blobsPath = temporaryFolder.newFolder();
        BlobContainer blobContainer = new BlobContainer(blobsPath.toPath());
        blobContainer.getFile(digest("Content A")).createNewFile();
        blobContainer.getFile(digest("Content B")).createNewFile();
        blobContainer.getFile(digest("Content C")).createNewFile();

        final AtomicInteger blobsCount = new AtomicInteger(0);
        blobContainer.visitBlobs(new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                blobsCount.getAndIncrement();
                return FileVisitResult.CONTINUE;
            }
        });

        assertThat(blobsCount).hasValue(3);
    }

    private static String digest(String content) {
        return Hex.encodeHexString(Blobs.digest(content));
    }
}
