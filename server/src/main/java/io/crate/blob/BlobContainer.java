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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.crate.blob.exceptions.DigestNotFoundException;
import io.crate.common.Hex;

public class BlobContainer {

    private static final Logger LOGGER = LogManager.getLogger(BlobContainer.class);
    private static final String[] SUB_DIRS = new String[256];

    public static final byte[] PREFIXES = new byte[256];

    private final File[] subDirs = new File[256];

    static {
        for (int i = 0; i < 256; i++) {
            SUB_DIRS[i] = String.format(Locale.ENGLISH, "%02x", i & 0xFFFFF);
            PREFIXES[i] = (byte) i;
        }
    }

    private final Path baseDirectory;
    private final Path tmpDirectory;
    private final Path varDirectory;
    private final BlobCoordinator blobCoordinator;

    public BlobContainer(Path baseDirectory) {
        this.baseDirectory = baseDirectory;
        this.tmpDirectory = baseDirectory.resolve("tmp");
        this.varDirectory = baseDirectory.resolve("var");
        this.blobCoordinator = new BlobCoordinator();
        try {
            Files.createDirectories(this.varDirectory);
            createSubDirectories(this.varDirectory);
        } catch (IOException e) {
            LOGGER.error("Could not create 'var' path {}", this.varDirectory);
            throw new RuntimeException(e);
        }

        try {
            Files.createDirectories(this.tmpDirectory);
        } catch (IOException e) {
            LOGGER.error("Could not create 'tmp' path {}", this.tmpDirectory);
            throw new RuntimeException(e);
        }
    }

    /**
     * All files are saved into a sub-folder
     * that is named after the first two characters of the file's sha1 hash
     * pre create these folders so that a .exists() check can be saved on each put request.
     *
     * @param parentDir
     */
    private void createSubDirectories(Path parentDir) throws IOException {
        for (int i = 0; i < SUB_DIRS.length; i++) {
            Path subDir = parentDir.resolve(SUB_DIRS[i]);
            subDirs[i] = subDir.toFile();
            Files.createDirectories(subDir);
        }
    }

    public Iterable<File> getFiles() {
        return new RecursiveFileIterable(subDirs);
    }

    /**
     * get all digests in a subfolder
     * the digests are returned as byte[][] instead as String[] to save overhead in the BlobRecovery
     * <p>
     * incomplete files leftover from a previous recovery are deleted.
     *
     * @param prefix the subfolder for which to get the digests
     * @return byte array containing the digests (digest = byte[20])
     */
    public byte[][] cleanAndReturnDigests(byte prefix) {
        int index = prefix & 0xFF;  // byte is signed and may be negative, convert to int to get correct index
        List<String> names = cleanDigests(subDirs[index].list(), index);
        byte[][] digests = new byte[names.size()][];
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            try {
                digests[i] = Hex.decodeHex(name);
            } catch (IllegalStateException ex) {
                LOGGER.error("Can't convert string {} to byte array", name);
                throw ex;
            }
        }
        return digests;
    }

    /**
     * delete all digests that have a .X suffix.
     * they are leftover files from a previous recovery that was interrupted
     */
    private List<String> cleanDigests(String[] names, int index) {
        if (names == null) {
            List.of();
        }
        List<String> newNames = new ArrayList<>(names.length);
        for (String name : names) {
            if (name.contains(".")) {
                if (!new File(subDirs[index], name).delete()) {
                    LOGGER.error("Could not delete {}/{}", subDirs[index], name);
                }
            } else {
                newNames.add(name);
            }
        }
        return newNames;
    }

    /**
     * Walks the blobs data tree directory and visits all items using the provided {@link FileVisitor}
     *
     * NOTE: USE WITH CAUTION!
     * NOTE: THIS IS AN EXPENSIVE OPERATION AS IT ITERATES OVER THE ENTIRE BLOB CONTAINER
     *
     * @param visitor
     * @throws IOException
     */
    public void visitBlobs(FileVisitor<Path> visitor) throws IOException {
        Files.walkFileTree(varDirectory, visitor);
    }

    public Semaphore digestCoordinator(String digest) {
        return blobCoordinator.digestCoordinator(digest);
    }

    public Path getBaseDirectory() {
        return baseDirectory;
    }

    public Path getTmpDirectory() {
        return tmpDirectory;
    }

    public File getFile(String digest) {
        return varDirectory.resolve(digest.substring(0, 2)).resolve(digest).toFile();
    }

    public DigestBlob createBlob(String digest, UUID transferId) {
        // TODO: check if exists already
        return new DigestBlob(this, digest, transferId);
    }

    public RandomAccessFile getRandomAccessFile(String digest) {
        try {
            return new RandomAccessFile(getFile(digest), "r");
        } catch (FileNotFoundException e) {
            throw new DigestNotFoundException(digest);
        }
    }

    private static class RecursiveFileIterable implements Iterable<File> {

        private final File[] subDirs;

        private RecursiveFileIterable(File[] subDirs) {
            this.subDirs = subDirs;
        }

        @Override
        public Iterator<File> iterator() {
            return new RecursiveFileIterator(subDirs);
        }

    }

    private static class RecursiveFileIterator implements Iterator<File> {

        private final File[] subDirs;
        private int subDirIndex = -1;

        private File[] files = null;
        private int fileIndex = -1;

        private RecursiveFileIterator(File[] subDirs) {
            this.subDirs = subDirs;
        }

        /**
         * Returns {@code true} if the current sub-directory have files to traverse. Otherwise, iterates
         * until it finds the next sub-directory with files inside it.
         * Returns {@code false} only after reaching the last file of the last sub-directory.
         */
        @Override
        public boolean hasNext() {
            if (files == null || (fileIndex + 1) == files.length) {
                files = null;
                fileIndex = -1;
                while (subDirIndex + 1 < subDirs.length && (files == null || files.length == 0)) {
                    files = subDirs[++subDirIndex].listFiles();
                }
            }
            return (files != null && fileIndex + 1 < files.length);
        }

        @Override
        public File next() {
            if (hasNext()) {
                return files[++fileIndex];
            }

            throw new NoSuchElementException("List of files is empty");
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is unsupported for " + BlobContainer.class.getSimpleName());
        }
    }
}
