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

package io.crate.blob;

import io.crate.blob.exceptions.DigestNotFoundException;
import io.crate.common.Hex;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BlobContainer {

    private final ESLogger logger = Loggers.getLogger(getClass());

    public static final String[] SUB_DIRS = new String[256];

    public static final byte[] PREFIXES = new byte[256];

    private final File[] subDirs = new File[256];

    static {
        for (int i = 0; i < 256; i++) {
            SUB_DIRS[i] = String.format("%02x", i & 0xFFFFF);
            PREFIXES[i] = (byte)i;
        }
    }

    private final File baseDirectory;
    private final File tmpDirectory;
    private final File varDirectory;

    public BlobContainer(File baseDirectory) {
        this.baseDirectory = baseDirectory;
        this.tmpDirectory = new File(baseDirectory, "tmp");
        this.varDirectory = new File(baseDirectory, "var");
        FileSystemUtils.mkdirs(this.varDirectory);
        FileSystemUtils.mkdirs(this.tmpDirectory);

        createSubDirectories(this.varDirectory);
    }

    /**
     * All files are saved into a sub-folder
     * that is named after the first two characters of the file's sha1 hash
     * pre create these folders so that a .exists() check can be saved on each put request.
     *
     * @param parentDir
     */
    private void createSubDirectories(File parentDir) {
        for (int i = 0; i < SUB_DIRS.length; i++) {
            subDirs[i] = new File(parentDir, SUB_DIRS[i]);
            subDirs[i].mkdir();
        }
    }

    public interface FileVisitor {

        public boolean visit(File file);

    }

    public void walkFiles(FilenameFilter filter, FileVisitor visitor) {
        for (File dir : subDirs) {
            File[] files = dir.listFiles(filter);
            if (files == null) {
                continue;
            }
            for (File file : files) {
                if (!visitor.visit(file)) {
                    return;
                }
            }
        }
    }

    /**
     * get all digests in a subfolder
     * the digests are returned as byte[][] instead as String[] to save overhead in the BlobRecovery
     *
     * incomplete files leftover from a previous recovery are deleted.
     *
     * @param prefix the subfolder for which to get the digests
     * @return byte array containing the digests (digest = byte[20])
     */
    public byte[][] cleanAndReturnDigests(byte prefix) {
        int index = prefix & 0xFF;  // byte is signed and may be negative, convert to int to get correct index
        String[] names = cleanDigests(subDirs[index].list(), index);
        byte[][] digests = new byte[names.length][];
        for(int i = 0; i < names.length; i ++){
            try {
                digests[i] = Hex.decodeHex(names[i]);
            } catch (ElasticsearchIllegalStateException ex) {
                logger.error("Can't convert string {} to byte array", names[i]);
                throw ex;
            }
        }
        return digests;
    }

    /**
     * delete all digests that have a .X suffix.
     * they are leftover files from a previous recovery that was interrupted
     */
    private String[] cleanDigests(String[] names, int index) {
        if (names == null) {
            return null;
        }
        List<String> newNames = new ArrayList<>(names.length);
        for (String name : names) {
            if (name.contains(".")) {
                if (!new File(subDirs[index], name).delete()) {
                    logger.error("Could not delete {}/{}", subDirs[index], name);
                }
            } else {
                newNames.add(name);
            }
        }

        return newNames.toArray(new String[newNames.size()]);
    }

    public File getBaseDirectory() {
        return baseDirectory;
    }

    public File getTmpDirectory() {
        return tmpDirectory;
    }

    public File getVarDirectory() {
        return varDirectory;
    }

    public File getFile(String digest) {
        return new File(getVarDirectory(), digest.substring(0, 2) + File.separator + digest);
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
}
