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

package org.cratedb.blob;

import org.cratedb.blob.exceptions.DigestMismatchException;
import org.cratedb.common.Hex;
import org.elasticsearch.common.bytes.BytesReference;
import org.jboss.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class DigestBlob {

    private final String digest;
    private final BlobContainer container;
    private final UUID transferId;
    protected File file;
    private FileChannel fileChannel;
    private FileChannel headFileChannel;
    private int size;
    private long headLength;
    private AtomicLong headSize;
    private MessageDigest md;
    private long chunks;
    private CountDownLatch headCatchedupLatch;
    private static final ESLogger logger = Loggers.getLogger(DigestBlob.class);

    public DigestBlob(BlobContainer container, String digest, UUID transferId) {
        this.digest = digest;
        this.container = container;
        this.size = 0;
        this.transferId = transferId;
    }

    public String getDigest() {
        return digest;
    }

    public int size() {
        return size;
    }

    public File file() {
        return file;
    }

    private static File getTmpFilePath(BlobContainer blobContainer, String digest, UUID transferId) {
       return new File(blobContainer.getTmpDirectory(), String.format("%s.%s", digest, transferId.toString()));
    }

    private File createTmpFile() throws IOException {
        File tmpFile = getTmpFilePath(container, digest, transferId);
        tmpFile.createNewFile();
        tmpFile.deleteOnExit();
        return tmpFile;
    }

    private void updateDigest(ByteBuffer bbf){
        if (md == null){
            try {
                md = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                // TODO: error handling
                e.printStackTrace();
            }
        }
        md.update(bbf.slice());
    }

    private void addContent(ChannelBuffer buffer, boolean last) throws IOException {
        if (buffer != null) {
            int readableBytes = buffer.readableBytes();
            ByteBuffer byteBuffer = buffer.toByteBuffer();
            if (file == null) {
                file = createTmpFile();
            }
            if (fileChannel == null) {
                FileOutputStream outputStream = new FileOutputStream(file);
                fileChannel = outputStream.getChannel();
            }

            int written = 0;
            do {
                if (headLength == 0) {
                    updateDigest(byteBuffer);
                }
                written += fileChannel.write(byteBuffer);
            } while (written < readableBytes);
            size += readableBytes;
            buffer.readerIndex(buffer.readerIndex() + written);
            chunks ++;
        }
        if (last) {
            if (file == null) {
                file = createTmpFile();
            }
            if (fileChannel == null) {
                FileOutputStream outputStream = new FileOutputStream(file);
                fileChannel = outputStream.getChannel();
            }
            fileChannel.force(false);
            fileChannel.close();
            fileChannel = null;
        } else {
            if (buffer == null) {
                throw new NullPointerException("buffer");
            }
        }
    }

    private void calculateDigest() {
        assert headSize.get() == headLength : "Head hasn't catched up, can't calculate digest";
        try {
            FileInputStream stream = new FileInputStream(file);
            stream.skip(headLength);
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = stream.read(buffer, 0, 4096)) > 0) {
                md.update(buffer, 0, bytesRead);
            }
            stream.close();
        } catch (IOException ex) {
            logger.error("error accessing file to calculate digest", ex);
        }
    }

    public File commit() throws DigestMismatchException {
        if (headLength > 0) {
            calculateDigest();
        }

        assert md != null;
        String contentDigest = Hex.encodeHexString(md.digest());
        if (!contentDigest.equals(digest)) {
            file.delete();
            throw new DigestMismatchException(digest, contentDigest);
        }
        File newFile = container.getFile(digest);
        file.renameTo(newFile);
        return newFile;
    }

    public void addContent(BytesReference content, boolean last){
        try {
            addContent(content.toChannelBuffer(), last);
        } catch (IOException e) {
            throw new BlobWriteException(digest, size, e);
        }
    }

    public void addToHead(BytesReference content) throws IOException {
        if (content == null) {
            return;
        }

        int written = 0;
        ChannelBuffer channelBuffer = content.toChannelBuffer();
        int readableBytes = channelBuffer.readableBytes();
        assert readableBytes + headSize.get() <= headLength : "Got too many bytes in addToHead()";

        ByteBuffer byteBuffer = channelBuffer.toByteBuffer();
        while (written < readableBytes) {
            updateDigest(byteBuffer);
            written += headFileChannel.write(byteBuffer);
        }
        headSize.addAndGet(written);
        if (headSize.get() == headLength) {
            headCatchedupLatch.countDown();
        }
    }

    public long chunks() {
        return chunks;
    }

    public BlobContainer container() {
        return this.container;
    }

    public static DigestBlob resumeTransfer(BlobContainer blobContainer, String digest,
                                            UUID transferId, long currentPos)
    {
        DigestBlob digestBlob = new DigestBlob(blobContainer, digest, transferId);
        digestBlob.file = getTmpFilePath(blobContainer, digest, transferId);

        try {
            logger.trace("Resuming DigestBlob {}. CurrentPos {}", digest, currentPos);

            digestBlob.headFileChannel = new FileOutputStream(digestBlob.file, false).getChannel();
            digestBlob.headLength = currentPos;
            digestBlob.headSize = new AtomicLong();
            digestBlob.headCatchedupLatch = new CountDownLatch(1);

            RandomAccessFile raf = new RandomAccessFile(digestBlob.file, "rw");
            raf.setLength(currentPos);
            raf.close();

            FileOutputStream outputStream = new FileOutputStream(digestBlob.file, true);
            digestBlob.fileChannel = outputStream.getChannel();
        } catch (IOException ex) {
            return null;
        }

        return digestBlob;
    }

    public void waitForHead() {
        if (headLength == 0) {
            return;
        }

        assert headCatchedupLatch != null;
        try {
            headCatchedupLatch.await();
        } catch (InterruptedException e) {
            // pass
        }
    }
}
