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

import io.crate.blob.exceptions.BlobAlreadyExistsException;
import io.crate.blob.exceptions.DigestMismatchException;
import io.crate.common.Hex;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class DigestBlob implements Closeable {

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
    private CountDownLatch headCatchedUpLatch;
    private static final Logger LOGGER = LogManager.getLogger(DigestBlob.class);

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

    private static Path getTmpFilePath(BlobContainer blobContainer, String digest, UUID transferId) {
        return blobContainer.getTmpDirectory().resolve(digest + "." + transferId.toString());
    }

    private File createTmpFile() throws IOException {
        File tmpFile = getTmpFilePath(container, digest, transferId).toFile();
        tmpFile.createNewFile();
        return tmpFile;
    }

    private void updateDigest(ByteBuffer bbf) throws IOException {
        if (md == null) {
            try {
                md = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                throw new IOException(e);
            }
        }
        md.update(bbf.slice());
    }

    private void addContent(ByteBuf buffer, boolean last) throws IOException {
        if (buffer != null) {
            int readableBytes = buffer.readableBytes();
            ByteBuffer byteBuffer = buffer.nioBuffer();
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
            chunks++;
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
        try (FileInputStream stream = new FileInputStream(file)) {
            stream.skipNBytes(headLength);
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = stream.read(buffer, 0, 4096)) > 0) {
                md.update(buffer, 0, bytesRead);
            }
        } catch (IOException ex) {
            LOGGER.error("error accessing file to calculate digest", ex);
        }
    }

    public File commit() throws DigestMismatchException, BlobAlreadyExistsException {
        if (headLength > 0) {
            calculateDigest();
        }

        assert md != null : "MessageDigest should not be null";
        try {
            String contentDigest = Hex.encodeHexString(md.digest());
            if (!contentDigest.equals(digest)) {
                file.delete();
                throw new DigestMismatchException(digest, contentDigest);
            }
        } finally {
            IOUtils.closeWhileHandlingException(headFileChannel);
            headFileChannel = null;
        }
        File newFile = container.getFile(digest);
        Semaphore semaphore = container.digestCoordinator(digest);
        try {
            semaphore.acquire();

            try {
                if (Files.exists(newFile.toPath())) {
                    throw new BlobAlreadyExistsException(digest);
                }
                file.renameTo(newFile);
                file = null;
            } finally {
                // semaphore was acquired successfully, release it
                semaphore.release();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Unable to commit blob {}", e, file.getName());
            throw new IllegalStateException("Unable to commit blob because exclusive execution could not be achieved");
        }

        return newFile;
    }

    public File getContainerFile() {
        return container.getFile(digest);
    }

    public void addContent(BytesReference content, boolean last) {
        try {
            addContent(Netty4Utils.toByteBuf(content), last);
        } catch (IOException e) {
            throw new BlobWriteException(digest, size, e);
        }
    }

    public void addToHead(BytesReference content) throws IOException {
        if (content == null) {
            return;
        }

        int written = 0;
        ByteBuf byteBuf = Netty4Utils.toByteBuf(content);
        int readableBytes = byteBuf.readableBytes();
        assert readableBytes + headSize.get() <= headLength : "Got too many bytes in addToHead()";

        ByteBuffer byteBuffer = byteBuf.nioBuffer();
        while (written < readableBytes) {
            updateDigest(byteBuffer);
            written += headFileChannel.write(byteBuffer);
        }
        headSize.addAndGet(written);
        if (headSize.get() == headLength) {
            headCatchedUpLatch.countDown();
        }
    }

    public long chunks() {
        return chunks;
    }

    public static DigestBlob resumeTransfer(BlobContainer blobContainer, String digest,
                                            UUID transferId, long currentPos) {
        DigestBlob digestBlob = new DigestBlob(blobContainer, digest, transferId);
        digestBlob.file = getTmpFilePath(blobContainer, digest, transferId).toFile();

        try {
            LOGGER.trace("Resuming DigestBlob {}. CurrentPos {}", digest, currentPos);
            digestBlob.headFileChannel = new FileOutputStream(digestBlob.file, false).getChannel();
            digestBlob.headLength = currentPos;
            digestBlob.headSize = new AtomicLong();
            digestBlob.headCatchedUpLatch = new CountDownLatch(1);

            RandomAccessFile raf = new RandomAccessFile(digestBlob.file, "rw");
            raf.setLength(currentPos);
            raf.close();

            FileOutputStream outputStream = new FileOutputStream(digestBlob.file, true);
            digestBlob.fileChannel = outputStream.getChannel();
        } catch (IOException ex) {
            LOGGER.error("error resuming transfer of {}, id: {}", ex, digest, transferId);
            return null;
        }

        return digestBlob;
    }

    public void waitForHead() {
        if (headLength == 0) {
            return;
        }

        assert headCatchedUpLatch != null : "headCatchedUpLatch should not be null";
        try {
            headCatchedUpLatch.await();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    @Override
    public void close() throws IOException {
        if (file != null) {
            file.delete();
        }
    }
}
