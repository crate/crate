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

package io.crate.blob.transfer;

import io.crate.blob.BlobTransferTarget;
import io.crate.blob.DigestBlob;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PutHeadChunkRunnable implements Runnable {

    private final DigestBlob digestBlob;
    private final long bytesToSend;
    private final DiscoveryNode recipientNode;
    private final TransportService transportService;
    private final BlobTransferTarget blobTransferTarget;
    private final UUID transferId;
    private WatchKey watchKey;
    private WatchService watcher;
    private static final Logger logger = Loggers.getLogger(PutHeadChunkRunnable.class);

    public PutHeadChunkRunnable(DigestBlob digestBlob, long bytesToSend,
                                TransportService transportService,
                                BlobTransferTarget blobTransferTarget,
                                DiscoveryNode recipientNode, UUID transferId) {
        this.digestBlob = digestBlob;
        this.bytesToSend = bytesToSend;
        this.recipientNode = recipientNode;
        this.blobTransferTarget = blobTransferTarget;
        this.transferId = transferId;
        this.transportService = transportService;
    }

    @Override
    public void run() {
        FileInputStream fileInputStream = null;
        try {
            int bufSize = 4096;
            int bytesRead;
            int size;
            int maxFileGrowthWait = 5;
            int fileGrowthWaited = 0;
            byte[] buffer = new byte[bufSize];
            long remainingBytes = bytesToSend;

            File pendingFile;
            try {
                pendingFile = digestBlob.file();
                if (pendingFile == null) {
                    pendingFile = digestBlob.getContainerFile();
                }
                fileInputStream = new FileInputStream(pendingFile);
            } catch (FileNotFoundException e) {
                // this happens if the file has already been moved from tmpDirectory to containerDirectory
                pendingFile = digestBlob.getContainerFile();
                fileInputStream = new FileInputStream(pendingFile);
            }

            while (remainingBytes > 0) {
                size = (int) Math.min(bufSize, remainingBytes);
                bytesRead = fileInputStream.read(buffer, 0, size);
                if (bytesRead < size) {
                    waitUntilFileHasGrown(pendingFile);
                    fileGrowthWaited++;
                    if (fileGrowthWaited == maxFileGrowthWait) {
                        throw new HeadChunkFileTooSmallException(pendingFile.getAbsolutePath());
                    }
                    if (bytesRead < 1) {
                        continue;
                    }
                }
                remainingBytes -= bytesRead;

                transportService.submitRequest(
                    recipientNode,
                    BlobHeadRequestHandler.Actions.PUT_BLOB_HEAD_CHUNK,
                    new PutBlobHeadChunkRequest(transferId, new BytesArray(buffer, 0, bytesRead)),
                    TransportRequestOptions.EMPTY,
                    EmptyTransportResponseHandler.INSTANCE_SAME
                ).txGet();
            }

        } catch (IOException ex) {
            logger.error("IOException in PutHeadChunkRunnable", ex);
        } finally {
            blobTransferTarget.putHeadChunkTransferFinished(transferId);
            if (watcher != null) {
                try {
                    watcher.close();
                } catch (IOException e) {
                    logger.error("Error closing WatchService in {}", e, getClass().getSimpleName());
                }
            }
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    logger.error("Error closing HeadChunk", e);
                }
            }
        }
    }

    private void waitUntilFileHasGrown(File pendingFile) {
        try {
            if (watcher == null) {
                initWatcher(pendingFile.getParent());
            }

            watchKey = watcher.poll(5, TimeUnit.SECONDS);
            if (watchKey == null) {
                return;
            }
            for (WatchEvent<?> event : watchKey.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path filename = ev.context();
                if (filename.toString().equals(pendingFile.getName())) {
                    break;
                }
            }
        } catch (IOException | InterruptedException ex) {
            logger.warn(ex.getMessage(), ex);
        }
    }

    private void initWatcher(String directoryToWatch) throws IOException {
        FileSystem fs = FileSystems.getDefault();
        watcher = fs.newWatchService();
        Path path = fs.getPath(directoryToWatch);
        watchKey = path.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
    }
}
