package org.cratedb.blob.pending_transfer;

import org.cratedb.blob.BlobTransferTarget;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PutHeadChunkRunnable implements Runnable {

    private final File pendingFile;
    private final long bytesToSend;
    private final DiscoveryNode recipientNode;
    private final TransportService transportService;
    private final BlobTransferTarget blobTransferTarget;
    private final UUID transferId;
    private WatchKey watchKey;
    private WatchService watcher;
    private final ESLogger logger = Loggers.getLogger(getClass());

    public PutHeadChunkRunnable(File pendingFile, long bytesToSend,
                                TransportService transportService,
                                BlobTransferTarget blobTransferTarget,
                                DiscoveryNode recipientNode, UUID transferId) {
        this.pendingFile = pendingFile;
        this.bytesToSend = bytesToSend;
        this.recipientNode = recipientNode;
        this.blobTransferTarget = blobTransferTarget;
        this.transferId = transferId;
        this.transportService = transportService;
    }

    @Override
    public void run() {
        try {
            FileInputStream inputStream = new FileInputStream(pendingFile);
            int bufSize = 4096;
            int bytesRead;
            int size;
            int maxFileGrowthWait = 5;
            int fileGrowthWaited = 0;
            byte[] buffer = new byte[bufSize];
            long remainingBytes = bytesToSend;

            while (remainingBytes > 0) {
                size = (int)Math.min(bufSize, remainingBytes);
                bytesRead = inputStream.read(buffer, 0, size);
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
                    TransportRequestOptions.options(),
                    EmptyTransportResponseHandler.INSTANCE_SAME
                ).txGet();
            }


        } catch (FileNotFoundException ex) {
            logger.error("Can't send HeadChunk - file not found", ex, null);
        } catch (IOException ex) {
            logger.error("IOException in PutHeadChunkRunnable", ex, null);
        } finally {
            blobTransferTarget.putHeadChunkTransferFinished(transferId);
        }
    }

    private void waitUntilFileHasGrown(File pendingFile)
    {
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

                WatchEvent<Path> ev = (WatchEvent<Path>)event;
                Path filename = ev.context();
                if (filename.toString().equals(pendingFile.getName())) {
                    break;
                }
            }
        } catch (IOException ex) {
            return;
        } catch (InterruptedException ex) {
            return;
        }
    }

    private void initWatcher(String directoryToWatch) throws IOException {
        FileSystem fs = FileSystems.getDefault();
        watcher = fs.newWatchService();
        Path path = fs.getPath(directoryToWatch);
        watchKey = path.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
    }
}
