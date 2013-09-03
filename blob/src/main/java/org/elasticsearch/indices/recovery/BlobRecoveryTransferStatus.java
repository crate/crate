package org.elasticsearch.indices.recovery;

import java.io.FileOutputStream;
import java.io.RandomAccessFile;

public class BlobRecoveryTransferStatus {

    private final long transferId;
    private final FileOutputStream outputStream;
    private final String sourcePath;
    private final String targetPath;

    public BlobRecoveryTransferStatus(long transferId, FileOutputStream outputStream,
                                      String sourcePath, String targetPath) {
        this.transferId = transferId;
        this.outputStream = outputStream;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
    }

    public String sourcePath() {
        return sourcePath;
    }

    public String targetPath() {
        return targetPath;
    }

    public FileOutputStream outputStream() {
        return outputStream;
    }

    public long transferId() {
        return transferId;
    }
}
