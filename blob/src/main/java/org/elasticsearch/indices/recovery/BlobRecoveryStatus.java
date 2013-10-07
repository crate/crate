package org.elasticsearch.indices.recovery;

import org.cratedb.blob.v2.BlobShard;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.index.shard.ShardId;

public class BlobRecoveryStatus {

    private final RecoveryStatus indexRecoveryStatus;
    private final ConcurrentMapLong<BlobRecoveryTransferStatus> onGoingTransfers = ConcurrentCollections.newConcurrentMapLong();
    final BlobShard blobShard;


    public BlobRecoveryStatus(RecoveryStatus indexRecoveryStatus, BlobShard blobShard) {
        this.indexRecoveryStatus = indexRecoveryStatus;
        this.blobShard = blobShard;
    }

    public long recoveryId(){
        return indexRecoveryStatus.recoveryId;
    }

    public boolean canceled() {
        return indexRecoveryStatus.isCanceled();
    }

    public void sentCanceledToSource() {
        indexRecoveryStatus.sentCanceledToSource = true;
    }

    public ShardId shardId() {
        return indexRecoveryStatus.shardId;
    }

    public ConcurrentMapLong<BlobRecoveryTransferStatus> onGoingTransfers() {
        return onGoingTransfers;
    }
}

