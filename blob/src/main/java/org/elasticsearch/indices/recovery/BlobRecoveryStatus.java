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

package org.elasticsearch.indices.recovery;

import io.crate.blob.v2.BlobShard;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.index.shard.ShardId;

public class BlobRecoveryStatus {

    private final RecoveryTarget recoveryTarget;
    private final ConcurrentMapLong<BlobRecoveryTransferStatus> onGoingTransfers = ConcurrentCollections.newConcurrentMapLong();
    final BlobShard blobShard;


    public BlobRecoveryStatus(RecoveryTarget recoveryTarget, BlobShard blobShard) {
        this.recoveryTarget = recoveryTarget;
        this.blobShard = blobShard;
    }

    public long recoveryId() {
        return recoveryTarget.recoveryId();
    }

    public boolean canceled() {
        return recoveryTarget.CancellableThreads().isCancelled();
    }

    public void sentCanceledToSource() {
        recoveryTarget.CancellableThreads().checkForCancel();
    }

    public ShardId shardId() {
        return recoveryTarget.shardId();
    }

    public ConcurrentMapLong<BlobRecoveryTransferStatus> onGoingTransfers() {
        return onGoingTransfers;
    }
}

