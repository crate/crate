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

package org.elasticsearch.indices.recovery;

import io.crate.blob.v2.BlobShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BlobRecoveryStatus {

    private final RecoveryTarget recoveryTarget;
    private final ConcurrentMap<Long, BlobRecoveryTransferStatus> onGoingTransfers = new ConcurrentHashMap<>();
    final BlobShard blobShard;


    public BlobRecoveryStatus(RecoveryTarget recoveryTarget, BlobShard blobShard) {
        this.recoveryTarget = recoveryTarget;
        this.blobShard = blobShard;
    }

    public long recoveryId() {
        return recoveryTarget.recoveryId();
    }

    public boolean canceled() {
        return recoveryTarget.cancellableThreads().isCancelled();
    }

    public void sentCanceledToSource() {
        recoveryTarget.cancellableThreads().checkForCancel();
    }

    public ShardId shardId() {
        return recoveryTarget.shardId();
    }

    public ConcurrentMap<Long, BlobRecoveryTransferStatus> onGoingTransfers() {
        return onGoingTransfers;
    }
}

