/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.repository;

import io.crate.replication.logical.action.GetFileChunkAction;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.MultiChunkTransfer;
import org.elasticsearch.indices.recovery.MultiFileWriter;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public class RemoteClusterMultiChunkTransfer extends MultiChunkTransfer<StoreFileMetadata, RemoteClusterRepositoryFileChunk> {

    private static final String RESTORE_SHARD_TEMP_FILE_PREFIX = "CLUSTER_REPO_TEMP_";
    private static final Logger LOGGER = Loggers.getLogger(RemoteClusterMultiChunkTransfer.class);

    private final DiscoveryNode remoteNode;
    private final String restoreUUID;
    private final ShardId remoteShardId;
    private final String localClusterName;
    private final RecoveryState recoveryState;
    private final Client client;
    private final ThreadPool threadPool;
    private final ByteSizeValue chunkSize;
    private final String tempFilePrefix;
    private final MultiFileWriter multiFileWriter;
    private final Object lock = new Object();
    private long offset = 0L;

    public RemoteClusterMultiChunkTransfer(Logger logger,
                                           String localClusterName,
                                           Store localStore,
                                           int maxConcurrentChunks,
                                           String restoreUUID,
                                           DiscoveryNode remoteNode,
                                           ShardId remoteShardId,
                                           List<StoreFileMetadata> remoteFiles,
                                           Client client,
                                           ThreadPool threadPool,
                                           RecoveryState recoveryState,
                                           ByteSizeValue chunkSize,
                                           ActionListener<Void> listener) {
        super(logger, listener, maxConcurrentChunks, remoteFiles);
        this.localClusterName = localClusterName;
        this.restoreUUID = restoreUUID;
        this.remoteNode = remoteNode;
        this.remoteShardId = remoteShardId;
        this.recoveryState = recoveryState;
        this.client = client;
        this.threadPool = threadPool;
        this.chunkSize = chunkSize;
        this.tempFilePrefix = RESTORE_SHARD_TEMP_FILE_PREFIX + restoreUUID + ".";
        this.multiFileWriter = new MultiFileWriter(localStore, recoveryState.getIndex(), tempFilePrefix, logger, () -> {});

        // Add all the available files to show the recovery status
        for (var fileMetadata : remoteFiles) {
            recoveryState.getIndex().addFileDetail(fileMetadata.name(), fileMetadata.length(), false);
        }
    }

    @Override
    protected RemoteClusterRepositoryFileChunk nextChunkRequest(StoreFileMetadata resource) throws IOException {
        var chunkReq = new RemoteClusterRepositoryFileChunk(resource, offset, chunkSize.bytesAsInt());
        offset += chunkSize.bytesAsInt();
        return chunkReq;
    }

    @Override
    protected void executeChunkRequest(RemoteClusterRepositoryFileChunk request,
                                       ActionListener<Void> listener) {
        var getFileChunkRequest = new GetFileChunkAction.Request(
            restoreUUID,
            remoteNode,
            remoteShardId,
            localClusterName,
            recoveryState.getShardId(),
            request.storeFileMetadata(),
            request.offset(),
            request.length()
        );


        client.execute(
            GetFileChunkAction.INSTANCE,
            getFileChunkRequest,
            new ActionListener<>() {
                @Override
                public void onResponse(GetFileChunkAction.Response response) {
                    LOGGER.debug("Filename: {}, response_size: {}, response_offset: {}",
                                 request.storeFileMetadata().name(),
                                 response.data().length(),
                                 response.offset()
                    );
                    threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(
                        () -> {
                            synchronized (lock) {
                                try {
                                    multiFileWriter.writeFileChunk(
                                        response.storeFileMetadata(),
                                        response.offset(),
                                        response.data(),
                                        request.lastChunk()
                                    );
                                    listener.onResponse(null);
                                } catch (IOException e) {
                                    listener.onFailure(new UncheckedIOException(e));
                                }
                            }
                        }
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.error("Failed to fetch file chunk for " + request.storeFileMetadata().name() +
                                 " with offset " + request.offset() + ": ",
                                 e
                    );
                    listener.onFailure(e);
                }
            }
        );
    }

    @Override
    protected void handleError(StoreFileMetadata resource, Exception e) throws Exception {
        LOGGER.error("Error while transferring segments ", e);
    }

    @Override
    protected void onNewResource(StoreFileMetadata resource) throws IOException {
        offset = 0L;
    }

    @Override
    public void close() throws IOException {
        multiFileWriter.renameAllTempFiles();
        multiFileWriter.close();
    }
}
