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

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.netty4.Netty4Utils;

import io.crate.common.Hex;
import io.netty.buffer.ByteBuf;

public class RemoteDigestBlob {

    public enum Status {
        FULL((byte) 0),
        PARTIAL((byte) 1),
        MISMATCH((byte) 2),
        EXISTS((byte) 3),
        FAILED((byte) 4);


        private final byte id;

        Status(byte id) {
            this.id = id;
        }

        /**
         * The internal representation of the status.
         */
        public byte id() {
            return id;
        }

        public static Status fromId(byte id) {
            switch (id) {
                case 0:
                    return FULL;
                case 1:
                    return PARTIAL;
                case 2:
                    return MISMATCH;
                case 3:
                    return EXISTS;
                case 4:
                    return FAILED;
                default:
                    throw new IllegalArgumentException("No status match for [" + id + "]");
            }
        }
    }


    private static final Logger LOGGER = LogManager.getLogger(RemoteDigestBlob.class);

    private final ShardId shardId;
    private final String digest;
    private final Client client;
    private long size;
    private StartBlobResponse startResponse;
    private UUID transferId;
    private Status status;


    public RemoteDigestBlob(Client client, ShardId shardId, String digest) {
        this.digest = digest;
        this.client = client;
        this.size = 0;
        this.shardId = shardId;
    }

    public Status status() {
        return status;
    }

    public boolean delete() {
        LOGGER.trace("delete");
        assert transferId == null : "transferId should be null";
        DeleteBlobRequest request = new DeleteBlobRequest(
            shardId,
            Hex.decodeHex(digest)
        );
        return FutureUtils.get(client.execute(TransportDeleteBlob.ACTION, request)).deleted;
    }

    private Status start(ByteBuf buffer, boolean last) {
        LOGGER.trace("start blob upload");
        assert transferId == null : "transferId should be null";
        StartBlobRequest request = new StartBlobRequest(
            shardId,
            Hex.decodeHex(digest),
            Netty4Utils.toBytesReference(buffer),
            last
        );
        transferId = request.transferId();
        size += buffer.readableBytes();

        startResponse = FutureUtils.get(client.execute(TransportStartBlob.ACTION, request));
        status = startResponse.status();
        return status;
    }

    private Status chunk(ByteBuf buffer, boolean last) {
        assert transferId != null : "transferId should not be null";
        PutChunkRequest request = new PutChunkRequest(
            shardId,
            Hex.decodeHex(digest),
            transferId,
            Netty4Utils.toBytesReference(buffer),
            size,
            last
        );
        size += buffer.readableBytes();
        PutChunkResponse putChunkResponse = FutureUtils.get(client.execute(TransportPutChunk.ACTION, request));
        return putChunkResponse.status();
    }

    public Status addContent(ByteBuf buffer, boolean last) {
        if (startResponse == null) {
            // this is the first call to addContent
            return start(buffer, last);
        } else if (status == Status.EXISTS) {
            // client probably doesn't support 100-continue and is sending chunked requests
            // need to ignore the content.
            return status;
        } else if (status != Status.PARTIAL) {
            throw new IllegalStateException("Expected Status.PARTIAL for chunk but got: " + status);
        } else {
            return chunk(buffer, last);
        }
    }

    public long size() {
        return size;
    }
}
