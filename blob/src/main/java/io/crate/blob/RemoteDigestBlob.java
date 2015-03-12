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

package io.crate.blob;

import io.crate.common.Hex;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.UUID;

public class RemoteDigestBlob {

    private final String index;
    private Status status;

    public static enum Status {
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
            }
            throw new ElasticsearchIllegalArgumentException("No status match for [" + id + "]");

        }
    }


    private final static ESLogger logger = Loggers.getLogger(RemoteDigestBlob.class);

    private final String digest;
    private final BlobService blobService;
    private final Client client;
    private long size;
    private StartBlobResponse startResponse;
    private UUID transferId;


    public RemoteDigestBlob(BlobService blobService, String index, String digest) {
        this.digest = digest;
        this.blobService = blobService;
        this.client = blobService.getInjector().getInstance(Client.class);
        this.size = 0;
        this.index = index;
    }

    public Status status(){
        return status;
    }

    public boolean delete() {
        logger.trace("delete");
        assert (transferId == null);
        DeleteBlobRequest request = new DeleteBlobRequest(
                index,
                Hex.decodeHex(digest)
        );

        DeleteBlobResponse response = client.execute(DeleteBlobAction.INSTANCE, request).actionGet();
        return response.deleted;
    }

    private void start(ChannelBuffer buffer, boolean last) {
        logger.trace("start blob upload");
        assert (transferId == null);
        StartBlobRequest request = new StartBlobRequest(
                index,
                Hex.decodeHex(digest),
                new BytesArray(buffer.array()),
                last
        );
        size += buffer.readableBytes();
        startResponse = client.execute(StartBlobAction.INSTANCE, request).actionGet();
        transferId = request.transferId();
        status = startResponse.status();
    }

    private void chunk(ChannelBuffer buffer, boolean last) {
        assert (transferId != null);
        PutChunkRequest request = new PutChunkRequest(
            index,
            Hex.decodeHex(digest),
            transferId,
            new BytesArray(buffer.array()),
            size,
            last
        );
        size += buffer.readableBytes();
        PutChunkResponse response = client.execute(PutChunkAction.INSTANCE, request).actionGet();
        status = response.status();
    }

    public Status addContent(ChannelBuffer buffer, boolean last) {
        if (startResponse == null) {
            // this is the first call to addContent
            start(buffer, last);
        } else if (status == Status.EXISTS) {
            // client probably doesn't support 100-continue and is sending chunked requests
            // need to ignore the content.
        } else if (status != Status.PARTIAL){
            throw new IllegalStateException("Expected Status.PARTIAL for chunk but got: " + status);
        } else {
            chunk(buffer, last);
        }

        return status;
    }

    public long size() {
        return size;
    }
}
