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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.UUID;

public class RemoteDigestBlob {

    private final String index;
    private Status status;

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
            }
            throw new IllegalArgumentException("No status match for [" + id + "]");
        }
    }


    private final static ESLogger logger = Loggers.getLogger(RemoteDigestBlob.class);

    private final String digest;
    private final Client client;
    private long size;
    private StartBlobResponse startResponse;
    private UUID transferId;


    public RemoteDigestBlob(BlobService blobService, String index, String digest) {
        this.digest = digest;
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

        return client.execute(DeleteBlobAction.INSTANCE, request).actionGet().deleted;
    }

    private Status start(ChannelBuffer buffer, boolean last) {
        logger.trace("start blob upload");
        assert (transferId == null);
        StartBlobRequest request = new StartBlobRequest(
                index,
                Hex.decodeHex(digest),
                new BytesArray(buffer.array()),
                last
        );
        transferId = request.transferId();
        size += buffer.readableBytes();

        startResponse = client.execute(StartBlobAction.INSTANCE, request).actionGet();
        status = startResponse.status();
        return status;
    }

    private Status chunk(ChannelBuffer buffer, boolean last) {
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
        PutChunkResponse putChunkResponse = client.execute(PutChunkAction.INSTANCE, request).actionGet();
        return putChunkResponse.status();
    }

    public Status addContent(ChannelBuffer buffer, boolean last) {
        if (startResponse == null) {
            // this is the first call to addContent
            return start(buffer, last);
        } else if (status == Status.EXISTS) {
            // client probably doesn't support 100-continue and is sending chunked requests
            // need to ignore the content.
            return status;
        } else if (status != Status.PARTIAL){
            throw new IllegalStateException("Expected Status.PARTIAL for chunk but got: " + status);
        } else {
            return chunk(buffer, last);
        }
    }

    public long size() {
        return size;
    }
}
