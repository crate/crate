package org.cratedb.blob;

import org.cratedb.common.Hex;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

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
            throw new ElasticSearchIllegalArgumentException("No status match for [" + id + "]");

        }
    }


    private final ESLogger logger = Loggers.getLogger(getClass());

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
        logger.info("delete");
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
