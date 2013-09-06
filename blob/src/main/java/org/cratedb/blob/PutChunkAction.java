package org.cratedb.blob;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

public class PutChunkAction extends Action<PutChunkRequest, PutChunkResponse, PutChunkRequestBuilder> {

    public static final PutChunkAction INSTANCE = new PutChunkAction();
    public static final String NAME = "put_chunk";

    protected PutChunkAction() {
        super(NAME);
    }

    @Override
    public PutChunkRequestBuilder newRequestBuilder(Client client) {
        return new PutChunkRequestBuilder(client);
    }

    @Override
    public PutChunkResponse newResponse() {
        return new PutChunkResponse();
    }
}
