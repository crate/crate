package org.cratedb.blob;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

public class DeleteBlobAction extends Action<DeleteBlobRequest, DeleteBlobResponse, DeleteBlobRequestBuilder> {

    public static final DeleteBlobAction INSTANCE = new DeleteBlobAction();
    public static final String NAME = "delete_blob";

    protected DeleteBlobAction() {
        super(NAME);
    }

    @Override
    public DeleteBlobRequestBuilder newRequestBuilder(Client client) {
        return new DeleteBlobRequestBuilder(client);
    }

    @Override
    public DeleteBlobResponse newResponse() {
        return new DeleteBlobResponse();
    }
}
