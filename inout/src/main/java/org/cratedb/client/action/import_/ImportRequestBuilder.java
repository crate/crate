package org.cratedb.client.action.import_;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

import org.cratedb.action.import_.ImportAction;
import org.cratedb.action.import_.ImportRequest;
import org.cratedb.action.import_.ImportResponse;

public class ImportRequestBuilder extends ActionRequestBuilder<ImportRequest, ImportResponse, ImportRequestBuilder> {

    public ImportRequestBuilder(Client client) {
        super((InternalClient) client, new ImportRequest());
    }

    @Override
    protected void doExecute(ActionListener<ImportResponse> listener) {
        ((Client) client).execute(ImportAction.INSTANCE, request, listener);
    }
}
