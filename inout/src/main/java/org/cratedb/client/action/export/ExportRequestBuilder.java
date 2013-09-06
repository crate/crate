package org.cratedb.client.action.export;

import org.cratedb.action.export.ExportAction;
import org.cratedb.action.export.ExportRequest;
import org.cratedb.action.export.ExportResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

public class ExportRequestBuilder extends ActionRequestBuilder<ExportRequest, ExportResponse, ExportRequestBuilder> {

    public ExportRequestBuilder(Client client) {
        super((InternalClient) client, new ExportRequest());
    }

    @Override
    protected void doExecute(ActionListener<ExportResponse> listener) {
        ((Client)client).execute(ExportAction.INSTANCE, request, listener);
    }

    public ExportRequestBuilder setIndices(String ... indices) {
        request.indices(indices);
        return this;
    }
}