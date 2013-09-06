package org.cratedb.client.action.searchinto;

import org.cratedb.action.searchinto.SearchIntoAction;
import org.cratedb.action.searchinto.SearchIntoRequest;
import org.cratedb.action.searchinto.SearchIntoResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

public class SearchIntoRequestBuilder extends
        ActionRequestBuilder<SearchIntoRequest, SearchIntoResponse,
                SearchIntoRequestBuilder> {

    public SearchIntoRequestBuilder(Client client) {
        super((InternalClient) client, new SearchIntoRequest());
    }

    @Override
    protected void doExecute(ActionListener<SearchIntoResponse> listener) {
        ((Client) client).execute(SearchIntoAction.INSTANCE, request,
                listener);
    }

    public SearchIntoRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }
}