package crate.elasticsearch.client.action.searchinto;

import crate.elasticsearch.action.searchinto.SearchIntoAction;
import crate.elasticsearch.action.searchinto.SearchIntoRequest;
import crate.elasticsearch.action.searchinto.SearchIntoResponse;
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