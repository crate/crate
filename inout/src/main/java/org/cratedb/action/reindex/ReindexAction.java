package org.cratedb.action.reindex;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

import org.cratedb.action.searchinto.SearchIntoRequest;
import org.cratedb.action.searchinto.SearchIntoResponse;
import org.cratedb.client.action.searchinto.SearchIntoRequestBuilder;

public class ReindexAction extends Action<SearchIntoRequest, SearchIntoResponse, SearchIntoRequestBuilder>{

    public static final ReindexAction INSTANCE = new ReindexAction();
    public static final String NAME = "el-crate-reindex";

    protected ReindexAction() {
        super(NAME);
    }

    @Override
    public SearchIntoRequestBuilder newRequestBuilder(Client client) {
        return new SearchIntoRequestBuilder(client);
    }

    @Override
    public SearchIntoResponse newResponse() {
        return new SearchIntoResponse();
    }

}
