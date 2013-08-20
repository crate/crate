package crate.elasticsearch.action.searchinto;


import crate.elasticsearch.client.action.searchinto.SearchIntoRequestBuilder;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;


/**
 *
 */
public class SearchIntoAction extends Action<SearchIntoRequest,
        SearchIntoResponse, SearchIntoRequestBuilder> {

    public static final SearchIntoAction INSTANCE = new SearchIntoAction();
    public static final String NAME = "el-crate-searchinto";

    private SearchIntoAction() {
        super(NAME);
    }

    @Override
    public SearchIntoResponse newResponse() {
        return new SearchIntoResponse();
    }

    @Override
    public SearchIntoRequestBuilder newRequestBuilder(Client client) {
        return new SearchIntoRequestBuilder(client);
    }

}
