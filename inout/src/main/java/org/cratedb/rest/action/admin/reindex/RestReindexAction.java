package org.cratedb.rest.action.admin.reindex;

import static org.elasticsearch.rest.RestRequest.Method.POST;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;

import org.cratedb.action.reindex.ReindexAction;
import org.cratedb.action.searchinto.SearchIntoRequest;
import org.cratedb.action.searchinto.SearchIntoResponse;
import org.cratedb.client.action.searchinto.SearchIntoRequestBuilder;
import org.cratedb.rest.action.admin.searchinto.RestSearchIntoAction;

/**
 * Rest action for the _reindex end points. Does the _search_into action to
 * the own index with the defined fields _id and _source.
 *
 * Does a re-index to either all indexes, a specified index or a specific type of
 * a specified index.
 */
public class RestReindexAction extends RestSearchIntoAction {

    @Inject
    public RestReindexAction(Settings settings, Client client, RestController controller) {
        super(settings, client, controller);
    }

    @Override
    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_reindex", this);
        controller.registerHandler(POST, "/{index}/_reindex", this);
        controller.registerHandler(POST, "/{index}/{type}/_reindex", this);
    }

    @Override
    protected Action<SearchIntoRequest, SearchIntoResponse, SearchIntoRequestBuilder> action() {
        return ReindexAction.INSTANCE;
    }
}
