package io.crate.action.sql;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class SQLBulkAction extends Action<SQLBulkRequest, SQLBulkResponse, SQLBulkRequestBuilder> {

    public static final SQLBulkAction INSTANCE = new SQLBulkAction();
    public static final String NAME = "crate_bulk_sql";

    protected SQLBulkAction() {
        super(NAME);
    }

    @Override
    public SQLBulkRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new SQLBulkRequestBuilder(client);
    }

    @Override
    public SQLBulkResponse newResponse() {
        return new SQLBulkResponse();
    }
}
