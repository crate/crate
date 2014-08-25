package io.crate.action.sql;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.client.Client;

public class SQLBulkAction extends ClientAction<SQLBulkRequest, SQLBulkResponse, SQLBulkRequestBuilder> {

    public static final SQLBulkAction INSTANCE = new SQLBulkAction();
    public static final String NAME = "crate_bulk_sql";

    protected SQLBulkAction() {
        super(NAME);
    }

    @Override
    public SQLBulkRequestBuilder newRequestBuilder(Client client) {
        return new SQLBulkRequestBuilder(client);
    }

    @Override
    public SQLBulkResponse newResponse() {
        return new SQLBulkResponse();
    }
}
