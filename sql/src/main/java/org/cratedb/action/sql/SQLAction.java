package org.cratedb.action.sql;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

public class SQLAction extends Action<SQLRequest, SQLResponse, SQLRequestBuilder> {

    public static final SQLAction INSTANCE = new SQLAction();
    public static final String NAME = "crate_sql";

    private SQLAction() {
        super(NAME);
    }

    @Override
    public SQLResponse newResponse() {
        return new SQLResponse();
    }

    @Override
    public SQLRequestBuilder newRequestBuilder(Client client) {
        return new SQLRequestBuilder(client);
    }
}


