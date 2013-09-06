package org.cratedb.action.sql;

import org.cratedb.action.sql.parser.SQLXContentSourceContext;
import org.cratedb.action.sql.parser.SQLXContentSourceParser;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.bytes.BytesReference;

public class SQLRequestBuilder extends ActionRequestBuilder<SQLRequest, SQLResponse, SQLRequestBuilder> {

    private SQLXContentSourceParser parser;

    public SQLRequestBuilder(Client client) {
        super((InternalClient) client, new SQLRequest());
    }

    /**
     * Executes the built request on the client
     *
     * @param listener
     */
    @Override
    protected void doExecute(ActionListener<SQLResponse> listener) {
        ((Client) client).execute(SQLAction.INSTANCE, request, listener);
    }

    public SQLRequestBuilder source(BytesReference source) {
        SQLXContentSourceContext context = new SQLXContentSourceContext();
        parser = new SQLXContentSourceParser(context);
        parser.parseSource(source);
        request.stmt(context.stmt());
        return this;
    }

}
