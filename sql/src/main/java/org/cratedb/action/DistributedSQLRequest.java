package org.cratedb.action;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLRequest;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;

/**
 * Container that wraps a {@link SQLRequest} and a {@link ParsedStatement}
 *
 * This is used to pass the ParsedStatement to the {@link TransportDistributedSQLAction}
 * This is required because the {@link TransportDistributedSQLAction#execute(org.elasticsearch.action.ActionRequest, org.elasticsearch.action.ActionListener)}
 * method signature can't be extended.
 *
 * the DistributedSQLRequest is never sent over the wire, only locally passed around.
 */
public class DistributedSQLRequest extends ActionRequest {

    public SQLRequest sqlRequest;
    public ParsedStatement parsedStatement;

    public DistributedSQLRequest() {

    }

    public DistributedSQLRequest(SQLRequest request, ParsedStatement stmt) {
        sqlRequest = request;
        parsedStatement = stmt;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
